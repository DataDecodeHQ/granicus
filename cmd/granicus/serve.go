package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	_ "modernc.org/sqlite"

	"github.com/DataDecodeHQ/granicus/internal/logging"
	"github.com/DataDecodeHQ/granicus/internal/checker"
	"github.com/DataDecodeHQ/granicus/internal/config"
	"github.com/DataDecodeHQ/granicus/internal/events"
	"github.com/DataDecodeHQ/granicus/internal/executor"
	"github.com/DataDecodeHQ/granicus/internal/graph"
	"github.com/DataDecodeHQ/granicus/internal/pool"
	"github.com/DataDecodeHQ/granicus/internal/runner"
	"github.com/DataDecodeHQ/granicus/internal/scheduler"
	"github.com/DataDecodeHQ/granicus/internal/server"
	"github.com/DataDecodeHQ/granicus/internal/source"
)

func init() {
	// serve command is added in main
}

func newServeCmd() *cobra.Command {
	serveCmd := &cobra.Command{
		Use:   "serve",
		Short: "Start scheduler and HTTP trigger server",
		RunE:  runServe,
	}
	serveCmd.Flags().String("config-dir", "", "Directory containing pipeline YAML configs (required for local source)")
	serveCmd.Flags().String("server-config", "", "Path to granicus-server.yaml")
	serveCmd.Flags().String("env-config", "", "Path to granicus-env.yaml")
	serveCmd.Flags().String("env", "dev", "Environment name (default: dev)")
	serveCmd.Flags().String("project-root", ".", "Project root directory")
	serveCmd.Flags().Duration("orphan-timeout", 2*time.Hour, "Timeout before an in_progress interval is considered orphaned and recovered")
	return serveCmd
}

func runServe(cmd *cobra.Command, args []string) error {
	logging.Init(true)

	configDir, _ := cmd.Flags().GetString("config-dir")
	serverConfigPath, _ := cmd.Flags().GetString("server-config")
	envConfigPath, _ := cmd.Flags().GetString("env-config")
	envName, _ := cmd.Flags().GetString("env")
	projectRoot, _ := cmd.Flags().GetString("project-root")
	orphanTimeout, _ := cmd.Flags().GetDuration("orphan-timeout")

	// Load server config
	var serverCfg *config.ServerConfig
	if serverConfigPath != "" {
		var err error
		serverCfg, err = config.LoadServerConfig(serverConfigPath)
		if err != nil {
			return fmt.Errorf("server config: %w", err)
		}
	} else {
		serverCfg = &config.ServerConfig{Server: config.ServerSettings{Port: 8080}}
	}

	// Load environment config (optional)
	var envCfg *config.EnvironmentConfig
	if envConfigPath != "" {
		var err error
		envCfg, err = config.LoadEnvironmentConfig(envConfigPath)
		if err != nil {
			return fmt.Errorf("env config: %w", err)
		}
	}

	// Open state DB
	stateDBPath := config.StateDBPath(projectRoot, envName)
	os.MkdirAll(filepath.Dir(stateDBPath), 0755)
	db, err := sql.Open("sqlite", stateDBPath+"?_pragma=journal_mode(WAL)")
	if err != nil {
		return fmt.Errorf("state db: %w", err)
	}
	defer db.Close()

	// Initialize lock store
	lockStore, err := scheduler.NewLockStore(db)
	if err != nil {
		return fmt.Errorf("lock store: %w", err)
	}

	// Open event store
	eventsDBPath := filepath.Join(projectRoot, ".granicus", "events.db")
	eventStore, err := events.New(eventsDBPath)
	if err != nil {
		return fmt.Errorf("event store: %w", err)
	}
	defer eventStore.Close()

	// Recover stale locks
	recovered, err := lockStore.RecoverStaleLocks(6 * time.Hour)
	if err != nil {
		slog.Error("stale lock recovery error", "error", err)
	} else if recovered > 0 {
		slog.Info("recovered stale locks", "count", recovered)
		_ = eventStore.Emit(events.Event{
			EventType: "stale_lock_recovered", Severity: "warning",
			Summary: fmt.Sprintf("Recovered %d stale locks on startup", recovered),
			Details: map[string]any{"recovered_count": recovered},
		})
	}

	// Initialize all backends
	backends, err := initBackends(projectRoot, configDir, envName)
	if err != nil {
		return fmt.Errorf("backends: %w", err)
	}
	defer backends.State.Close()
	if backends.Dispatch != nil {
		if closer, ok := backends.Dispatch.(interface{ Close() error }); ok {
			defer closer.Close()
		}
	}

	// Recover orphaned intervals (in_progress longer than orphan_timeout)
	orphans, rerr := backends.State.RecoverOrphans(orphanTimeout)
	if rerr != nil {
		slog.Error("orphan interval recovery error", "error", rerr)
	} else if len(orphans) > 0 {
		slog.Info("recovered orphaned intervals", "count", len(orphans))
		for _, iv := range orphans {
			_ = eventStore.Emit(events.Event{
				EventType: "interval_recovered", Severity: "warning",
				Summary: fmt.Sprintf("Recovered orphaned interval %s/%s (was in_progress since %s)", iv.AssetName, iv.IntervalStart, iv.StartedAt),
				Details: map[string]any{
					"asset_name":     iv.AssetName,
					"interval_start": iv.IntervalStart,
					"interval_end":   iv.IntervalEnd,
					"run_id":         iv.RunID,
					"started_at":     iv.StartedAt,
				},
			})
		}
	}

	// Shutdown context: cancelled on SIGTERM/SIGINT to drain executor runs.
	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())
	defer shutdownCancel()

	// Build run function
	dispatch := backends.Dispatch
	runFunc := func(cfg *config.PipelineConfig, pr string) {
		runPipelineForScheduler(cfg, pr, envName, envCfg, eventStore, dispatch, shutdownCtx)
	}

	pipeSrc := backends.Source

	// Create scheduler
	sched, err := scheduler.NewScheduler(pipeSrc, projectRoot, db, runFunc, eventStore)
	if err != nil {
		return fmt.Errorf("scheduler: %w", err)
	}
	if err := sched.LoadAndRegister(); err != nil {
		return fmt.Errorf("load pipelines: %w", err)
	}
	sched.RegisterAssetPolls()

	// Start file watcher (only for local sources)
	if _, isLocal := pipeSrc.(*source.LocalSource); isLocal {
		watcher, err := scheduler.NewWatcher(sched)
		if err != nil {
			slog.Warn("file watcher not started", "error", err)
		} else {
			watcher.Start()
			defer watcher.Stop()
		}
	}

	// Start scheduler
	startedAt := time.Now()
	sched.Start()

	pipelines := sched.Pipelines()
	slog.Info("serve started", "environment", envName, "port", serverCfg.Server.Port, "pipelines", len(pipelines))
	for _, p := range pipelines {
		cfg := sched.Config(p)
		if cfg != nil {
			slog.Info("registered pipeline", "pipeline", p, "schedule", cfg.Schedule)
		}
	}

	// Build HTTP server
	var apiKeys []server.APIKey
	for _, k := range serverCfg.Server.APIKeys {
		apiKeys = append(apiKeys, server.APIKey{Name: k.Name, Key: k.Key})
	}

	srv := server.NewServer(serverCfg.Server.Port, projectRoot, lockStore, eventStore,
		func(cfg *config.PipelineConfig, pr string, runID string, req server.TriggerRequest) {
			runPipelineForTrigger(cfg, pr, runID, envName, envCfg, eventStore, req, dispatch, shutdownCtx)
		},
	)
	srv.SetShutdownCtx(shutdownCtx)

	// Set pipeline configs on the server
	configMap := make(map[string]*config.PipelineConfig)
	for _, p := range pipelines {
		if cfg := sched.Config(p); cfg != nil {
			configMap[p] = cfg
		}
	}
	srv.SetConfigs(configMap)

	// Start HTTP server with auth middleware
	mux := http.NewServeMux()
	mux.Handle("/metrics", server.MetricsHandler())
	mux.HandleFunc("/api/v1/health", func(w http.ResponseWriter, r *http.Request) {
		server.HealthHandler(w, r, startedAt, len(pipelines))
	})
	mux.Handle("/", srv.Handler())
	handler := server.AuthMiddleware(apiKeys, mux)
	httpSrv := &http.Server{
		Addr:    fmt.Sprintf(":%d", serverCfg.Server.Port),
		Handler: handler,
	}

	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP server error", "error", err)
		}
	}()

	// Wait for SIGTERM or SIGINT
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	slog.Info("shutting down")

	// Signal all in-progress executor runs to drain.
	shutdownCancel()

	// Stop the scheduler from starting new runs.
	sched.Stop()

	// Stop accepting new HTTP requests and drain in-flight HTTP connections.
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer drainCancel()
	if err := httpSrv.Shutdown(drainCtx); err != nil {
		slog.Error("HTTP shutdown error", "error", err)
	}

	// Wait for in-progress triggered pipeline runs to finish (same timeout).
	srv.WaitForRuns(drainCtx)

	slog.Info("shutdown complete")
	return nil
}

func runPipelineForScheduler(cfg *config.PipelineConfig, projectRoot, envName string, envCfg *config.EnvironmentConfig, eventStore *events.Store, dispatch runner.RunnerDispatch, ctx context.Context) {
	if envCfg != nil {
		merged, err := config.MergeEnvironment(cfg, envCfg, envName)
		if err == nil {
			cfg = merged
		}
	}

	runID := events.GenerateRunID()
	slog.Info("scheduled run", "pipeline", cfg.Pipeline, "run_id", runID)
	poolMgr, assetPools := buildPoolManager(cfg)
	executePipeline(cfg, projectRoot, runID, eventStore, nil, "", "", "scheduled", poolMgr, assetPools, dispatch, ctx)
}

func runPipelineForTrigger(cfg *config.PipelineConfig, projectRoot, runID, envName string, envCfg *config.EnvironmentConfig, eventStore *events.Store, req server.TriggerRequest, dispatch runner.RunnerDispatch, ctx context.Context) {
	if envCfg != nil {
		merged, err := config.MergeEnvironment(cfg, envCfg, envName)
		if err == nil {
			cfg = merged
		}
	}

	slog.Info("triggered run", "pipeline", cfg.Pipeline, "run_id", runID)

	_ = eventStore.Emit(events.Event{
		RunID: runID, Pipeline: cfg.Pipeline, EventType: "pipeline_triggered",
		Severity: "info",
		Summary:  fmt.Sprintf("Pipeline %s triggered via webhook", cfg.Pipeline),
		Details:  map[string]any{"assets": req.Assets, "from_date": req.FromDate, "to_date": req.ToDate},
	})

	poolMgr, assetPools := buildPoolManager(cfg)
	executePipeline(cfg, projectRoot, runID, eventStore, req.Assets, req.FromDate, req.ToDate, "webhook", poolMgr, assetPools, dispatch, ctx)
}

func executePipeline(cfg *config.PipelineConfig, projectRoot, runID string, eventStore *events.Store, assetFilter []string, fromDate, toDate, trigger string, poolMgr *pool.PoolManager, assetPools map[string]string, dispatch runner.RunnerDispatch, ctx context.Context) {
	start := time.Now()

	_ = eventStore.Emit(events.Event{
		RunID: runID, Pipeline: cfg.Pipeline, EventType: "run_started", Severity: "info",
		Summary: fmt.Sprintf("Pipeline %s started", cfg.Pipeline),
		Details: map[string]any{
			"asset_count":  len(cfg.Assets),
			"max_parallel": cfg.MaxParallel,
			"asset_filter": assetFilter,
			"trigger":      trigger,
		},
	})

	// Use config dir for source resolution when pipeline was fetched from GCS
	parseRoot := projectRoot
	if cfg.ConfigDir != "" {
		parseRoot = cfg.ConfigDir
	}
	deps, directives, err := graph.ParseAllDirectives(cfg, parseRoot)
	if err != nil {
		slog.Error("dependency parse error", "run_id", runID, "error", err)
		return
	}

	inputs := graph.ConfigToAssetInputs(cfg)
	for i := range inputs {
		if d, ok := directives[inputs[i].Name]; ok {
			inputs[i].TimeColumn = d.TimeColumn
			inputs[i].IntervalUnit = d.IntervalUnit
			inputs[i].Lookback = d.Lookback
			inputs[i].StartDate = d.StartDate
			inputs[i].BatchSize = d.BatchSize
			if d.Layer != "" {
				inputs[i].Layer = d.Layer
			}
			if d.Grain != "" {
				inputs[i].Grain = d.Grain
			}
			if d.DefaultChecks != nil {
				inputs[i].DefaultChecks = d.DefaultChecks
			}
		}
	}

	checkNodes, checkDeps := checker.GenerateCheckNodes(cfg)
	inputs = append(inputs, checkNodes...)
	for k, v := range checkDeps {
		deps[k] = v
	}

	defaultNodes, defaultDeps := checker.GenerateDefaultCheckNodes(cfg)
	inputs = append(inputs, defaultNodes...)
	for k, v := range defaultDeps {
		deps[k] = v
	}

	// Add source phantom nodes
	sourceNodes := graph.SourcePhantomNodes(cfg)
	inputs = append(inputs, sourceNodes...)

	// Generate source check nodes
	sourceCheckNodes, sourceCheckDeps := checker.GenerateSourceCheckNodes(cfg)
	inputs = append(inputs, sourceCheckNodes...)
	for k, v := range sourceCheckDeps {
		deps[k] = v
	}

	// Wire source checks to gate staging assets
	if len(sourceCheckNodes) > 0 {
		var sourceCheckNames []string
		for _, sc := range sourceCheckNodes {
			sourceCheckNames = append(sourceCheckNames, sc.Name)
		}
		for i := range inputs {
			if inputs[i].Layer == "staging" {
				if deps[inputs[i].Name] == nil {
					deps[inputs[i].Name] = sourceCheckNames
				} else {
					deps[inputs[i].Name] = append(deps[inputs[i].Name], sourceCheckNames...)
				}
			}
		}
	}

	g, err := graph.BuildGraph(inputs, deps)
	if err != nil {
		slog.Error("graph build error", "run_id", runID, "error", err)
		return
	}

	stateStore, err := initStateBackend(projectRoot, cfg.Pipeline, "")
	if err != nil {
		slog.Error("state store error", "run_id", runID, "error", err)
		return
	}
	defer stateStore.Close()

	registry := buildRegistry(cfg, projectRoot)

	runnerFunc := func(asset *graph.Asset, pr string, rid string) executor.NodeResult {
		_ = eventStore.Emit(events.Event{
			RunID: runID, Pipeline: cfg.Pipeline, Asset: asset.Name,
			EventType: "node_started", Severity: "info",
			Summary: fmt.Sprintf("Node %s started", asset.Name),
		})

		if asset.Source != "" {
			// Resolve source path: use config dir (GCS extraction) if set, else project root
			sourceBase := pr
			if cfg.ConfigDir != "" {
				sourceBase = cfg.ConfigDir
			}
			srcPath := filepath.Join(sourceBase, asset.Source)
			if hash, herr := events.HashFile(srcPath); herr == nil {
				eventStore.RecordModelVersion(asset.Name, srcPath, hash, runID)
			}
		}

		assetCfg := findAssetConfig(cfg, asset.Name)
		resolvedDataset := ""
		if assetCfg != nil {
			defaultDS := ""
			if conn := connectionForAsset(cfg, assetCfg); conn != nil {
				defaultDS = conn.Properties["dataset"]
			}
			resolvedDataset = cfg.DatasetForAsset(*assetCfg, defaultDS)
		}

		var resolvedDestConn, resolvedSourceConn *config.ConnectionConfig
		if assetCfg != nil {
			if assetCfg.DestinationConnection != "" {
				if conn, ok := cfg.Connections[assetCfg.DestinationConnection]; ok {
					resolvedDestConn = conn
				}
			}
			if assetCfg.SourceConnection != "" {
				if conn, ok := cfg.Connections[assetCfg.SourceConnection]; ok {
					resolvedSourceConn = conn
				}
			}
		}

		ra := &runner.Asset{
			Name:                  asset.Name,
			Type:                  asset.Type,
			Source:                asset.Source,
			DestinationConnection: asset.DestinationConnection,
			SourceConnection:      asset.SourceConnection,
			IntervalStart:         asset.IntervalStart,
			IntervalEnd:           asset.IntervalEnd,
			Prefix:                cfg.Prefix,
			InlineSQL:             asset.InlineSQL,
			DependsOn:             asset.DependsOn,
			Timeout:               asset.Timeout,
			Dataset:               resolvedDataset,
			Layer:                 asset.Layer,
			ResolvedDestConn:      resolvedDestConn,
			ResolvedSourceConn:    resolvedSourceConn,
		}

		// Use config dir for source resolution when pipeline was fetched from GCS
		runRoot := pr
		if cfg.ConfigDir != "" {
			runRoot = cfg.ConfigDir
		}

		var r runner.NodeResult
		if dispatch != nil && dispatch.Supports(ra.Type) {
			var derr error
			r, derr = dispatch.Execute(ctx, ra, runRoot, rid)
			if derr != nil {
				r = runner.NodeResult{
					AssetName: ra.Name,
					Status:    "failed",
					StartTime: time.Now(),
					EndTime:   time.Now(),
					Error:     fmt.Sprintf("dispatch error: %v", derr),
					ExitCode:  -1,
					Metadata:  map[string]string{"runner": "cloud_run_job"},
				}
			}
		} else {
			r = registry.Run(ra, runRoot, rid)
		}

		if r.Status == "success" {
			_ = eventStore.Emit(events.Event{
				RunID: runID, Pipeline: cfg.Pipeline, Asset: r.AssetName,
				EventType: "node_succeeded", Severity: "info",
				DurationMs: r.Duration.Milliseconds(),
				Summary:    fmt.Sprintf("Node %s succeeded (%.1fs)", r.AssetName, r.Duration.Seconds()),
				Details: map[string]any{
					"exit_code":    r.ExitCode,
					"metadata":     r.Metadata,
					"stdout_lines": events.CountLines(r.Stdout),
					"stderr_lines": events.CountLines(r.Stderr),
				},
			})
		} else {
			stdout := r.Stdout
			if len(stdout) > 10*1024 {
				stdout = stdout[:10*1024] + "[truncated]"
			}
			stderr := r.Stderr
			if len(stderr) > 10*1024 {
				stderr = stderr[:10*1024] + "[truncated]"
			}
			_ = eventStore.Emit(events.Event{
				RunID: runID, Pipeline: cfg.Pipeline, Asset: r.AssetName,
				EventType: "node_failed", Severity: "error",
				DurationMs: r.Duration.Milliseconds(),
				Summary:    fmt.Sprintf("Node %s failed: %s", r.AssetName, r.Error),
				Details: map[string]any{
					"error_message": r.Error,
					"exit_code":     r.ExitCode,
					"source_file":   asset.Source,
					"metadata":      r.Metadata,
					"stdout":        stdout,
					"stderr":        stderr,
				},
			})
		}

		return executor.NodeResult{
			AssetName: r.AssetName,
			Status:    r.Status,
			StartTime: r.StartTime,
			EndTime:   r.EndTime,
			Duration:  r.Duration,
			Error:     r.Error,
			Stdout:    r.Stdout,
			Stderr:    r.Stderr,
			ExitCode:  r.ExitCode,
			Metadata:  r.Metadata,
		}
	}

	runCfg := executor.RunConfig{
		MaxParallel: cfg.MaxParallel,
		Assets:      assetFilter,
		ProjectRoot: projectRoot,
		RunID:       runID,
		FromDate:    fromDate,
		ToDate:      toDate,
		StateStore:  stateStore,
		PoolManager: poolMgr,
		AssetPools:  assetPools,
		Ctx:         ctx,
	}

	rr := executor.Execute(g, runCfg, runnerFunc)

	var succeeded, failed, skipped int
	for _, r := range rr.Results {
		switch r.Status {
		case "success":
			succeeded++
		case "failed":
			failed++
		case "skipped":
			skipped++
			_ = eventStore.Emit(events.Event{
				RunID: runID, Pipeline: cfg.Pipeline, Asset: r.AssetName,
				EventType: "node_skipped", Severity: "warning",
				Summary: fmt.Sprintf("Node %s skipped", r.AssetName),
			})
		}
	}

	status := "success"
	if failed > 0 || skipped > 0 {
		status = "completed_with_failures"
	}

	totalDuration := rr.EndTime.Sub(start)
	_ = eventStore.Emit(events.Event{
		RunID: runID, Pipeline: cfg.Pipeline, EventType: "run_completed", Severity: "info",
		DurationMs: totalDuration.Milliseconds(),
		Summary:    fmt.Sprintf("Run %s: %d succeeded, %d failed, %d skipped", status, succeeded, failed, skipped),
		Details: map[string]any{
			"status":           status,
			"succeeded":        succeeded,
			"failed":           failed,
			"skipped":          skipped,
			"total_nodes":      len(rr.Results),
			"duration_seconds": totalDuration.Seconds(),
		},
	})

	// Record metrics
	server.RunsTotal.WithLabelValues(cfg.Pipeline, status).Inc()
	server.RunDuration.WithLabelValues(cfg.Pipeline).Observe(totalDuration.Seconds())
	for _, r := range rr.Results {
		server.NodesTotal.WithLabelValues(cfg.Pipeline, r.Status).Inc()
		server.NodeDuration.WithLabelValues(cfg.Pipeline, r.AssetName, r.Status).Observe(r.Duration.Seconds())
	}

	slog.Info("run completed", "run_id", runID, "pipeline", cfg.Pipeline, "succeeded", succeeded, "failed", failed, "skipped", skipped, "duration_s", totalDuration.Seconds())

	// Send run summary notification
	if cfg.Alerts != nil {
		alertMgr := server.NewAlertManager(cfg.Alerts, eventStore)
		var summaryResults []struct {
			AssetName string
			Status    string
			Error     string
			Duration  float64
			Cost      float64
		}
		for _, r := range rr.Results {
			cost := 0.0
			if r.Metadata != nil {
				if c, ok := r.Metadata["bq_total_bytes_billed"]; ok {
					if v, err := fmt.Sscanf(c, "%f", &cost); v == 1 && err == nil {
						cost = cost / 1e12 * 5.0
					}
				}
			}
			summaryResults = append(summaryResults, struct {
				AssetName string
				Status    string
				Error     string
				Duration  float64
				Cost      float64
			}{r.AssetName, r.Status, r.Error, r.Duration.Seconds(), cost})
		}
		alertData := server.BuildRunSummary(cfg.Pipeline, runID, summaryResults, totalDuration.Seconds())
		alertMgr.SendRunSummary(alertData)
	}

	// Post-run hooks: context.db + monitor.db
	bqClient := newBQClientForContext(cfg)
	if bqClient != nil {
		defer bqClient.Close()
	}
	hooks := []executor.PostRunHook{
		executor.WriteContextHook(bqClient),
		monitorHook(bqClient),
		executor.DuckDBAssemblyHook(),
	}
	executor.RunPostHooks(hooks, g, cfg, projectRoot, rr)
}
