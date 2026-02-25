package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"

	_ "modernc.org/sqlite"

	"github.com/analytehealth/granicus/internal/checker"
	"github.com/analytehealth/granicus/internal/config"
	"github.com/analytehealth/granicus/internal/events"
	"github.com/analytehealth/granicus/internal/executor"
	"github.com/analytehealth/granicus/internal/graph"
	"github.com/analytehealth/granicus/internal/runner"
	"github.com/analytehealth/granicus/internal/scheduler"
	"github.com/analytehealth/granicus/internal/server"
	"github.com/analytehealth/granicus/internal/state"
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
	serveCmd.Flags().String("config-dir", "", "Directory containing pipeline YAML configs (required)")
	serveCmd.Flags().String("server-config", "", "Path to granicus-server.yaml")
	serveCmd.Flags().String("env-config", "", "Path to granicus-env.yaml")
	serveCmd.Flags().String("env", "dev", "Environment name (default: dev)")
	serveCmd.Flags().String("project-root", ".", "Project root directory")
	serveCmd.MarkFlagRequired("config-dir")
	return serveCmd
}

func runServe(cmd *cobra.Command, args []string) error {
	configDir, _ := cmd.Flags().GetString("config-dir")
	serverConfigPath, _ := cmd.Flags().GetString("server-config")
	envConfigPath, _ := cmd.Flags().GetString("env-config")
	envName, _ := cmd.Flags().GetString("env")
	projectRoot, _ := cmd.Flags().GetString("project-root")

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
		log.Printf("stale lock recovery error: %v", err)
	} else if recovered > 0 {
		log.Printf("recovered %d stale locks", recovered)
		_ = eventStore.Emit(events.Event{
			EventType: "stale_lock_recovered", Severity: "warning",
			Summary: fmt.Sprintf("Recovered %d stale locks on startup", recovered),
			Details: map[string]any{"recovered_count": recovered},
		})
	}

	// Build run function
	runFunc := func(cfg *config.PipelineConfig, pr string) {
		runPipelineForScheduler(cfg, pr, envName, envCfg, eventStore)
	}

	// Create scheduler
	sched, err := scheduler.NewScheduler(configDir, projectRoot, db, runFunc, eventStore)
	if err != nil {
		return fmt.Errorf("scheduler: %w", err)
	}
	if err := sched.LoadAndRegister(); err != nil {
		return fmt.Errorf("load pipelines: %w", err)
	}

	// Start file watcher
	watcher, err := scheduler.NewWatcher(sched)
	if err != nil {
		log.Printf("warning: file watcher not started: %v", err)
	} else {
		watcher.Start()
		defer watcher.Stop()
	}

	// Start scheduler
	startedAt := time.Now()
	sched.Start()

	pipelines := sched.Pipelines()
	log.Printf("serve: environment=%s, port=%d, pipelines=%d", envName, serverCfg.Server.Port, len(pipelines))
	for _, p := range pipelines {
		cfg := sched.Config(p)
		if cfg != nil {
			log.Printf("  %s: schedule=%q", p, cfg.Schedule)
		}
	}

	// Build HTTP server
	var apiKeys []server.APIKey
	for _, k := range serverCfg.Server.APIKeys {
		apiKeys = append(apiKeys, server.APIKey{Name: k.Name, Key: k.Key})
	}

	srv := server.NewServer(serverCfg.Server.Port, projectRoot, lockStore, eventStore,
		func(cfg *config.PipelineConfig, pr string, runID string, req server.TriggerRequest) {
			runPipelineForTrigger(cfg, pr, runID, envName, envCfg, eventStore, req)
		},
	)

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
			log.Printf("HTTP server error: %v", err)
		}
	}()

	// Wait for interrupt
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	<-sigCh

	log.Println("shutting down...")

	// Graceful shutdown: stop accepting new work, wait for in-progress
	sched.Stop()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer shutdownCancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP shutdown error: %v", err)
	}

	log.Println("shutdown complete")
	return nil
}

func runPipelineForScheduler(cfg *config.PipelineConfig, projectRoot, envName string, envCfg *config.EnvironmentConfig, eventStore *events.Store) {
	if envCfg != nil {
		merged, err := config.MergeEnvironment(cfg, envCfg, envName)
		if err == nil {
			cfg = merged
		}
	}

	runID := events.GenerateRunID()
	log.Printf("scheduled run: %s (run_id=%s)", cfg.Pipeline, runID)
	executePipeline(cfg, projectRoot, runID, eventStore, nil, "", "", "scheduled")
}

func runPipelineForTrigger(cfg *config.PipelineConfig, projectRoot, runID, envName string, envCfg *config.EnvironmentConfig, eventStore *events.Store, req server.TriggerRequest) {
	if envCfg != nil {
		merged, err := config.MergeEnvironment(cfg, envCfg, envName)
		if err == nil {
			cfg = merged
		}
	}

	log.Printf("triggered run: %s (run_id=%s)", cfg.Pipeline, runID)

	_ = eventStore.Emit(events.Event{
		RunID: runID, Pipeline: cfg.Pipeline, EventType: "pipeline_triggered",
		Severity: "info",
		Summary:  fmt.Sprintf("Pipeline %s triggered via webhook", cfg.Pipeline),
		Details:  map[string]any{"assets": req.Assets, "from_date": req.FromDate, "to_date": req.ToDate},
	})

	executePipeline(cfg, projectRoot, runID, eventStore, req.Assets, req.FromDate, req.ToDate, "webhook")
}

func executePipeline(cfg *config.PipelineConfig, projectRoot, runID string, eventStore *events.Store, assetFilter []string, fromDate, toDate, trigger string) {
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

	deps, directives, err := graph.ParseAllDirectives(cfg, projectRoot)
	if err != nil {
		log.Printf("run %s: dependency parse error: %v", runID, err)
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

	g, err := graph.BuildGraph(inputs, deps)
	if err != nil {
		log.Printf("run %s: graph build error: %v", runID, err)
		return
	}

	stateDBPath := filepath.Join(projectRoot, ".granicus", "state.db")
	stateStore, err := state.New(stateDBPath)
	if err != nil {
		log.Printf("run %s: state store error: %v", runID, err)
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
			srcPath := filepath.Join(pr, asset.Source)
			if hash, herr := events.HashFile(srcPath); herr == nil {
				eventStore.RecordModelVersion(asset.Name, srcPath, hash, runID)
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
		}

		r := registry.Run(ra, pr, rid)

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

	log.Printf("run %s: %s — %d succeeded, %d failed, %d skipped (%.0fs)",
		runID, cfg.Pipeline, succeeded, failed, skipped, totalDuration.Seconds())
}
