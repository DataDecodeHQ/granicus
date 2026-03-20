package main

import (
	"context"
	"database/sql"
	"encoding/json"
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
	"github.com/DataDecodeHQ/granicus/internal/config"
	"github.com/DataDecodeHQ/granicus/internal/events"
	"github.com/DataDecodeHQ/granicus/internal/executor"
	"github.com/DataDecodeHQ/granicus/internal/pool"
	"github.com/DataDecodeHQ/granicus/internal/runner"
	"github.com/DataDecodeHQ/granicus/internal/scheduler"
	"github.com/DataDecodeHQ/granicus/internal/server"
	"github.com/DataDecodeHQ/granicus/internal/source"
)

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

// PipelineExecContext bundles the shared dependencies needed to execute a pipeline run.
type PipelineExecContext struct {
	cfg         *config.PipelineConfig
	projectRoot string
	runID       string
	eventStore  *events.Store
	dispatch    runner.RunnerDispatch
	ctx         context.Context
	poolMgr     *pool.PoolManager
	assetPools  map[string]string
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

	var apiKeys []server.APIKey
	for _, k := range serverCfg.Server.APIKeys {
		apiKeys = append(apiKeys, server.APIKey{Name: k.Name, Key: k.Key})
	}
	// GRANICUS_API_KEYS overrides / supplements server config api_keys.
	// Expected format: JSON array of {name, key} objects, e.g.
	//   [{"name":"scheduler","key":"<value>"}]
	if envKeys := os.Getenv("GRANICUS_API_KEYS"); envKeys != "" {
		var parsed []server.APIKey
		if err := json.Unmarshal([]byte(envKeys), &parsed); err != nil {
			return fmt.Errorf("parsing GRANICUS_API_KEYS: %w", err)
		}
		apiKeys = append(apiKeys, parsed...)
		slog.Info("api_keys_loaded", "source", "GRANICUS_API_KEYS", "count", len(parsed))
	}
	if err := server.ValidateAuth(apiKeys); err != nil {
		slog.Error("auth validation failed", "env", envName, "error", err)
		return fmt.Errorf("auth validation: %w", err)
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
		if err := eventStore.Emit(events.Event{
			EventType: "stale_lock_recovered", Severity: "warning",
			Summary: fmt.Sprintf("Recovered %d stale locks on startup", recovered),
			Details: map[string]any{"recovered_count": recovered},
		}); err != nil {
			slog.Warn("event emission failed", "event_type", "stale_lock_recovered", "error", err)
		}
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
			if err := eventStore.Emit(events.Event{
				EventType: "interval_recovered", Severity: "warning",
				Summary: fmt.Sprintf("Recovered orphaned interval %s/%s (was in_progress since %s)", iv.AssetName, iv.IntervalStart, iv.StartedAt),
				Details: map[string]any{
					"asset_name":     iv.AssetName,
					"interval_start": iv.IntervalStart,
					"interval_end":   iv.IntervalEnd,
					"run_id":         iv.RunID,
					"started_at":     iv.StartedAt,
				},
			}); err != nil {
				slog.Warn("event emission failed", "event_type", "interval_recovered", "error", err)
			}
		}
	}

	// Shutdown context: cancelled on SIGTERM/SIGINT to drain executor runs.
	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())
	defer shutdownCancel()

	// Build run function
	dispatch := backends.Dispatch
	runFunc := func(cfg *config.PipelineConfig, pr string) {
		pec := PipelineExecContext{
			cfg:         cfg,
			projectRoot: pr,
			eventStore:  eventStore,
			dispatch:    dispatch,
			ctx:         shutdownCtx,
		}
		runPipelineForScheduler(pec, envName, envCfg)
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
	srv := server.NewServer(serverCfg.Server.Port, projectRoot, lockStore, eventStore,
		func(cfg *config.PipelineConfig, pr string, runID string, req server.TriggerRequest) {
			pec := PipelineExecContext{
				cfg:         cfg,
				projectRoot: pr,
				runID:       runID,
				eventStore:  eventStore,
				dispatch:    dispatch,
				ctx:         shutdownCtx,
			}
			runPipelineForTrigger(pec, envName, envCfg, req)
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

func runPipelineForScheduler(pec PipelineExecContext, envName string, envCfg *config.EnvironmentConfig) {
	if envCfg != nil {
		merged, err := config.MergeEnvironment(pec.cfg, envCfg, envName)
		if err == nil {
			pec.cfg = merged
		}
	}

	pec.runID = events.GenerateRunID()
	slog.Info("scheduled run", "pipeline", pec.cfg.Pipeline, "run_id", pec.runID)
	pec.poolMgr, pec.assetPools = buildPoolManager(pec.cfg)
	executePipeline(pec, nil, "", "", "scheduled")
}

func runPipelineForTrigger(pec PipelineExecContext, envName string, envCfg *config.EnvironmentConfig, req server.TriggerRequest) {
	if envCfg != nil {
		merged, err := config.MergeEnvironment(pec.cfg, envCfg, envName)
		if err == nil {
			pec.cfg = merged
		}
	}

	slog.Info("triggered run", "pipeline", pec.cfg.Pipeline, "run_id", pec.runID)

	if err := pec.eventStore.Emit(events.Event{
		RunID: pec.runID, Pipeline: pec.cfg.Pipeline, EventType: "pipeline_triggered",
		Severity: "info",
		Summary:  fmt.Sprintf("Pipeline %s triggered via webhook", pec.cfg.Pipeline),
		Details:  map[string]any{"assets": req.Assets, "from_date": req.FromDate, "to_date": req.ToDate},
	}); err != nil {
		slog.Warn("event emission failed", "event_type", "pipeline_triggered", "error", err)
	}

	pec.poolMgr, pec.assetPools = buildPoolManager(pec.cfg)
	executePipeline(pec, req.Assets, req.FromDate, req.ToDate, "webhook")
}

func executePipeline(pec PipelineExecContext, assetFilter []string, fromDate, toDate, trigger string) {
	start := time.Now()

	if err := pec.eventStore.Emit(events.Event{
		RunID: pec.runID, Pipeline: pec.cfg.Pipeline, EventType: "run_started", Severity: "info",
		Summary: fmt.Sprintf("Pipeline %s started", pec.cfg.Pipeline),
		Details: map[string]any{
			"asset_count":  len(pec.cfg.Assets),
			"max_parallel": pec.cfg.MaxParallel,
			"asset_filter": assetFilter,
			"trigger":      trigger,
		},
	}); err != nil {
		slog.Warn("event emission failed", "event_type", "run_started", "error", err)
	}

	// Use config dir for source resolution when pipeline was fetched from GCS
	parseRoot := pec.projectRoot
	if pec.cfg.ConfigDir != "" {
		parseRoot = pec.cfg.ConfigDir
	}
	g, _, err := buildPipelineGraph(pec.cfg, parseRoot)
	if err != nil {
		slog.Error("graph build error", "run_id", pec.runID, "error", err)
		return
	}

	stateStore, err := initStateBackend(pec.projectRoot, pec.cfg.Pipeline, "")
	if err != nil {
		slog.Error("state store error", "run_id", pec.runID, "error", err)
		return
	}
	defer stateStore.Close()

	registry := buildRegistry(pec.cfg, parseRoot)

	runnerFunc := buildNodeRunner(pec.cfg, pec.runID, pec.eventStore, registry, nodeRunnerOptions{
		Dispatch:    pec.dispatch,
		DispatchCtx: pec.ctx,
		ConfigDir:   pec.cfg.ConfigDir,
	})

	runCfg := executor.RunConfig{
		MaxParallel: pec.cfg.MaxParallel,
		Assets:      assetFilter,
		ProjectRoot: pec.projectRoot,
		RunID:       pec.runID,
		FromDate:    fromDate,
		ToDate:      toDate,
		StateStore:  stateStore,
		PoolManager: pec.poolMgr,
		AssetPools:  pec.assetPools,
		Ctx:         pec.ctx,
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
			if err := pec.eventStore.Emit(events.Event{
				RunID: pec.runID, Pipeline: pec.cfg.Pipeline, Asset: r.AssetName,
				EventType: "node_skipped", Severity: "warning",
				Summary: fmt.Sprintf("Node %s skipped", r.AssetName),
			}); err != nil {
				slog.Warn("event emission failed", "event_type", "node_skipped", "error", err)
			}
		}
	}

	status := "success"
	if failed > 0 || skipped > 0 {
		status = "completed_with_failures"
	}

	totalDuration := rr.EndTime.Sub(start)
	if err := pec.eventStore.Emit(events.Event{
		RunID: pec.runID, Pipeline: pec.cfg.Pipeline, EventType: "run_completed", Severity: "info",
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
	}); err != nil {
		slog.Warn("event emission failed", "event_type", "run_completed", "error", err)
	}

	// Record metrics
	server.RunsTotal.WithLabelValues(pec.cfg.Pipeline, status).Inc()
	server.RunDuration.WithLabelValues(pec.cfg.Pipeline).Observe(totalDuration.Seconds())
	for _, r := range rr.Results {
		server.NodesTotal.WithLabelValues(pec.cfg.Pipeline, r.Status).Inc()
		server.NodeDuration.WithLabelValues(pec.cfg.Pipeline, r.AssetName, r.Status).Observe(r.Duration.Seconds())
	}

	slog.Info("run completed", "run_id", pec.runID, "pipeline", pec.cfg.Pipeline, "succeeded", succeeded, "failed", failed, "skipped", skipped, "duration_s", totalDuration.Seconds())

	// Send run summary notification
	sendRunAlerts(pec.cfg, pec.runID, pec.eventStore, rr, totalDuration)

	// Post-run hooks: context.db + monitor.db
	runPostRunHooks(pec.cfg, g, pec.projectRoot, rr)
}
