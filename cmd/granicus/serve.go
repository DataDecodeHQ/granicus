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
	"github.com/analytehealth/granicus/internal/executor"
	"github.com/analytehealth/granicus/internal/graph"
	"github.com/analytehealth/granicus/internal/logging"
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

	// Recover stale locks
	recovered, err := lockStore.RecoverStaleLocks(6 * time.Hour)
	if err != nil {
		log.Printf("stale lock recovery error: %v", err)
	} else if recovered > 0 {
		log.Printf("recovered %d stale locks", recovered)
	}

	// Build run function
	logStore := logging.NewStore(projectRoot)
	runFunc := func(cfg *config.PipelineConfig, pr string) {
		runPipelineForScheduler(cfg, pr, envName, envCfg, logStore)
	}

	// Create scheduler
	sched, err := scheduler.NewScheduler(configDir, projectRoot, db, runFunc)
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

	srv := server.NewServer(serverCfg.Server.Port, projectRoot, lockStore, logStore,
		func(cfg *config.PipelineConfig, pr string, runID string, req server.TriggerRequest) {
			runPipelineForTrigger(cfg, pr, runID, envName, envCfg, logStore, req)
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

func runPipelineForScheduler(cfg *config.PipelineConfig, projectRoot, envName string, envCfg *config.EnvironmentConfig, logStore *logging.Store) {
	if envCfg != nil {
		merged, err := config.MergeEnvironment(cfg, envCfg, envName)
		if err == nil {
			cfg = merged
		}
	}

	runID := logging.GenerateRunID()
	log.Printf("scheduled run: %s (run_id=%s)", cfg.Pipeline, runID)
	executePipeline(cfg, projectRoot, runID, logStore, nil, "", "")
}

func runPipelineForTrigger(cfg *config.PipelineConfig, projectRoot, runID, envName string, envCfg *config.EnvironmentConfig, logStore *logging.Store, req server.TriggerRequest) {
	if envCfg != nil {
		merged, err := config.MergeEnvironment(cfg, envCfg, envName)
		if err == nil {
			cfg = merged
		}
	}

	log.Printf("triggered run: %s (run_id=%s)", cfg.Pipeline, runID)

	executePipeline(cfg, projectRoot, runID, logStore, req.Assets, req.FromDate, req.ToDate)
}

func executePipeline(cfg *config.PipelineConfig, projectRoot, runID string, logStore *logging.Store, assetFilter []string, fromDate, toDate string) {
	start := time.Now()

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

	registry := buildRegistry(cfg)

	runnerFunc := func(asset *graph.Asset, pr string, rid string) executor.NodeResult {
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

		entry := logging.NodeEntry{
			Asset:       r.AssetName,
			Status:      r.Status,
			StartTime:   r.StartTime.Format(time.RFC3339),
			EndTime:     r.EndTime.Format(time.RFC3339),
			DurationMs:  r.Duration.Milliseconds(),
			ExitCode:    r.ExitCode,
			Error:       r.Error,
			Stdout:      r.Stdout,
			Stderr:      r.Stderr,
			StdoutLines: logging.CountLines(r.Stdout),
			StderrLines: logging.CountLines(r.Stderr),
			Metadata:    r.Metadata,
		}
		_ = logStore.WriteNodeResult(runID, entry)

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
		}
	}

	status := "success"
	if failed > 0 || skipped > 0 {
		status = "completed_with_failures"
	}

	totalDuration := rr.EndTime.Sub(start)
	summary := logging.RunSummary{
		RunID:           runID,
		Pipeline:        cfg.Pipeline,
		StartTime:       start,
		EndTime:         rr.EndTime,
		DurationSeconds: totalDuration.Seconds(),
		TotalNodes:      len(rr.Results),
		Succeeded:       succeeded,
		Failed:          failed,
		Skipped:         skipped,
		Status:          status,
		Config:          logging.RunConfig{MaxParallel: cfg.MaxParallel, AssetsFilter: assetFilter},
	}
	_ = logStore.WriteRunSummary(runID, summary)

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
