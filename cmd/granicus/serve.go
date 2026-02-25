package main

import (
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

	"github.com/analytehealth/granicus/internal/config"
	"github.com/analytehealth/granicus/internal/logging"
	"github.com/analytehealth/granicus/internal/scheduler"
	"github.com/analytehealth/granicus/internal/server"
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
	sched.Start()
	defer sched.Stop()

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
	go func() {
		handler := server.AuthMiddleware(apiKeys, srv.Handler())
		httpSrv := &http.Server{
			Addr:    fmt.Sprintf(":%d", serverCfg.Server.Port),
			Handler: handler,
		}
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	// Wait for interrupt
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	<-sigCh

	log.Println("shutting down...")
	return nil
}

func runPipelineForScheduler(cfg *config.PipelineConfig, projectRoot, envName string, envCfg *config.EnvironmentConfig, logStore *logging.Store) {
	if envCfg != nil {
		merged, err := config.MergeEnvironment(cfg, envCfg, envName)
		if err == nil {
			cfg = merged
		}
	}
	log.Printf("scheduled run: %s", cfg.Pipeline)
}

func runPipelineForTrigger(cfg *config.PipelineConfig, projectRoot, runID, envName string, envCfg *config.EnvironmentConfig, logStore *logging.Store, req server.TriggerRequest) {
	if envCfg != nil {
		merged, err := config.MergeEnvironment(cfg, envCfg, envName)
		if err == nil {
			cfg = merged
		}
	}
	log.Printf("triggered run: %s (run_id=%s)", cfg.Pipeline, runID)
}
