package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/spf13/cobra"

	"github.com/analytehealth/granicus/internal/backup"
	"github.com/analytehealth/granicus/internal/checker"
	"github.com/analytehealth/granicus/internal/config"
	"github.com/analytehealth/granicus/internal/events"
	"github.com/analytehealth/granicus/internal/executor"
	"github.com/analytehealth/granicus/internal/gc"
	"github.com/analytehealth/granicus/internal/graph"
	"github.com/analytehealth/granicus/internal/pool"
	"github.com/analytehealth/granicus/internal/rerun"
	"github.com/analytehealth/granicus/internal/runner"
	"github.com/analytehealth/granicus/internal/state"
	"github.com/analytehealth/granicus/internal/testmode"
)

var version = "0.2.0"

var (
	greenCheck  = color.New(color.FgGreen).Sprint("\u2713")
	redCross    = color.New(color.FgRed).Sprint("\u2717")
	yellowCirc  = color.New(color.FgYellow).Sprint("\u25CB")
	whiteBullet = color.New(color.FgWhite).Sprint("\u25CF")
)

func main() {
	rootCmd := &cobra.Command{
		Use:          "granicus",
		Short:        "A lightweight asset-oriented data pipeline orchestrator",
		SilenceUsage: true,
	}

	runCmd := &cobra.Command{
		Use:   "run <config.yaml>",
		Short: "Execute a pipeline",
		Args:  cobra.ExactArgs(1),
		RunE:  runRun,
	}
	runCmd.Flags().Int("max-parallel", 0, "Override max_parallel from config")
	runCmd.Flags().String("assets", "", "Run only these assets and their dependencies (comma-separated)")
	runCmd.Flags().String("project-root", ".", "Project root directory")
	runCmd.Flags().String("from-failure", "", "Re-run from a failed run ID")
	runCmd.Flags().String("from-date", "", "Override start_date for incremental assets (YYYY-MM-DD)")
	runCmd.Flags().String("to-date", "", "Override end date for incremental assets (YYYY-MM-DD)")
	runCmd.Flags().Bool("full-refresh", false, "Invalidate interval state and reprocess from start")
	runCmd.Flags().Bool("test", false, "Run in test mode (creates temporary dataset)")
	runCmd.Flags().String("test-window", "", "Test window duration (e.g., 7d, 4w, 3m)")
	runCmd.Flags().Bool("keep-test-data", false, "Preserve test dataset after run")

	validateCmd := &cobra.Command{
		Use:   "validate <config.yaml>",
		Short: "Validate pipeline config and graph",
		Args:  cobra.ExactArgs(1),
		RunE:  runValidate,
	}
	validateCmd.Flags().String("project-root", ".", "Project root directory")

	statusCmd := &cobra.Command{
		Use:   "status [run_id]",
		Short: "Show status of a run",
		Args:  cobra.MaximumNArgs(1),
		RunE:  runStatus,
	}
	statusCmd.Flags().String("project-root", ".", "Project root directory")

	historyCmd := &cobra.Command{
		Use:   "history",
		Short: "List recent runs",
		RunE:  runHistory,
	}
	historyCmd.Flags().Int("limit", 10, "Number of runs to show")
	historyCmd.Flags().String("project-root", ".", "Project root directory")

	versionCmd := &cobra.Command{
		Use:   "version",
		Short: "Print version",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("granicus %s\n", version)
		},
	}

	gcCmd := &cobra.Command{
		Use:   "gc",
		Short: "Clean up old run logs and test artifacts",
		RunE:  runGC,
	}
	gcCmd.Flags().Int("retention-days", 30, "Delete runs older than this many days")
	gcCmd.Flags().String("project-root", ".", "Project root directory")

	backupCmd := &cobra.Command{
		Use:   "backup",
		Short: "Backup the state store",
		RunE:  runBackup,
	}
	backupCmd.Flags().String("project-root", ".", "Project root directory")
	backupCmd.Flags().String("output", "", "Output path (default: alongside state.db)")
	backupCmd.Flags().Int("keep", 7, "Number of backups to retain")

	eventsCmd := &cobra.Command{
		Use:   "events",
		Short: "Query the event store",
		RunE:  runEvents,
	}
	eventsCmd.Flags().String("project-root", ".", "Project root directory")
	eventsCmd.Flags().String("run-id", "", "Filter by run ID")
	eventsCmd.Flags().String("asset", "", "Filter by asset")
	eventsCmd.Flags().String("type", "", "Filter by event type (comma-separated)")
	eventsCmd.Flags().String("pipeline", "", "Filter by pipeline")
	eventsCmd.Flags().String("since", "", "Show events since duration (e.g., 24h, 7d)")
	eventsCmd.Flags().Int("limit", 50, "Maximum events to show")
	eventsCmd.Flags().Bool("json", false, "Output as JSON")

	modelsCmd := &cobra.Command{
		Use:   "models [asset_name]",
		Short: "Show model registry and version history",
		Args:  cobra.MaximumNArgs(1),
		RunE:  runModels,
	}
	modelsCmd.Flags().String("project-root", ".", "Project root directory")
	modelsCmd.Flags().String("diff", "", "Show diff between two versions (e.g., 1,2)")

	rootCmd.AddCommand(runCmd, validateCmd, statusCmd, historyCmd, versionCmd, newServeCmd(), gcCmd, backupCmd, eventsCmd, modelsCmd)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func loadAndBuild(configPath, projectRoot string) (*config.PipelineConfig, *graph.Graph, []string, error) {
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("config: %w", err)
	}

	deps, directives, err := graph.ParseAllDirectives(cfg, projectRoot)
	if err != nil {
		return cfg, nil, nil, fmt.Errorf("dependencies: %w", err)
	}

	inputs := graph.ConfigToAssetInputs(cfg)

	// Apply directives (time_column, interval_unit, etc.) to asset inputs
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

	// Generate check nodes and merge into graph
	checkNodes, checkDeps := checker.GenerateCheckNodes(cfg)
	inputs = append(inputs, checkNodes...)
	for k, v := range checkDeps {
		deps[k] = v
	}

	// Generate default checks based on layer/grain
	defaultNodes, defaultDeps := checker.GenerateDefaultCheckNodes(cfg)
	inputs = append(inputs, defaultNodes...)
	for k, v := range defaultDeps {
		deps[k] = v
	}

	g, err := graph.BuildGraph(inputs, deps)
	if err != nil {
		return cfg, nil, nil, fmt.Errorf("graph: %w", err)
	}

	var missingFiles []string
	for _, a := range cfg.Assets {
		path := filepath.Join(projectRoot, a.Source)
		if _, err := os.Stat(path); os.IsNotExist(err) {
			missingFiles = append(missingFiles, a.Source)
		}
	}

	return cfg, g, missingFiles, nil
}

func buildRegistry(cfg *config.PipelineConfig, projectRoot string) *runner.RunnerRegistry {
	reg := runner.NewRunnerRegistry(cfg.Connections)

	// Load template functions
	funcMap := runner.BuiltinFuncMap()
	if cfg.FunctionsDir != "" {
		funcDir := cfg.FunctionsDir
		if !filepath.IsAbs(funcDir) {
			funcDir = filepath.Join(projectRoot, funcDir)
		}
		userFuncs, err := runner.LoadFunctions(funcDir)
		if err != nil {
			log.Printf("warning: loading functions from %s: %v", funcDir, err)
		} else {
			funcMap = runner.MergeFuncMaps(funcMap, userFuncs)
		}
	}

	// Build ref() function with pipeline context
	defaultDS := ""
	if cfg.Connections != nil {
		for _, conn := range cfg.Connections {
			if conn.Type == "bigquery" {
				defaultDS = conn.Properties["dataset"]
				break
			}
		}
	}
	var refAssets []runner.RefAsset
	for _, a := range cfg.Assets {
		ra := runner.RefAsset{Name: a.Name, Layer: a.Layer}
		ra.Dataset = cfg.DatasetForAsset(a, defaultDS)
		refAssets = append(refAssets, ra)
	}
	refFunc := runner.BuildRefFunc(runner.RefContext{
		Assets:         refAssets,
		Datasets:       cfg.Datasets,
		DefaultDataset: defaultDS,
		Prefix:         cfg.Prefix,
	})
	funcMap["ref"] = refFunc

	// Register runners per connection type
	if cfg.Connections != nil {
		for _, conn := range cfg.Connections {
			switch conn.Type {
			case "bigquery":
				sqlR := runner.NewSQLRunner(conn)
				sqlR.FuncMap = funcMap
				reg.Register("sql", sqlR)
				checkR := runner.NewSQLCheckRunner(conn)
				checkR.FuncMap = funcMap
				reg.Register("sql_check", checkR)
			case "gcs":
				reg.Register("gcs", runner.NewGCSRunner(conn))
			case "s3":
				reg.Register("s3", runner.NewS3Runner(conn))
			case "iceberg":
				reg.Register("iceberg", runner.NewIcebergRunner(conn))
			}
		}
	}

	// Register python/dlt runners
	reg.Register("python", runner.NewPythonRunner(nil, nil))
	reg.Register("python_check", runner.NewPythonCheckRunner(nil, nil))
	reg.Register("dlt", runner.NewDLTRunner(nil, nil))

	return reg
}

func findAssetConfig(cfg *config.PipelineConfig, name string) *config.AssetConfig {
	for i := range cfg.Assets {
		if cfg.Assets[i].Name == name {
			return &cfg.Assets[i]
		}
	}
	return nil
}

func connectionForAsset(cfg *config.PipelineConfig, asset *config.AssetConfig) *config.ConnectionConfig {
	connName := asset.DestinationConnection
	if connName == "" {
		return nil
	}
	if conn, ok := cfg.Connections[connName]; ok {
		return conn
	}
	return nil
}

func runRun(cmd *cobra.Command, args []string) error {
	projectRoot, _ := cmd.Flags().GetString("project-root")
	maxParallel, _ := cmd.Flags().GetInt("max-parallel")
	assetsFlag, _ := cmd.Flags().GetString("assets")
	fromFailure, _ := cmd.Flags().GetString("from-failure")
	fromDate, _ := cmd.Flags().GetString("from-date")
	toDate, _ := cmd.Flags().GetString("to-date")
	fullRefresh, _ := cmd.Flags().GetBool("full-refresh")
	testMode, _ := cmd.Flags().GetBool("test")
	testWindow, _ := cmd.Flags().GetString("test-window")
	keepTestData, _ := cmd.Flags().GetBool("keep-test-data")

	if testWindow != "" && !testMode {
		return fmt.Errorf("--test-window requires --test")
	}
	if keepTestData && !testMode {
		return fmt.Errorf("--keep-test-data requires --test")
	}

	var testStart, testEnd string
	if testMode {
		var err error
		testStart, testEnd, err = runner.ParseTestWindow(testWindow)
		if err != nil {
			return fmt.Errorf("test window: %w", err)
		}
	}

	cfg, g, _, err := loadAndBuild(args[0], projectRoot)
	if err != nil {
		return err
	}

	if maxParallel > 0 {
		cfg.MaxParallel = maxParallel
	}

	var assetFilter []string
	if fromFailure != "" && assetsFlag != "" {
		return fmt.Errorf("--from-failure and --assets are mutually exclusive")
	}

	// Open event store
	eventsDBPath := filepath.Join(projectRoot, ".granicus", "events.db")
	eventStore, err := events.New(eventsDBPath)
	if err != nil {
		return fmt.Errorf("event store: %w", err)
	}
	defer eventStore.Close()

	if fromFailure != "" {
		rerunAssets, warnings, err := rerun.ComputeRerunSet(eventStore, fromFailure, g)
		if err != nil {
			return fmt.Errorf("from-failure: %w", err)
		}
		for _, w := range warnings {
			fmt.Printf("Warning: %s\n", w)
		}
		assetFilter = rerunAssets
		fmt.Printf("Re-running from failure %s: %d nodes\n\n", fromFailure, len(assetFilter))
	} else if assetsFlag != "" {
		assetFilter = strings.Split(assetsFlag, ",")
	}

	fmt.Printf("Pipeline: %s\n", cfg.Pipeline)
	fmt.Printf("Assets: %d (%d root nodes)\n", len(g.Assets), len(g.RootNodes))
	fmt.Printf("Max parallel: %d\n\n", cfg.MaxParallel)

	runID := events.GenerateRunID()
	registry := buildRegistry(cfg, projectRoot)

	// Initialize state store
	stateDBName := "state.db"
	if testMode {
		stateDBName = "test-state.db"
	}
	stateDBPath := filepath.Join(projectRoot, ".granicus", stateDBName)
	stateStore, err := state.New(stateDBPath)
	if err != nil {
		return fmt.Errorf("state store: %w", err)
	}
	defer stateStore.Close()

	// Test mode: create temporary dataset and override connection properties
	if testMode {
		for _, conn := range cfg.Connections {
			if conn.Type == "bigquery" {
				baseDataset := conn.Properties["dataset"]
				testDatasetName := testmode.TestDatasetName(baseDataset, runID)
				fmt.Printf("Test mode: using dataset %s\n", testDatasetName)
				conn.Properties["dataset"] = testDatasetName
				break
			}
		}
		// Rebuild registry with updated connection properties
		registry = buildRegistry(cfg, projectRoot)
	}

	// Emit run_started event
	_ = eventStore.Emit(events.Event{
		RunID: runID, Pipeline: cfg.Pipeline, EventType: "run_started", Severity: "info",
		Summary: fmt.Sprintf("Pipeline %s started", cfg.Pipeline),
		Details: map[string]any{
			"asset_count":  len(g.Assets),
			"max_parallel": cfg.MaxParallel,
			"asset_filter": assetFilter,
			"test_mode":    testMode,
			"full_refresh": fullRefresh,
		},
	})

	runnerFunc := func(asset *graph.Asset, pr string, rid string) executor.NodeResult {
		ts := time.Now().Format("15:04:05")
		fmt.Printf("[%s] %s %-24s started\n", ts, whiteBullet, asset.Name)

		_ = eventStore.Emit(events.Event{
			RunID: runID, Pipeline: cfg.Pipeline, Asset: asset.Name,
			EventType: "node_started", Severity: "info",
			Summary: fmt.Sprintf("Node %s started", asset.Name),
		})

		// Model version tracking
		if asset.Source != "" {
			srcPath := filepath.Join(pr, asset.Source)
			if hash, herr := events.HashFile(srcPath); herr == nil {
				eventStore.RecordModelVersion(asset.Name, srcPath, hash, runID)
			}
		}

		// Resolve per-asset dataset from layer routing
		assetCfg := findAssetConfig(cfg, asset.Name)
		resolvedDataset := ""
		if assetCfg != nil {
			defaultDS := ""
			if conn := connectionForAsset(cfg, assetCfg); conn != nil {
				defaultDS = conn.Properties["dataset"]
			}
			resolvedDataset = cfg.DatasetForAsset(*assetCfg, defaultDS)
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
			TestStart:             asset.TestStart,
			TestEnd:               asset.TestEnd,
			Dataset:               resolvedDataset,
			Layer:                 asset.Layer,
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

		ts = time.Now().Format("15:04:05")
		switch r.Status {
		case "success":
			fmt.Printf("[%s] %s %-24s success (%.1fs)\n", ts, greenCheck, r.AssetName, r.Duration.Seconds())
		case "failed":
			fmt.Printf("[%s] %s %-24s failed (%.1fs) -- %s\n", ts, redCross, r.AssetName, r.Duration.Seconds(), r.Error)
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

	// Build pool manager and asset-pool mappings
	poolMgr, assetPools := buildPoolManager(cfg)

	runCfg := executor.RunConfig{
		MaxParallel:  cfg.MaxParallel,
		Assets:       assetFilter,
		ProjectRoot:  projectRoot,
		RunID:        runID,
		FromDate:     fromDate,
		ToDate:       toDate,
		FullRefresh:  fullRefresh,
		StateStore:   stateStore,
		TestMode:     testMode,
		TestStart:    testStart,
		TestEnd:      testEnd,
		KeepTestData: keepTestData,
		PoolManager:  poolMgr,
		AssetPools:   assetPools,
	}

	rr := executor.Execute(g, runCfg, runnerFunc)

	for _, r := range rr.Results {
		if r.Status == "skipped" {
			ts := time.Now().Format("15:04:05")
			fmt.Printf("[%s] %s %-24s skipped -- dependency failed\n", ts, yellowCirc, r.AssetName)

			_ = eventStore.Emit(events.Event{
				RunID: runID, Pipeline: cfg.Pipeline, Asset: r.AssetName,
				EventType: "node_skipped", Severity: "warning",
				Summary: fmt.Sprintf("Node %s skipped: dependency failed", r.AssetName),
				Details: map[string]any{"error_message": r.Error},
			})
		}
	}

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

	totalDuration := rr.EndTime.Sub(rr.StartTime)
	status := "success"
	if failed > 0 || skipped > 0 {
		status = "completed_with_failures"
	}

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

	fmt.Printf("\nRun complete: %d succeeded, %d failed, %d skipped (%.0fs total)\n", succeeded, failed, skipped, totalDuration.Seconds())
	fmt.Printf("Run ID: %s\n", runID)

	if failed > 0 {
		return fmt.Errorf("%d node(s) failed", failed)
	}
	return nil
}

func runValidate(cmd *cobra.Command, args []string) error {
	projectRoot, _ := cmd.Flags().GetString("project-root")

	cfg, g, missingFiles, err := loadAndBuild(args[0], projectRoot)

	if cfg == nil {
		if err != nil {
			return err
		}
		return fmt.Errorf("failed to load config")
	}

	fmt.Printf("Pipeline: %s\n", cfg.Pipeline)
	fmt.Printf("Assets: %d\n", len(cfg.Assets))

	if g != nil {
		depCount := 0
		for _, a := range g.Assets {
			depCount += len(a.DependsOn)
		}
		fmt.Printf("Dependencies: %d\n", depCount)
		fmt.Printf("Root nodes: %d\n", len(g.RootNodes))
	}

	fmt.Println("\nValidation:")

	hasErrors := false

	if err != nil {
		errStr := err.Error()
		if strings.Contains(errStr, "cycle") {
			fmt.Printf("  %s Cycle detected: %s\n", redCross, errStr)
			hasErrors = true
		}
		if strings.Contains(errStr, "depends on") {
			fmt.Printf("  %s %s\n", redCross, errStr)
			hasErrors = true
		}
		if strings.Contains(errStr, "missing source") {
			fmt.Printf("  %s %s\n", redCross, errStr)
			hasErrors = true
		}
		if !hasErrors {
			fmt.Printf("  %s %s\n", redCross, errStr)
			hasErrors = true
		}
	} else {
		fmt.Printf("  %s No cycles detected\n", greenCheck)
		fmt.Printf("  %s All dependencies resolved\n", greenCheck)

		if len(missingFiles) > 0 {
			for _, f := range missingFiles {
				fmt.Printf("  %s Source file not found: %s\n", redCross, f)
			}
			hasErrors = true
		} else {
			fmt.Printf("  %s All source files exist\n", greenCheck)
		}

		fmt.Printf("  %s No duplicate asset names\n", greenCheck)

		// Report functions
		funcMap := runner.BuiltinFuncMap()
		if cfg.FunctionsDir != "" {
			funcDir := cfg.FunctionsDir
			if !filepath.IsAbs(funcDir) {
				funcDir = filepath.Join(projectRoot, funcDir)
			}
			userFuncs, fErr := runner.LoadFunctions(funcDir)
			if fErr != nil {
				fmt.Printf("  %s Functions dir error: %v\n", redCross, fErr)
				hasErrors = true
			} else {
				funcMap = runner.MergeFuncMaps(funcMap, userFuncs)
			}
		}
		funcNames := make([]string, 0, len(funcMap))
		for name := range funcMap {
			funcNames = append(funcNames, name)
		}
		if len(funcNames) > 0 {
			fmt.Printf("  %s Functions: %d (%s)\n", greenCheck, len(funcNames), strings.Join(funcNames, ", "))
		}
	}

	if hasErrors {
		fmt.Println("\nGraph is invalid.")
		return fmt.Errorf("validation failed")
	}

	fmt.Println("\nGraph is valid.")
	return nil
}

func runStatus(cmd *cobra.Command, args []string) error {
	projectRoot, _ := cmd.Flags().GetString("project-root")
	eventsDBPath := filepath.Join(projectRoot, ".granicus", "events.db")
	eventStore, err := events.New(eventsDBPath)
	if err != nil {
		return fmt.Errorf("event store: %w", err)
	}
	defer eventStore.Close()

	var runID string
	if len(args) > 0 {
		runID = args[0]
	} else {
		runs, err := eventStore.ListRuns(1)
		if err != nil || len(runs) == 0 {
			return fmt.Errorf("no runs found")
		}
		runID = runs[0].RunID
	}

	summary, err := eventStore.GetRunSummary(runID)
	if err != nil {
		return fmt.Errorf("reading run %s: %w", runID, err)
	}

	fmt.Printf("Run: %s\n", summary.RunID)
	fmt.Printf("Pipeline: %s\n", summary.Pipeline)
	fmt.Printf("Status: %s\n", summary.Status)
	fmt.Printf("Duration: %.0fs\n", summary.DurationSeconds)
	fmt.Printf("Nodes: %d succeeded, %d failed, %d skipped\n", summary.Succeeded, summary.Failed, summary.Skipped)

	nodes, err := eventStore.GetNodeResults(runID)
	if err != nil {
		return nil
	}

	var failedNodes, skippedNodes []events.NodeResult
	for _, n := range nodes {
		switch n.Status {
		case "failed":
			failedNodes = append(failedNodes, n)
		case "skipped":
			skippedNodes = append(skippedNodes, n)
		}
	}

	if len(failedNodes) > 0 {
		fmt.Println("\nFailed:")
		for _, n := range failedNodes {
			fmt.Printf("  %s -- %s\n", n.Asset, n.Error)
		}
	}
	if len(skippedNodes) > 0 {
		fmt.Println("\nSkipped:")
		for _, n := range skippedNodes {
			fmt.Printf("  %s -- %s\n", n.Asset, n.Error)
		}
	}

	return nil
}

func runHistory(cmd *cobra.Command, args []string) error {
	projectRoot, _ := cmd.Flags().GetString("project-root")
	limit, _ := cmd.Flags().GetInt("limit")
	eventsDBPath := filepath.Join(projectRoot, ".granicus", "events.db")
	eventStore, err := events.New(eventsDBPath)
	if err != nil {
		return fmt.Errorf("event store: %w", err)
	}
	defer eventStore.Close()

	runs, err := eventStore.ListRuns(limit)
	if err != nil {
		return err
	}

	if len(runs) == 0 {
		fmt.Println("No runs found.")
		return nil
	}

	fmt.Printf("%-32s %-16s %-24s %-10s %s\n", "Run ID", "Pipeline", "Status", "Duration", "Date")
	for _, r := range runs {
		fmt.Printf("%-32s %-16s %-24s %-10s %s\n",
			r.RunID,
			r.Pipeline,
			r.Status,
			fmt.Sprintf("%.0fs", r.DurationSeconds),
			r.StartTime.Format("2006-01-02 15:04"),
		)
	}

	return nil
}

func runGC(cmd *cobra.Command, args []string) error {
	projectRoot, _ := cmd.Flags().GetString("project-root")
	retentionDays, _ := cmd.Flags().GetInt("retention-days")

	// Clean old JSONL runs (legacy)
	result, err := gc.Collect(projectRoot, retentionDays)
	if err != nil {
		return err
	}

	if result.RunsDeleted > 0 {
		fmt.Printf("Deleted %d legacy runs, freed %s\n", result.RunsDeleted, gc.FormatBytes(result.BytesFreed))
	}
	if result.TestCleanup > 0 {
		fmt.Printf("Cleaned up %d test artifacts\n", result.TestCleanup)
	}

	// Clean events
	eventsDBPath := filepath.Join(projectRoot, ".granicus", "events.db")
	if _, statErr := os.Stat(eventsDBPath); statErr == nil {
		eventStore, err := events.New(eventsDBPath)
		if err != nil {
			return fmt.Errorf("event store: %w", err)
		}
		defer eventStore.Close()

		cutoff := time.Now().AddDate(0, 0, -retentionDays)
		deleted, err := eventStore.DeleteBefore(cutoff)
		if err != nil {
			return fmt.Errorf("event gc: %w", err)
		}
		if deleted > 0 {
			fmt.Printf("Deleted %d events older than %d days\n", deleted, retentionDays)
		}

		// Report DB size
		if info, err := os.Stat(eventsDBPath); err == nil {
			fmt.Printf("Events DB: %s\n", gc.FormatBytes(info.Size()))
		}
	}

	return nil
}

func runBackup(cmd *cobra.Command, args []string) error {
	projectRoot, _ := cmd.Flags().GetString("project-root")
	output, _ := cmd.Flags().GetString("output")
	keep, _ := cmd.Flags().GetInt("keep")

	stateDBPath := filepath.Join(projectRoot, ".granicus", "state.db")

	backupPath, err := backup.BackupStateDB(stateDBPath, output)
	if err != nil {
		return err
	}
	fmt.Printf("State backup: %s\n", backupPath)

	// Backup events.db
	eventsDBPath := filepath.Join(projectRoot, ".granicus", "events.db")
	if _, statErr := os.Stat(eventsDBPath); statErr == nil {
		eventsBackup, err := backup.BackupStateDB(eventsDBPath, "")
		if err != nil {
			fmt.Printf("Warning: events backup failed: %v\n", err)
		} else {
			fmt.Printf("Events backup: %s\n", eventsBackup)
		}
	}

	if keep > 0 {
		pruned, err := backup.PruneBackups(filepath.Dir(backupPath), keep)
		if err != nil {
			return fmt.Errorf("pruning: %w", err)
		}
		if pruned > 0 {
			fmt.Printf("Pruned %d old backups\n", pruned)
		}
	}

	return nil
}

func runEvents(cmd *cobra.Command, args []string) error {
	projectRoot, _ := cmd.Flags().GetString("project-root")
	runID, _ := cmd.Flags().GetString("run-id")
	asset, _ := cmd.Flags().GetString("asset")
	eventType, _ := cmd.Flags().GetString("type")
	pipeline, _ := cmd.Flags().GetString("pipeline")
	since, _ := cmd.Flags().GetString("since")
	limit, _ := cmd.Flags().GetInt("limit")
	asJSON, _ := cmd.Flags().GetBool("json")

	eventsDBPath := filepath.Join(projectRoot, ".granicus", "events.db")
	if _, err := os.Stat(eventsDBPath); os.IsNotExist(err) {
		fmt.Println("No events found (events.db does not exist).")
		return nil
	}

	eventStore, err := events.New(eventsDBPath)
	if err != nil {
		return fmt.Errorf("event store: %w", err)
	}
	defer eventStore.Close()

	filters := events.QueryFilters{
		RunID:     runID,
		Pipeline:  pipeline,
		Asset:     asset,
		EventType: eventType,
		Limit:     limit,
	}

	if since != "" {
		dur, err := parseDuration(since)
		if err != nil {
			return fmt.Errorf("invalid --since: %w", err)
		}
		filters.Since = time.Now().Add(-dur)
	}

	results, err := eventStore.Query(filters)
	if err != nil {
		return err
	}

	if len(results) == 0 {
		fmt.Println("No events found.")
		return nil
	}

	if asJSON {
		for _, e := range results {
			data, _ := json.Marshal(e)
			fmt.Println(string(data))
		}
		return nil
	}

	fmt.Printf("%-20s %-20s %-24s %s\n", "Timestamp", "Type", "Asset", "Summary")
	for _, e := range results {
		asset := e.Asset
		if asset == "" {
			asset = "-"
		}
		fmt.Printf("%-20s %-20s %-24s %s\n",
			e.Timestamp.Format("2006-01-02 15:04:05"),
			e.EventType,
			asset,
			e.Summary,
		)
	}
	return nil
}

func runModels(cmd *cobra.Command, args []string) error {
	projectRoot, _ := cmd.Flags().GetString("project-root")
	diffFlag, _ := cmd.Flags().GetString("diff")

	eventsDBPath := filepath.Join(projectRoot, ".granicus", "events.db")
	if _, err := os.Stat(eventsDBPath); os.IsNotExist(err) {
		fmt.Println("No models found (events.db does not exist).")
		return nil
	}

	eventStore, err := events.New(eventsDBPath)
	if err != nil {
		return fmt.Errorf("event store: %w", err)
	}
	defer eventStore.Close()

	if len(args) == 0 {
		models, err := eventStore.ListModels()
		if err != nil {
			return err
		}
		if len(models) == 0 {
			fmt.Println("No models registered.")
			return nil
		}

		fmt.Printf("%-32s %-8s %-10s %s\n", "Asset", "Version", "Hash", "Last Run")
		for _, m := range models {
			hash := m.SourceHash
			if len(hash) > 8 {
				hash = hash[:8]
			}
			fmt.Printf("%-32s v%-7d %-10s %s\n", m.AssetName, m.Version, hash, m.ActivatedAt[:19])
		}
		return nil
	}

	assetName := args[0]

	if diffFlag != "" {
		var v1, v2 int
		if _, err := fmt.Sscanf(diffFlag, "%d,%d", &v1, &v2); err != nil {
			return fmt.Errorf("--diff expects N,M (e.g., --diff 1,2)")
		}
		history, err := eventStore.GetModelHistory(assetName)
		if err != nil {
			return err
		}
		var src1, src2 string
		for _, h := range history {
			if h.Version == v1 {
				src1 = h.SourceSnapshot
			}
			if h.Version == v2 {
				src2 = h.SourceSnapshot
			}
		}
		if src1 == "" || src2 == "" {
			return fmt.Errorf("version not found")
		}
		// Simple line-by-line diff
		lines1 := strings.Split(src1, "\n")
		lines2 := strings.Split(src2, "\n")
		fmt.Printf("--- %s v%d\n+++ %s v%d\n", assetName, v1, assetName, v2)
		maxLen := len(lines1)
		if len(lines2) > maxLen {
			maxLen = len(lines2)
		}
		for i := 0; i < maxLen; i++ {
			l1, l2 := "", ""
			if i < len(lines1) {
				l1 = lines1[i]
			}
			if i < len(lines2) {
				l2 = lines2[i]
			}
			if l1 != l2 {
				if l1 != "" {
					fmt.Printf("-%s\n", l1)
				}
				if l2 != "" {
					fmt.Printf("+%s\n", l2)
				}
			} else {
				fmt.Printf(" %s\n", l1)
			}
		}
		return nil
	}

	history, err := eventStore.GetModelHistory(assetName)
	if err != nil {
		return err
	}
	if len(history) == 0 {
		return fmt.Errorf("no history for %s", assetName)
	}

	fmt.Printf("Model: %s\n\n", assetName)
	fmt.Printf("%-8s %-10s %-20s %-32s %s\n", "Version", "Hash", "Activated", "Run", "Replaced")
	for _, h := range history {
		hash := h.SourceHash
		if len(hash) > 8 {
			hash = hash[:8]
		}
		replaced := "-"
		if h.ReplacedAt != "" {
			replaced = h.ReplacedAt[:19]
		}
		activated := h.ActivatedAt
		if len(activated) > 19 {
			activated = activated[:19]
		}
		fmt.Printf("v%-7d %-10s %-20s %-32s %s\n", h.Version, hash, activated, h.ActivatedRun, replaced)
	}
	return nil
}

func buildPoolManager(cfg *config.PipelineConfig) (*pool.PoolManager, map[string]string) {
	if len(cfg.Pools) == 0 {
		return nil, nil
	}

	poolConfigs := make(map[string]pool.PoolConfig, len(cfg.Pools))
	for name, pc := range cfg.Pools {
		var timeout time.Duration
		if pc.Timeout != "" {
			timeout, _ = time.ParseDuration(pc.Timeout) // already validated in LoadConfig
		}
		poolConfigs[name] = pool.PoolConfig{
			Slots:         pc.Slots,
			ParsedTimeout: timeout,
			DefaultFor:    pc.DefaultFor,
		}
	}

	assetPools := make(map[string]string, len(cfg.Assets))
	for _, a := range cfg.Assets {
		if p := config.ResolveAssetPool(a, cfg.Pools, cfg.Connections); p != "" {
			assetPools[a.Name] = p
		}
	}

	return pool.NewPoolManager(poolConfigs), assetPools
}

func parseDuration(s string) (time.Duration, error) {
	// Try standard Go duration first (24h, 1h30m, etc.)
	if d, err := time.ParseDuration(s); err == nil {
		return d, nil
	}
	// Try custom formats: 7d, 30d
	if len(s) > 1 && s[len(s)-1] == 'd' {
		var n int
		if _, err := fmt.Sscanf(s[:len(s)-1], "%d", &n); err == nil {
			return time.Duration(n) * 24 * time.Hour, nil
		}
	}
	return 0, fmt.Errorf("invalid duration: %q (use e.g., 24h, 7d, 30d)", s)
}
