package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"cloud.google.com/go/auth/credentials"
	"cloud.google.com/go/auth/oauth2adapt"
	"cloud.google.com/go/bigquery"
	"github.com/fatih/color"
	"github.com/spf13/cobra"
	"google.golang.org/api/option"

	"github.com/DataDecodeHQ/granicus/internal/backup"
	"github.com/DataDecodeHQ/granicus/internal/config"
	"github.com/DataDecodeHQ/granicus/internal/doctor"
	"github.com/DataDecodeHQ/granicus/internal/events"
	"github.com/DataDecodeHQ/granicus/internal/executor"
	"github.com/DataDecodeHQ/granicus/internal/gc"
	"github.com/DataDecodeHQ/granicus/internal/graph"
	"github.com/DataDecodeHQ/granicus/internal/logging"
	"github.com/DataDecodeHQ/granicus/internal/migrate"
	"github.com/DataDecodeHQ/granicus/internal/monitor"
	"github.com/DataDecodeHQ/granicus/internal/pool"
	"github.com/DataDecodeHQ/granicus/internal/rerun"
	"github.com/DataDecodeHQ/granicus/internal/runner"
	"github.com/DataDecodeHQ/granicus/internal/state"
	"github.com/DataDecodeHQ/granicus/internal/testmode"
	"github.com/DataDecodeHQ/granicus/internal/validate"
)

const version = "0.2.0"

var (
	greenCheck  = color.New(color.FgGreen).Sprint("\u2713")
	redCross    = color.New(color.FgRed).Sprint("\u2717")
	yellowCirc  = color.New(color.FgYellow).Sprint("\u25CB")
	whiteBullet = color.New(color.FgWhite).Sprint("\u25CF")
)

func statusIcon(status interface{}) string {
	s := fmt.Sprintf("%v", status)
	switch s {
	case "pass", "ok":
		return greenCheck
	case "fail", "error":
		return redCross
	case "warn", "warning":
		return yellowCirc
	default:
		return whiteBullet
	}
}

type jsonRunOutput struct {
	RunID           string        `json:"run_id"`
	Pipeline        string        `json:"pipeline"`
	Status          string        `json:"status"`
	DurationSeconds float64       `json:"duration_seconds"`
	Succeeded       int           `json:"succeeded"`
	Failed          int           `json:"failed"`
	Skipped         int           `json:"skipped"`
	TotalAssets     int            `json:"total_assets"`
	Interrupted     bool           `json:"interrupted,omitempty"`
	Assets          []jsonRunAsset `json:"assets"`
}

type jsonRunAsset struct {
	Asset           string  `json:"asset"`
	Status          string  `json:"status"`
	DurationSeconds float64 `json:"duration_seconds,omitempty"`
	Error           string  `json:"error,omitempty"`
	Stderr          string  `json:"stderr,omitempty"`
}

type jsonStatusOutput struct {
	RunID           string              `json:"run_id"`
	Pipeline        string              `json:"pipeline"`
	Status          string              `json:"status"`
	StartTime       time.Time           `json:"start_time"`
	EndTime         time.Time           `json:"end_time"`
	DurationSeconds float64             `json:"duration_seconds"`
	Succeeded       int                 `json:"succeeded"`
	Failed          int                 `json:"failed"`
	Skipped         int                 `json:"skipped"`
	TotalAssets     int                    `json:"total_assets"`
	Assets          []events.AssetResult   `json:"assets,omitempty"`
}

type jsonErrorOutput struct {
	Error jsonErrorDetail `json:"error"`
}

type jsonErrorDetail struct {
	Code       string         `json:"code"`
	Message    string         `json:"message"`
	Suggestion string         `json:"suggestion,omitempty"`
	Context    map[string]any `json:"context,omitempty"`
}

func logEmit(es *events.Store, event events.Event) {
	if err := es.Emit(event); err != nil {
		slog.Warn("failed to emit event", "event_type", event.EventType, "error", err)
	}
}

func printJSONError(code, message, suggestion string, ctx map[string]any) {
	out := jsonErrorOutput{
		Error: jsonErrorDetail{
			Code:       code,
			Message:    message,
			Suggestion: suggestion,
			Context:    ctx,
		},
	}
	data, _ := json.MarshalIndent(out, "", "  ")
	fmt.Println(string(data))
}

func main() {
	logging.Init(false)

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
	runCmd.Flags().Bool("downstream-only", false, "With --assets, run only downstream dependents (skip upstream)")
	runCmd.Flags().Bool("only", false, "With --assets, run only the named assets (skip upstream and downstream)")
	runCmd.Flags().String("output", "", "Output format (json)")
	runCmd.Flags().Bool("dry-run", false, "Show execution plan without running (assets, intervals, checks)")

	validateCmd := &cobra.Command{
		Use:   "validate <config.yaml>",
		Short: "Validate pipeline config and graph",
		Args:  cobra.ExactArgs(1),
		RunE:  runValidate,
	}
	validateCmd.Flags().String("project-root", ".", "Project root directory")
	validateCmd.Flags().Bool("strict", false, "Promote warnings to errors")
	validateCmd.Flags().Bool("json", false, "Output validation results as JSON")
	validateCmd.Flags().String("output", "", "Output format (json)")
	validateCmd.Flags().Bool("quiet", false, "Only show errors and warnings")

	statusCmd := &cobra.Command{
		Use:   "status [run_id]",
		Short: "Show status of a run",
		Args:  cobra.MaximumNArgs(1),
		RunE:  runStatus,
	}
	statusCmd.Flags().String("project-root", ".", "Project root directory")
	statusCmd.Flags().String("output", "", "Output format (json)")

	historyCmd := &cobra.Command{
		Use:   "history",
		Short: "List recent runs",
		RunE:  runHistory,
	}
	historyCmd.Flags().Int("limit", 10, "Number of runs to show")
	historyCmd.Flags().String("project-root", ".", "Project root directory")
	historyCmd.Flags().String("output", "", "Output format (json)")
	historyCmd.Flags().Bool("costs", false, "Show per-run BQ cost summary (total bytes, cost, cache hit rate)")

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
	eventsCmd.Flags().String("output", "", "Output format (json)")

	modelsCmd := &cobra.Command{
		Use:   "models [asset_name]",
		Short: "Show model registry and version history",
		Args:  cobra.MaximumNArgs(1),
		RunE:  runModels,
	}
	modelsCmd.Flags().String("project-root", ".", "Project root directory")
	modelsCmd.Flags().String("diff", "", "Show diff between two versions (e.g., 1,2)")
	modelsCmd.Flags().String("output", "", "Output format (json)")

	migrateCmd := &cobra.Command{
		Use:   "migrate <config.yaml>",
		Short: "Migrate a pipeline config to the latest format version",
		Args:  cobra.ExactArgs(1),
		RunE:  runMigrate,
	}
	migrateCmd.Flags().Bool("dry-run", false, "Show what would change without modifying the file")
	migrateCmd.Flags().String("from-version", "", "Override detected config version (e.g., 0.2)")

	completionCmd := &cobra.Command{
		Use:   "completion <bash|zsh|fish|powershell>",
		Short: "Generate shell completion script",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runCompletion(rootCmd, args[0])
		},
	}

	doctorCmd := &cobra.Command{
		Use:   "doctor [config.yaml]",
		Short: "Run health checks on the Granicus environment",
		Args:  cobra.MaximumNArgs(1),
		RunE:  runDoctor,
	}
	doctorCmd.Flags().String("project-root", ".", "Project root directory")
	doctorCmd.Flags().String("output", "", "Output format (json)")

	rootCmd.AddCommand(runCmd, validateCmd, statusCmd, historyCmd, versionCmd, newServeCmd(), gcCmd, backupCmd, eventsCmd, modelsCmd, migrateCmd, completionCmd, doctorCmd,
		newPushCmd(), newActivateCmd(), newVersionsCmd(), newDiffCmd(),
		newHistoryCmd2(), newEventsCmd2(), newFailuresCmd(), newStatsCmd(),
		newCloudStatusCmd(), newIntervalsCmd(),
		newTriggerCmd(), newSubscribeCmd(), newConfigCmd(), newLoginCmd())

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func loadAndBuild(configPath, projectRoot string) (*config.PipelineConfig, *graph.Graph, []string, error) {
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("config: %w", err)
	}

	g, _, err := buildPipelineGraph(cfg, projectRoot)
	if err != nil {
		return cfg, nil, nil, err
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
			slog.Warn("loading functions", "dir", funcDir, "error", err)
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

	// Build source() function with pipeline context
	if len(cfg.Sources) > 0 {
		resolvedSources := make(map[string]runner.ResolvedSource, len(cfg.Sources))
		for name, src := range cfg.Sources {
			rs := runner.ResolvedSource{Identifier: src.Identifier}
			if src.Connection != "" {
				if conn, ok := cfg.Connections[src.Connection]; ok {
					rs.ConnectionType = conn.Type
					rs.Project = conn.Properties["project"]
				}
			} else {
				// Default to first bigquery connection
				for _, conn := range cfg.Connections {
					if conn.Type == "bigquery" {
						rs.ConnectionType = "bigquery"
						rs.Project = conn.Properties["project"]
						break
					}
				}
			}
			resolvedSources[name] = rs
		}
		sourceFunc := runner.BuildSourceFunc(runner.SourceContext{Sources: resolvedSources})
		funcMap["source"] = sourceFunc
	}

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
				reg.Register("gcs_export", runner.NewGCSRunner(conn))
				reg.Register("gcs_ingest", runner.NewGCSIngestRunner(conn, nil))
			case "s3":
				reg.Register("s3", runner.NewS3Runner(conn))
			case "iceberg":
				reg.Register("iceberg", runner.NewIcebergRunner(conn))
			}
		}
	}

	// Register python/dlt runners with ref resolution
	pyRunner := runner.NewPythonRunner(nil, nil, nil, "")
	pyRunner.RefFunc = refFunc
	reg.Register("python", pyRunner)
	pyCheckRunner := runner.NewPythonCheckRunner(nil, nil, nil, "")
	pyCheckRunner.SetRefFunc(refFunc)
	reg.Register("python_check", pyCheckRunner)
	dltRunner := runner.NewDLTRunner(nil, nil, nil, "")
	dltRunner.SetRefFunc(refFunc)
	reg.Register("dlt", dltRunner)

	return reg
}

func runDryRun(g *graph.Graph, cfg *config.PipelineConfig, assetFilter []string, downstreamOnly bool, fromDate, toDate string, fullRefresh bool, projectRoot string) error {
	// Determine which nodes to run
	nodesToRun := make(map[string]bool)
	if len(assetFilter) > 0 {
		var subgraph []string
		if downstreamOnly {
			subgraph = g.DownstreamSubgraph(assetFilter)
		} else {
			subgraph = g.Subgraph(assetFilter)
		}
		for _, n := range subgraph {
			nodesToRun[n] = true
		}
	} else {
		for name := range g.Assets {
			nodesToRun[name] = true
		}
	}

	// Topological order, excluding check nodes (shown inline per asset)
	allSorted := g.TopologicalSort()
	var sorted []string
	for _, name := range allSorted {
		if nodesToRun[name] && !strings.HasPrefix(name, "check:") {
			sorted = append(sorted, name)
		}
	}

	// Try to open state store for interval counts (read-only, ignore errors if missing)
	var stateStore *state.Store
	stateDBPath := filepath.Join(projectRoot, ".granicus", "state.db")
	if _, statErr := os.Stat(stateDBPath); statErr == nil {
		stateStore, _ = state.New(stateDBPath) // dag:intentional -- read-only fallback, missing state DB is expected
		if stateStore != nil {
			defer stateStore.Close()
		}
	}

	endDate := toDate
	if endDate == "" {
		endDate = time.Now().UTC().Format("2006-01-02")
	}

	fmt.Printf("Dry run: %s\n", cfg.Pipeline)
	fmt.Printf("Assets to run: %d\n\n", len(sorted))

	const (
		numW      = 4
		nameW     = 32
		typeW     = 10
		intervalW = 22
	)
	sep := strings.Repeat("-", numW+2+nameW+2+typeW+2+intervalW+2+40)
	fmt.Printf("%-*s  %-*s  %-*s  %-*s  %s\n", numW, "#", nameW, "Asset", typeW, "Type", intervalW, "Intervals", "Checks")
	fmt.Println(sep)

	for i, name := range sorted {
		asset := g.Assets[name]

		typeLabel := asset.Type
		if typeLabel == "" {
			typeLabel = "sql"
		}
		if asset.Type == graph.AssetTypeSource {
			typeLabel = "source"
		}

		intervalsLabel := "full"
		if asset.TimeColumn != "" {
			startDate := asset.StartDate
			if fromDate != "" {
				startDate = fromDate
			}
			unit := asset.IntervalUnit
			if unit == "" {
				unit = "day"
			}
			if startDate != "" {
				allIntervals, err := state.GenerateIntervals(startDate, endDate, unit)
				if err == nil {
					if fullRefresh {
						intervalsLabel = fmt.Sprintf("%d total (%s)", len(allIntervals), unit)
					} else {
						pending := len(allIntervals)
						if stateStore != nil {
							completed, cerr := stateStore.GetIntervals(name)
							if cerr == nil {
								missing := state.ComputeMissing(allIntervals, completed, asset.Lookback)
								missing = state.ApplyBatchSize(missing, asset.BatchSize)
								pending = len(missing)
							}
						}
						intervalsLabel = fmt.Sprintf("%d pending (%s)", pending, unit)
					}
				}
			}
		}

		var checkNames []string
		for _, downstream := range asset.DependedOnBy {
			if nodesToRun[downstream] && strings.HasPrefix(downstream, "check:") {
				parts := strings.SplitN(downstream, ":", 3)
				if len(parts) == 3 {
					checkNames = append(checkNames, parts[2])
				} else {
					checkNames = append(checkNames, downstream)
				}
			}
		}
		checksStr := strings.Join(checkNames, ", ")
		if checksStr == "" {
			checksStr = "(none)"
		}

		fmt.Printf("%-*d  %-*s  %-*s  %-*s  %s\n",
			numW, i+1,
			nameW, name,
			typeW, typeLabel,
			intervalW, intervalsLabel,
			checksStr,
		)
	}
	fmt.Println(sep)
	return nil
}

func runRun(cmd *cobra.Command, args []string) error {
	projectRoot, _ := cmd.Flags().GetString("project-root")
	maxParallel, _ := cmd.Flags().GetInt("max-parallel")
	assetsFlag, _ := cmd.Flags().GetString("assets")
	downstreamOnly, _ := cmd.Flags().GetBool("downstream-only")
	only, _ := cmd.Flags().GetBool("only")
	fromFailure, _ := cmd.Flags().GetString("from-failure")
	fromDate, _ := cmd.Flags().GetString("from-date")
	toDate, _ := cmd.Flags().GetString("to-date")
	fullRefresh, _ := cmd.Flags().GetBool("full-refresh")
	testMode, _ := cmd.Flags().GetBool("test")
	testWindow, _ := cmd.Flags().GetString("test-window")
	keepTestData, _ := cmd.Flags().GetBool("keep-test-data")
	outputFormat, _ := cmd.Flags().GetString("output")
	outputJSON := outputFormat == "json"
	dryRun, _ := cmd.Flags().GetBool("dry-run")

	if testWindow != "" && !testMode {
		if outputJSON {
			printJSONError("INVALID_FLAGS", "--test-window requires --test", "Add --test flag to use --test-window", nil)
		}
		return fmt.Errorf("--test-window requires --test")
	}
	if keepTestData && !testMode {
		if outputJSON {
			printJSONError("INVALID_FLAGS", "--keep-test-data requires --test", "Add --test flag to use --keep-test-data", nil)
		}
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
		if outputJSON {
			printJSONError("CONFIG_ERROR", err.Error(), "Check your pipeline configuration file", nil)
		}
		return err
	}

	if maxParallel > 0 {
		cfg.MaxParallel = maxParallel
	}

	if downstreamOnly && assetsFlag == "" {
		return fmt.Errorf("--downstream-only requires --assets")
	}
	if only && assetsFlag == "" {
		return fmt.Errorf("--only requires --assets")
	}
	if only && downstreamOnly {
		return fmt.Errorf("--only and --downstream-only are mutually exclusive")
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
		if !outputJSON {
			for _, w := range warnings {
				fmt.Printf("Warning: %s\n", w)
			}
		}
		assetFilter = rerunAssets
		if !outputJSON {
			fmt.Printf("Re-running from failure %s: %d nodes\n\n", fromFailure, len(assetFilter))
		}
	} else if assetsFlag != "" {
		assetFilter = strings.Split(assetsFlag, ",")
	}

	if dryRun {
		return runDryRun(g, cfg, assetFilter, downstreamOnly, fromDate, toDate, fullRefresh, projectRoot)
	}

	if !outputJSON {
		fmt.Printf("Pipeline: %s\n", cfg.Pipeline)
		fmt.Printf("Assets: %d (%d root nodes)\n", len(g.Assets), len(g.RootNodes))
		fmt.Printf("Max parallel: %d\n\n", cfg.MaxParallel)
	}

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
				if !outputJSON {
					fmt.Printf("Test mode: using dataset %s\n", testDatasetName)
				}
				conn.Properties["dataset"] = testDatasetName
				break
			}
		}
		// Rebuild registry with updated connection properties
		registry = buildRegistry(cfg, projectRoot)
	}

	// Ensure all destination datasets exist before running
	if err := ensureDatasets(cfg, eventStore, runID); err != nil {
		return fmt.Errorf("ensuring datasets: %w", err)
	}

	// Set up graceful shutdown on SIGTERM/SIGINT
	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())
	defer shutdownCancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		select {
		case sig := <-sigCh:
			slog.Info("received signal, initiating graceful shutdown", "signal", sig)
			shutdownCancel()
		case <-shutdownCtx.Done():
		}
	}()
	defer signal.Stop(sigCh)

	// Emit run_started event
	logEmit(eventStore, events.Event{
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

	runnerFunc := buildNodeRunner(cfg, runID, eventStore, registry, nodeRunnerOptions{
		OutputJSON: outputJSON,
	})

	// Build pool manager and asset-pool mappings
	poolMgr, assetPools := buildPoolManager(cfg)

	runCfg := executor.RunConfig{
		MaxParallel:    cfg.MaxParallel,
		Assets:         assetFilter,
		ProjectRoot:    projectRoot,
		RunID:          runID,
		FromDate:       fromDate,
		ToDate:         toDate,
		FullRefresh:    fullRefresh,
		StateStore:     stateStore,
		TestMode:       testMode,
		TestStart:      testStart,
		TestEnd:        testEnd,
		KeepTestData:   keepTestData,
		DownstreamOnly: downstreamOnly,
		Only:           only,
		PoolManager:    poolMgr,
		AssetPools:     assetPools,
		Ctx:            shutdownCtx,
	}

	rr := executor.Execute(g, runCfg, runnerFunc)

	for _, r := range rr.Results {
		if r.Status == "skipped" {
			if !outputJSON {
				ts := time.Now().Format("15:04:05")
				fmt.Printf("[%s] %s %-24s skipped -- dependency failed\n", ts, yellowCirc, r.AssetName)
			}

			logEmit(eventStore, events.Event{
				RunID: runID, Pipeline: cfg.Pipeline, Asset: r.AssetName,
				EventType: "asset_skipped", Severity: "warning",
				Summary: fmt.Sprintf("Asset %s skipped: dependency failed", r.AssetName),
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

	if rr.Interrupted {
		logEmit(eventStore, events.Event{
			RunID: runID, Pipeline: cfg.Pipeline, EventType: "run_interrupted", Severity: "warning",
			DurationMs: totalDuration.Milliseconds(),
			Summary:    fmt.Sprintf("Run interrupted: %d succeeded, %d failed, %d skipped", succeeded, failed, skipped),
			Details: map[string]any{
				"succeeded":        succeeded,
				"failed":           failed,
				"skipped":          skipped,
				"total_nodes":      len(rr.Results),
				"duration_seconds": totalDuration.Seconds(),
			},
		})
		if outputJSON {
			data, _ := json.MarshalIndent(buildRunJSON(runID, cfg.Pipeline, "interrupted", totalDuration.Seconds(), succeeded, failed, skipped, rr), "", "  ")
			fmt.Println(string(data))
		} else {
			fmt.Printf("\nRun interrupted: %d succeeded, %d failed, %d skipped (%.0fs)\n", succeeded, failed, skipped, totalDuration.Seconds())
			fmt.Printf("Run ID: %s\n", runID)
		}
		return fmt.Errorf("run interrupted: %d succeeded, %d failed, %d skipped", succeeded, failed, skipped)
	}

	status := "success"
	if failed > 0 || skipped > 0 {
		status = "completed_with_failures"
	}

	logEmit(eventStore, events.Event{
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

	if outputJSON {
		data, _ := json.MarshalIndent(buildRunJSON(runID, cfg.Pipeline, status, totalDuration.Seconds(), succeeded, failed, skipped, rr), "", "  ")
		fmt.Println(string(data))
	} else {
		fmt.Printf("\nRun complete: %d succeeded, %d failed, %d skipped (%.0fs total)\n", succeeded, failed, skipped, totalDuration.Seconds())
		fmt.Printf("Run ID: %s\n", runID)
	}

	// Send run summary notification
	sendRunAlerts(cfg, runID, eventStore, rr, totalDuration)

	// Post-run hooks: context.db + monitor.db
	runPostRunHooks(cfg, g, projectRoot, rr)

	if failed > 0 {
		return fmt.Errorf("%d node(s) failed", failed)
	}
	return nil
}

func buildRunJSON(runID, pipeline, status string, durationSeconds float64, succeeded, failed, skipped int, rr *executor.RunResult) jsonRunOutput {
	var assets []jsonRunAsset
	for _, r := range rr.Results {
		a := jsonRunAsset{
			Asset:  r.AssetName,
			Status: r.Status,
		}
		if r.Duration > 0 {
			a.DurationSeconds = r.Duration.Seconds()
		}
		if r.Error != "" {
			a.Error = r.Error
		}
		if r.Status == "failed" && r.Stderr != "" {
			a.Stderr = r.Stderr
		}
		assets = append(assets, a)
	}
	return jsonRunOutput{
		RunID:           runID,
		Pipeline:        pipeline,
		Status:          status,
		DurationSeconds: durationSeconds,
		Succeeded:       succeeded,
		Failed:          failed,
		Skipped:         skipped,
		TotalAssets:     len(rr.Results),
		Interrupted:     rr.Interrupted,
		Assets:          assets,
	}
}

func runValidate(cmd *cobra.Command, args []string) error {
	projectRoot, _ := cmd.Flags().GetString("project-root")
	strict, _ := cmd.Flags().GetBool("strict")
	asJSON, _ := cmd.Flags().GetBool("json")
	outputFormat, _ := cmd.Flags().GetString("output")
	if outputFormat == "json" {
		asJSON = true
	}
	quiet, _ := cmd.Flags().GetBool("quiet")

	cfg, g, missingFiles, err := loadAndBuild(args[0], projectRoot)

	if cfg == nil {
		if err != nil {
			if asJSON {
				outputValidateJSON(cfg, nil, true)
			}
			return err
		}
		return fmt.Errorf("failed to load config")
	}

	var allResults []validate.ValidationResult
	hasErrors := false
	hasWarnings := false

	// Phase 1: graph-level checks (from loadAndBuild)
	if err != nil {
		allResults = append(allResults, validate.ValidationResult{
			Name:   "graph",
			Status: validate.StatusError,
			Items:  []string{err.Error()},
		})
		hasErrors = true
	} else {
		allResults = append(allResults, validate.ValidationResult{
			Name:   "cycles",
			Status: validate.StatusPass,
		})
		allResults = append(allResults, validate.ValidationResult{
			Name:   "dependencies",
			Status: validate.StatusPass,
		})

		if len(missingFiles) > 0 {
			allResults = append(allResults, validate.ValidationResult{
				Name:   "source_files",
				Status: validate.StatusError,
				Items:  missingFiles,
			})
			hasErrors = true
		} else {
			allResults = append(allResults, validate.ValidationResult{
				Name:   "source_files",
				Status: validate.StatusPass,
			})
		}

		allResults = append(allResults, validate.ValidationResult{
			Name:   "duplicates",
			Status: validate.StatusPass,
		})

		// Functions check
		funcMap := runner.BuiltinFuncMap()
		if cfg.FunctionsDir != "" {
			funcDir := cfg.FunctionsDir
			if !filepath.IsAbs(funcDir) {
				funcDir = filepath.Join(projectRoot, funcDir)
			}
			userFuncs, fErr := runner.LoadFunctions(funcDir)
			if fErr != nil {
				allResults = append(allResults, validate.ValidationResult{
					Name:   "functions",
					Status: validate.StatusError,
					Items:  []string{fErr.Error()},
				})
				hasErrors = true
			} else {
				funcMap = runner.MergeFuncMaps(funcMap, userFuncs)
				var funcNames []string
				for name := range funcMap {
					funcNames = append(funcNames, name)
				}
				allResults = append(allResults, validate.ValidationResult{
					Name:   "functions",
					Status: validate.StatusPass,
					Details: map[string]string{
						"count": fmt.Sprintf("%d", len(funcNames)),
						"names": strings.Join(funcNames, ", "),
					},
				})
			}
		}

		// Phase 2: template parsing + ref/source resolution
		templateResults := validate.ValidateTemplates(cfg, g, projectRoot)
		allResults = append(allResults, templateResults...)

		// Phase 3: orphan files
		orphanResults := validate.DetectOrphanFiles(cfg, projectRoot)
		allResults = append(allResults, orphanResults...)

		// Phase 4: layer direction checks
		layerResults := validate.CheckLayerDirection(g)
		allResults = append(allResults, layerResults...)

		// Phase 5: hardcoded ref detection
		hardcodedResults := validate.DetectHardcodedRefs(cfg, projectRoot)
		allResults = append(allResults, hardcodedResults...)

		// Phase 6: default checks summary
		defaultCheckResults := validate.CheckDefaultChecks(cfg)
		allResults = append(allResults, defaultCheckResults...)

		// Phase 7: source contracts
		sourceContractResults := validate.CheckSourceContracts(cfg)
		allResults = append(allResults, sourceContractResults...)

		// Phase 8: orphaned checks (check files not wired to any asset)
		orphanedCheckResults := validate.CheckOrphanedChecks(cfg, projectRoot)
		allResults = append(allResults, orphanedCheckResults...)
	}

	// Check for errors/warnings
	for _, r := range allResults {
		switch r.Status {
		case validate.StatusError:
			hasErrors = true
		case validate.StatusWarn:
			hasWarnings = true
		}
	}

	if strict && hasWarnings {
		hasErrors = true
	}

	// Output
	if asJSON {
		return outputValidateJSON(cfg, allResults, hasErrors)
	}

	if !quiet {
		fmt.Printf("Pipeline: %s\n", cfg.Pipeline)
		fmt.Printf("Assets: %d\n", len(cfg.Assets))
		if len(cfg.Sources) > 0 {
			fmt.Printf("Sources: %d\n", len(cfg.Sources))
		}
		if g != nil {
			depCount := 0
			for _, a := range g.Assets {
				depCount += len(a.DependsOn)
			}
			fmt.Printf("Dependencies: %d\n", depCount)
			fmt.Printf("Root nodes: %d\n", len(g.RootNodes))
		}
		fmt.Println()
	}

	fmt.Println("Validation:")
	for _, r := range allResults {
		if quiet && r.Status == validate.StatusPass {
			continue
		}

		icon := statusIcon(string(r.Status))

		detail := ""
		if r.Details != nil {
			var parts []string
			for k, v := range r.Details {
				parts = append(parts, k+"="+v)
			}
			if len(parts) > 0 {
				detail = " (" + strings.Join(parts, ", ") + ")"
			}
		}

		fmt.Printf("  %s %s%s\n", icon, r.Name, detail)
		if r.Status != validate.StatusPass {
			for _, item := range r.Items {
				fmt.Printf("      %s\n", item)
			}
		}
	}

	if hasErrors {
		fmt.Println("\nValidation failed.")
		return fmt.Errorf("validation failed")
	}

	if hasWarnings && !quiet {
		fmt.Println("\nValid with warnings.")
	} else if !quiet {
		fmt.Println("\nValid.")
	}
	return nil
}

type jsonValidateOutput struct {
	Pipeline    string              `json:"pipeline"`
	AssetCount  int                 `json:"asset_count"`
	SourceCount int                 `json:"source_count,omitempty"`
	DepCount    int                 `json:"dependency_count"`
	Valid       bool                `json:"valid"`
	Checks      []jsonValidateCheck `json:"checks"`
}

type jsonValidateCheck struct {
	Name    string            `json:"name"`
	Status  string            `json:"status"`
	Details map[string]string `json:"details,omitempty"`
	Items   []string          `json:"items,omitempty"`
}

func outputValidateJSON(cfg *config.PipelineConfig, results []validate.ValidationResult, hasErrors bool) error {
	out := jsonValidateOutput{
		Valid: !hasErrors,
	}
	if cfg != nil {
		out.Pipeline = cfg.Pipeline
		out.AssetCount = len(cfg.Assets)
		out.SourceCount = len(cfg.Sources)
	}
	for _, r := range results {
		c := jsonValidateCheck{
			Name:    r.Name,
			Status:  string(r.Status),
			Details: r.Details,
		}
		if len(r.Items) > 0 {
			c.Items = r.Items
		}
		out.Checks = append(out.Checks, c)
	}
	data, _ := json.MarshalIndent(out, "", "  ")
	fmt.Println(string(data))
	if hasErrors {
		return fmt.Errorf("validation failed")
	}
	return nil
}

func runStatus(cmd *cobra.Command, args []string) error {
	projectRoot, _ := cmd.Flags().GetString("project-root")
	outputFormat, _ := cmd.Flags().GetString("output")
	outputJSON := outputFormat == "json"

	eventsDBPath := filepath.Join(projectRoot, ".granicus", "events.db")
	eventStore, err := events.New(eventsDBPath)
	if err != nil {
		if outputJSON {
			printJSONError("INTERNAL_ERROR", fmt.Sprintf("event store: %s", err), "", nil)
		}
		return fmt.Errorf("event store: %w", err)
	}
	defer eventStore.Close()

	var runID string
	if len(args) > 0 {
		runID = args[0]
	} else {
		runs, err := eventStore.ListRuns(1)
		if err != nil || len(runs) == 0 {
			if outputJSON {
				printJSONError("NO_RUNS", "no runs found", "Run a pipeline first with: granicus run <config>", nil)
			}
			return fmt.Errorf("no runs found")
		}
		runID = runs[0].RunID
	}

	summary, err := eventStore.GetRunSummary(runID)
	if err != nil {
		if outputJSON {
			printJSONError("RUN_NOT_FOUND", fmt.Sprintf("reading run %s: %s", runID, err), "Check the run ID with: granicus history", map[string]any{"run_id": runID})
		}
		return fmt.Errorf("reading run %s: %w", runID, err)
	}

	nodes, err := eventStore.GetNodeResults(runID)
	if err != nil {
		nodes = nil
	}

	if outputJSON {
		out := jsonStatusOutput{
			RunID:           summary.RunID,
			Pipeline:        summary.Pipeline,
			Status:          summary.Status,
			StartTime:       summary.StartTime,
			EndTime:         summary.EndTime,
			DurationSeconds: summary.DurationSeconds,
			Succeeded:       summary.Succeeded,
			Failed:          summary.Failed,
			Skipped:         summary.Skipped,
			TotalAssets:     summary.TotalNodes,
			Assets:          nodes,
		}
		data, _ := json.MarshalIndent(out, "", "  ")
		fmt.Println(string(data))
		return nil
	}

	fmt.Printf("Run: %s\n", summary.RunID)
	fmt.Printf("Pipeline: %s\n", summary.Pipeline)
	fmt.Printf("Status: %s\n", summary.Status)
	fmt.Printf("Duration: %.0fs\n", summary.DurationSeconds)
	fmt.Printf("Nodes: %d succeeded, %d failed, %d skipped\n", summary.Succeeded, summary.Failed, summary.Skipped)

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

type jsonHistoryOutput struct {
	Runs []events.RunSummary `json:"runs"`
}

type jsonHistoryCostEntry struct {
	events.RunSummary
	Cost *events.RunCostSummary `json:"cost,omitempty"`
}

type jsonHistoryCostOutput struct {
	Runs []jsonHistoryCostEntry `json:"runs"`
}

func runHistory(cmd *cobra.Command, args []string) error {
	projectRoot, _ := cmd.Flags().GetString("project-root")
	limit, _ := cmd.Flags().GetInt("limit")
	outputFormat, _ := cmd.Flags().GetString("output")
	showCosts, _ := cmd.Flags().GetBool("costs")
	outputJSON := outputFormat == "json"

	eventsDBPath := filepath.Join(projectRoot, ".granicus", "events.db")
	eventStore, err := events.New(eventsDBPath)
	if err != nil {
		if outputJSON {
			printJSONError("INTERNAL_ERROR", fmt.Sprintf("event store: %s", err), "", nil)
		}
		return fmt.Errorf("event store: %w", err)
	}
	defer eventStore.Close()

	runs, err := eventStore.ListRuns(limit)
	if err != nil {
		if outputJSON {
			printJSONError("INTERNAL_ERROR", err.Error(), "", nil)
		}
		return err
	}

	if outputJSON {
		if showCosts {
			entries := make([]jsonHistoryCostEntry, 0, len(runs))
			for _, r := range runs {
				entry := jsonHistoryCostEntry{RunSummary: r}
				if cost, err := eventStore.GetRunCostSummary(r.RunID); err == nil {
					entry.Cost = cost
				}
				entries = append(entries, entry)
			}
			out := jsonHistoryCostOutput{Runs: entries}
			data, _ := json.MarshalIndent(out, "", "  ")
			fmt.Println(string(data))
		} else {
			out := jsonHistoryOutput{Runs: runs}
			if out.Runs == nil {
				out.Runs = []events.RunSummary{}
			}
			data, _ := json.MarshalIndent(out, "", "  ")
			fmt.Println(string(data))
		}
		return nil
	}

	if len(runs) == 0 {
		fmt.Println("No runs found.")
		return nil
	}

	if showCosts {
		fmt.Printf("%-32s %-16s %-10s %-12s %-12s %s\n", "Run ID", "Pipeline", "Duration", "Bytes", "Est. Cost", "Cache Hit")
		for _, r := range runs {
			cost, err := eventStore.GetRunCostSummary(r.RunID)
			if err != nil || cost.TotalBQNodes == 0 {
				fmt.Printf("%-32s %-16s %-10s %-12s %-12s %s\n",
					r.RunID,
					r.Pipeline,
					fmt.Sprintf("%.0fs", r.DurationSeconds),
					"-", "-", "-",
				)
				continue
			}
			cacheStr := fmt.Sprintf("%.0f%% (%d/%d)", cost.CacheHitRate*100, cost.CachedNodes, cost.TotalBQNodes)
			fmt.Printf("%-32s %-16s %-10s %-12s %-12s %s\n",
				r.RunID,
				r.Pipeline,
				fmt.Sprintf("%.0fs", r.DurationSeconds),
				gc.FormatBytes(cost.TotalBytesProcessed),
				fmt.Sprintf("$%.6f", cost.TotalCostUSD),
				cacheStr,
			)
		}
	} else {
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
	outputFormat, _ := cmd.Flags().GetString("output")
	if outputFormat == "json" {
		asJSON = true
	}

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

type jsonModelsListOutput struct {
	Models []events.ModelVersion `json:"models"`
}

type jsonModelsHistoryOutput struct {
	Asset   string                `json:"asset"`
	History []events.ModelVersion `json:"history"`
}

func listModels(eventStore *events.Store, outputJSON bool) error {
	models, err := eventStore.ListModels()
	if err != nil {
		if outputJSON {
			printJSONError("INTERNAL_ERROR", err.Error(), "", nil)
		}
		return err
	}

	if outputJSON {
		out := jsonModelsListOutput{Models: models}
		if out.Models == nil {
			out.Models = []events.ModelVersion{}
		}
		data, _ := json.MarshalIndent(out, "", "  ")
		fmt.Println(string(data))
		return nil
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

func diffModelVersions(eventStore *events.Store, asset string, diffFlag string) error {
	var v1, v2 int
	if _, err := fmt.Sscanf(diffFlag, "%d,%d", &v1, &v2); err != nil {
		return fmt.Errorf("--diff expects N,M (e.g., --diff 1,2)")
	}
	history, err := eventStore.GetModelHistory(asset)
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
	fmt.Printf("--- %s v%d\n+++ %s v%d\n", asset, v1, asset, v2)
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

func showModelHistory(eventStore *events.Store, asset string, outputJSON bool) error {
	history, err := eventStore.GetModelHistory(asset)
	if err != nil {
		if outputJSON {
			printJSONError("INTERNAL_ERROR", err.Error(), "", nil)
		}
		return err
	}
	if len(history) == 0 {
		if outputJSON {
			printJSONError("NOT_FOUND", fmt.Sprintf("no history for %s", asset), "Check the asset name with: granicus models", map[string]any{"asset": asset})
			return nil
		}
		return fmt.Errorf("no history for %s", asset)
	}

	if outputJSON {
		out := jsonModelsHistoryOutput{Asset: asset, History: history}
		data, _ := json.MarshalIndent(out, "", "  ")
		fmt.Println(string(data))
		return nil
	}

	fmt.Printf("Model: %s\n\n", asset)
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

func runModels(cmd *cobra.Command, args []string) error {
	projectRoot, _ := cmd.Flags().GetString("project-root")
	diffFlag, _ := cmd.Flags().GetString("diff")
	outputFormat, _ := cmd.Flags().GetString("output")
	outputJSON := outputFormat == "json"

	eventsDBPath := filepath.Join(projectRoot, ".granicus", "events.db")
	if _, err := os.Stat(eventsDBPath); os.IsNotExist(err) {
		if outputJSON {
			printJSONError("NO_EVENTS_DB", "events.db does not exist", "Run a pipeline first with: granicus run <config>", nil)
			return nil
		}
		fmt.Println("No models found (events.db does not exist).")
		return nil
	}

	eventStore, err := events.New(eventsDBPath)
	if err != nil {
		if outputJSON {
			printJSONError("INTERNAL_ERROR", fmt.Sprintf("event store: %s", err), "", nil)
		}
		return fmt.Errorf("event store: %w", err)
	}
	defer eventStore.Close()

	if len(args) == 0 {
		return listModels(eventStore, outputJSON)
	}

	assetName := args[0]

	if diffFlag != "" {
		return diffModelVersions(eventStore, assetName, diffFlag)
	}

	return showModelHistory(eventStore, assetName, outputJSON)
}

func buildPoolManager(cfg *config.PipelineConfig) (*pool.PoolManager, map[string]string) {
	if len(cfg.Pools) == 0 {
		return nil, nil
	}

	poolConfigs := make(map[string]pool.PoolConfig, len(cfg.Pools))
	for name, pc := range cfg.Pools {
		var timeout time.Duration
		if pc.Timeout != "" {
			timeout, _ = time.ParseDuration(pc.Timeout) // dag:intentional -- timeout format already validated during config load
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

// dag:boundary
func monitorHook(bqClient *bigquery.Client) executor.PostRunHook {
	return func(g *graph.Graph, cfg *config.PipelineConfig, projectRoot string, rr *executor.RunResult) error {
		monitorCfgPath := filepath.Join(projectRoot, "monitoring.yaml")
		monCfg, err := monitor.LoadMonitorConfig(monitorCfgPath)
		if err != nil {
			return fmt.Errorf("loading monitoring.yaml: %w", err)
		}
		if monCfg == nil {
			return nil
		}

		dbPath := filepath.Join(projectRoot, ".granicus", "monitor.db")

		// Collect check errors
		if err := monitor.CollectCheckErrors(dbPath, cfg.Pipeline, rr); err != nil {
			slog.Warn("monitor check error collection failed", "error", err)
		}

		// Collect structural metrics
		now := time.Now().UTC()
		tables := make(map[string]string)
		_, defaultDS := primaryBQProjectDataset(cfg)
		for _, a := range cfg.Assets {
			ds := cfg.DatasetForAsset(a, defaultDS)
			if ds != "" {
				tables[a.Name] = ds
			}
		}
		structural := monitor.CollectStructuralMetrics(bqClient, monCfg.Monitoring.Structural, cfg.Pipeline, tables, now)

		// Collect business metrics
		project, _ := primaryBQProjectDataset(cfg)
		business := monitor.CollectBusinessMetrics(monitor.MonitorContext{
			Ctx:      context.Background(),
			BQ:       bqClient,
			Cfg:      monCfg,
			Pipeline: cfg.Pipeline,
			Project:  project,
			Tables:   tables,
		})

		// Append all snapshots
		allSnapshots := append(structural, business...)
		if len(allSnapshots) > 0 {
			if err := monitor.AppendSnapshots(dbPath, allSnapshots); err != nil {
				slog.Warn("monitor snapshot append failed", "error", err)
			}
		}

		// Compare and flag drift
		flags, err := monitor.CompareSnapshots(dbPath, monCfg, allSnapshots)
		if err != nil {
			slog.Warn("monitor comparison failed", "error", err)
		}
		if len(flags) > 0 {
			if err := monitor.AppendFlags(dbPath, flags); err != nil {
				slog.Warn("monitor flag append failed", "error", err)
			}
		}

		return nil
	}
}

func primaryBQProjectDataset(cfg *config.PipelineConfig) (string, string) {
	for _, conn := range cfg.Connections {
		if conn.Type == "bigquery" {
			return conn.Properties["project"], conn.Properties["dataset"]
		}
	}
	return "", ""
}

func newBQClientForContext(cfg *config.PipelineConfig) *bigquery.Client {
	for _, conn := range cfg.Connections {
		if conn.Type != "bigquery" {
			continue
		}
		var opts []option.ClientOption
		credMethod := "default"
		if creds := conn.Properties["credentials"]; creds != "" {
			gcreds, err := credentials.NewCredentialsFromFile(
				credentials.ServiceAccount,
				creds,
				&credentials.DetectOptions{
					Scopes: []string{bigquery.Scope},
				},
			)
			if err != nil {
				slog.Warn("could not load credentials file", "error", err)
				return nil
			}
			opts = append(opts, option.WithTokenSource(oauth2adapt.TokenSourceFromTokenProvider(gcreds)))
			credMethod = "file"
		}
		client, err := bigquery.NewClient(context.Background(), conn.Properties["project"], opts...)
		if err != nil {
			slog.Warn("could not create BQ client for context", "error", err)
			return nil
		}
		slog.Info("credential_access", "event", "bq_client_create", "connection", conn.Name, "credential_method", credMethod)
		return client
	}
	return nil
}

// dag:boundary
func ensureDatasets(cfg *config.PipelineConfig, eventStore *events.Store, runID string) error {
	ctx := context.Background()

	type dsKey struct{ project, dataset, creds string }
	seen := map[dsKey]bool{}

	for _, conn := range cfg.Connections {
		if conn.Type != "bigquery" {
			continue
		}
		key := dsKey{
			project: conn.Properties["project"],
			dataset: conn.Properties["dataset"],
			creds:   conn.Properties["credentials"],
		}
		if key.dataset == "" || seen[key] {
			continue
		}
		seen[key] = true

		var opts []option.ClientOption
		if key.creds != "" {
			gcreds, err := credentials.NewCredentialsFromFile(
				credentials.ServiceAccount,
				key.creds,
				&credentials.DetectOptions{
					Scopes: []string{bigquery.Scope},
				},
			)
			if err != nil {
				return fmt.Errorf("loading credentials file %s: %w", key.creds, err)
			}
			opts = append(opts, option.WithTokenSource(oauth2adapt.TokenSourceFromTokenProvider(gcreds)))
		}
		client, err := bigquery.NewClient(ctx, key.project, opts...)
		if err != nil {
			return fmt.Errorf("creating BQ client for %s: %w", key.dataset, err)
		}

		location := conn.Properties["location"]
		if location == "" {
			location = "us-central1"
		}
		meta := &bigquery.DatasetMetadata{
			Location: location,
		}
		if err := client.Dataset(key.dataset).Create(ctx, meta); err != nil {
			client.Close()
			if strings.Contains(err.Error(), "Already Exists") || strings.Contains(err.Error(), "already exists") {
				continue
			}
			return fmt.Errorf("creating dataset %s.%s: %w", key.project, key.dataset, err)
		}
		client.Close()

		fmt.Printf("Created dataset %s.%s (%s)\n", key.project, key.dataset, location)
		if eventStore != nil {
			logEmit(eventStore, events.Event{
				RunID: runID, EventType: "dataset_created", Severity: "info",
				Summary: fmt.Sprintf("Created dataset %s.%s (%s)", key.project, key.dataset, location),
			})
		}
	}
	return nil
}

func runMigrate(cmd *cobra.Command, args []string) error {
	configPath := args[0]
	dryRun, _ := cmd.Flags().GetBool("dry-run")
	fromVersionFlag, _ := cmd.Flags().GetString("from-version")

	content, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("reading config: %w", err)
	}

	fromVersion := fromVersionFlag
	if fromVersion == "" {
		fromVersion = migrate.DetectVersion(content)
	}

	result, err := migrate.Migrate(content, fromVersion)
	if err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}

	if result.AlreadyCurrent {
		fmt.Printf("Config is already at version %s, nothing to do.\n", migrate.LatestVersion)
		return nil
	}

	fmt.Printf("Migrating %s: %s -> %s\n", configPath, result.FromVersion, result.ToVersion)
	for _, c := range result.Changes {
		fmt.Printf("  - %s\n", c.Description)
	}

	if dryRun {
		fmt.Println("\n(dry-run: no changes written)")
		return nil
	}

	backupPath, err := migrate.WriteBackup(configPath)
	if err != nil {
		return fmt.Errorf("creating backup: %w", err)
	}
	fmt.Printf("Backup written: %s\n", backupPath)

	if err := os.WriteFile(configPath, result.Content, 0644); err != nil {
		return fmt.Errorf("writing migrated config: %w", err)
	}

	fmt.Printf("Migration complete: %s\n", configPath)
	return nil
}

func runCompletion(rootCmd *cobra.Command, shell string) error {
	switch shell {
	case "bash":
		return rootCmd.GenBashCompletion(os.Stdout)
	case "zsh":
		return rootCmd.GenZshCompletion(os.Stdout)
	case "fish":
		return rootCmd.GenFishCompletion(os.Stdout, true)
	case "powershell":
		return rootCmd.GenPowerShellCompletion(os.Stdout)
	default:
		return fmt.Errorf("unsupported shell: %s (supported: bash, zsh, fish, powershell)", shell)
	}
}

type jsonDoctorOutput struct {
	Healthy bool                 `json:"healthy"`
	Checks  []doctor.CheckResult `json:"checks"`
}

func runDoctor(cmd *cobra.Command, args []string) error {
	projectRoot, _ := cmd.Flags().GetString("project-root")
	outputFormat, _ := cmd.Flags().GetString("output")
	outputJSON := outputFormat == "json"

	var cfg *config.PipelineConfig
	if len(args) > 0 {
		var err error
		cfg, err = config.LoadConfig(args[0])
		if err != nil {
			if outputJSON {
				printJSONError("CONFIG_ERROR", err.Error(), "Check your pipeline configuration file", nil)
			}
			return fmt.Errorf("config: %w", err)
		}
	}

	results := doctor.RunChecks(cfg, projectRoot)

	hasFailures := false
	for _, r := range results {
		if r.Status == doctor.StatusFail {
			hasFailures = true
			break
		}
	}

	if outputJSON {
		out := jsonDoctorOutput{
			Healthy: !hasFailures,
			Checks:  results,
		}
		data, _ := json.MarshalIndent(out, "", "  ")
		fmt.Println(string(data))
		if hasFailures {
			return fmt.Errorf("health check failed")
		}
		return nil
	}

	const nameW = 28
	fmt.Printf("  %-*s  %s\n", nameW, "Check", "Details")
	fmt.Printf("  %-*s  %s\n", nameW, strings.Repeat("-", nameW), strings.Repeat("-", 40))

	for _, r := range results {
		icon := statusIcon(r.Status)
		fmt.Printf("  %s %-*s  %s\n", icon, nameW, r.Name, r.Message)
	}

	fmt.Println()
	if hasFailures {
		fmt.Println("Health check failed.")
		return fmt.Errorf("health check failed")
	}
	fmt.Println("All checks passed.")
	return nil
}
