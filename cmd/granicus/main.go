package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/spf13/cobra"

	"github.com/analytehealth/granicus/internal/checker"
	"github.com/analytehealth/granicus/internal/config"
	"github.com/analytehealth/granicus/internal/executor"
	"github.com/analytehealth/granicus/internal/graph"
	"github.com/analytehealth/granicus/internal/logging"
	"github.com/analytehealth/granicus/internal/rerun"
	"github.com/analytehealth/granicus/internal/runner"
	"github.com/analytehealth/granicus/internal/state"
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

	rootCmd.AddCommand(runCmd, validateCmd, statusCmd, historyCmd, versionCmd, newServeCmd())

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func loadAndBuild(configPath, projectRoot string) (*config.PipelineConfig, *graph.Graph, []string, error) {
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("config: %w", err)
	}

	deps, err := graph.ParseAllDependencies(cfg, projectRoot)
	if err != nil {
		return cfg, nil, nil, fmt.Errorf("dependencies: %w", err)
	}

	inputs := graph.ConfigToAssetInputs(cfg)

	// Generate check nodes and merge into graph
	checkNodes, checkDeps := checker.GenerateCheckNodes(cfg)
	inputs = append(inputs, checkNodes...)
	for k, v := range checkDeps {
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

func buildRegistry(cfg *config.PipelineConfig) *runner.RunnerRegistry {
	reg := runner.NewRunnerRegistry(cfg.Connections)

	// Register SQL runner per connection
	if cfg.Connections != nil {
		for _, conn := range cfg.Connections {
			if conn.Type == "bigquery" {
				reg.Register("sql", runner.NewSQLRunner(conn))
				reg.Register("sql_check", runner.NewSQLCheckRunner(conn))
				break
			}
		}
	}

	// Register python/dlt runners
	reg.Register("python", runner.NewPythonRunner(nil, nil))
	reg.Register("python_check", runner.NewPythonCheckRunner(nil, nil))
	reg.Register("dlt", runner.NewDLTRunner(nil, nil))

	return reg
}

func runRun(cmd *cobra.Command, args []string) error {
	projectRoot, _ := cmd.Flags().GetString("project-root")
	maxParallel, _ := cmd.Flags().GetInt("max-parallel")
	assetsFlag, _ := cmd.Flags().GetString("assets")
	fromFailure, _ := cmd.Flags().GetString("from-failure")
	fromDate, _ := cmd.Flags().GetString("from-date")
	toDate, _ := cmd.Flags().GetString("to-date")
	fullRefresh, _ := cmd.Flags().GetBool("full-refresh")

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

	if fromFailure != "" {
		store := logging.NewStore(projectRoot)
		rerunAssets, warnings, err := rerun.ComputeRerunSet(store, fromFailure, g)
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

	runID := logging.GenerateRunID()
	store := logging.NewStore(projectRoot)
	registry := buildRegistry(cfg)

	// Initialize state store
	stateDBPath := filepath.Join(projectRoot, ".granicus", "state.db")
	stateStore, err := state.New(stateDBPath)
	if err != nil {
		return fmt.Errorf("state store: %w", err)
	}
	defer stateStore.Close()

	runnerFunc := func(asset *graph.Asset, pr string, rid string) executor.NodeResult {
		ts := time.Now().Format("15:04:05")
		fmt.Printf("[%s] %s %-24s started\n", ts, whiteBullet, asset.Name)

		// Look up connection for this specific asset
		ra := &runner.Asset{
			Name:                  asset.Name,
			Type:                  asset.Type,
			Source:                asset.Source,
			DestinationConnection: asset.DestinationConnection,
			SourceConnection:      asset.SourceConnection,
			IntervalStart:         asset.IntervalStart,
			IntervalEnd:           asset.IntervalEnd,
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
		_ = store.WriteNodeResult(runID, entry)

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

	runCfg := executor.RunConfig{
		MaxParallel: cfg.MaxParallel,
		Assets:      assetFilter,
		ProjectRoot: projectRoot,
		RunID:       runID,
		FromDate:    fromDate,
		ToDate:      toDate,
		FullRefresh: fullRefresh,
		StateStore:  stateStore,
	}

	rr := executor.Execute(g, runCfg, runnerFunc)

	for _, r := range rr.Results {
		if r.Status == "skipped" {
			ts := time.Now().Format("15:04:05")
			fmt.Printf("[%s] %s %-24s skipped -- dependency failed\n", ts, yellowCirc, r.AssetName)

			entry := logging.NodeEntry{
				Asset:    r.AssetName,
				Status:   "skipped",
				ExitCode: -1,
				Error:    r.Error,
			}
			_ = store.WriteNodeResult(runID, entry)
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

	summary := logging.RunSummary{
		RunID:           runID,
		Pipeline:        cfg.Pipeline,
		StartTime:       rr.StartTime,
		EndTime:         rr.EndTime,
		DurationSeconds: totalDuration.Seconds(),
		TotalNodes:      len(rr.Results),
		Succeeded:       succeeded,
		Failed:          failed,
		Skipped:         skipped,
		Status:          status,
		Config:          logging.RunConfig{MaxParallel: cfg.MaxParallel, AssetsFilter: assetFilter},
	}
	_ = store.WriteRunSummary(runID, summary)

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
	store := logging.NewStore(projectRoot)

	var runID string
	if len(args) > 0 {
		runID = args[0]
	} else {
		runs, err := store.ListRuns()
		if err != nil || len(runs) == 0 {
			return fmt.Errorf("no runs found")
		}
		runID = runs[0].RunID
	}

	summary, err := store.ReadRunSummary(runID)
	if err != nil {
		return fmt.Errorf("reading run %s: %w", runID, err)
	}

	fmt.Printf("Run: %s\n", summary.RunID)
	fmt.Printf("Pipeline: %s\n", summary.Pipeline)
	fmt.Printf("Status: %s\n", summary.Status)
	fmt.Printf("Duration: %.0fs\n", summary.DurationSeconds)
	fmt.Printf("Nodes: %d succeeded, %d failed, %d skipped\n", summary.Succeeded, summary.Failed, summary.Skipped)

	nodes, err := store.ReadNodeResults(runID)
	if err != nil {
		return nil
	}

	var failedNodes, skippedNodes []logging.NodeEntry
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
	store := logging.NewStore(projectRoot)

	runs, err := store.ListRuns()
	if err != nil {
		return err
	}

	if len(runs) == 0 {
		fmt.Println("No runs found.")
		return nil
	}

	if limit > 0 && len(runs) > limit {
		runs = runs[:limit]
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
