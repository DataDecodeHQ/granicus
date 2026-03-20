package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/spf13/cobra"

	"github.com/DataDecodeHQ/granicus/internal/source"
	"github.com/DataDecodeHQ/granicus/internal/state"
)

// newPushCmd creates the granicus push command.
func newPushCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "push <directory>",
		Short: "Package and upload a pipeline version",
		Args:  cobra.ExactArgs(1),
		RunE:  cloudGate(runPush),
	}
	cmd.Flags().String("pipeline", "", "Pipeline name (default: inferred from pipeline.yaml)")
	cmd.Flags().Bool("activate", false, "Activate this version after push")
	cmd.Flags().Bool("json", false, "JSON output")
	return cmd
}

func runPush(cmd *cobra.Command, args []string) error {
	sourceDir := args[0]
	pipeline, _ := cmd.Flags().GetString("pipeline")
	activate, _ := cmd.Flags().GetBool("activate")
	jsonOutput, _ := cmd.Flags().GetBool("json")

	if pipeline == "" {
		pipeline = inferPipelineName(sourceDir)
	}

	ctx := context.Background()
	src, err := source.NewGCSVersionedSource(ctx, "", "")
	if err != nil {
		return fmt.Errorf("connecting to version store: %w", err)
	}

	ver, err := src.Register(ctx, pipeline, sourceDir)
	if err != nil {
		return fmt.Errorf("push failed: %w", err)
	}

	if activate {
		if err := src.Activate(ctx, pipeline, ver.Number); err != nil {
			return fmt.Errorf("activate failed: %w", err)
		}
		ver.Active = true
	}

	if jsonOutput {
		json.NewEncoder(os.Stdout).Encode(ver)
	} else {
		fmt.Printf("Pushed %s v%d (%s)\n", pipeline, ver.Number, ver.ContentHash)
		fmt.Printf("  Files: %d, Size: %d bytes\n", ver.FileCount, ver.SizeBytes)
		if activate {
			color.Green("  Activated: yes")
		} else {
			fmt.Printf("  Activate with: granicus activate %s %d\n", pipeline, ver.Number)
		}
	}

	return nil
}

// newActivateCmd creates the granicus activate command.
func newActivateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "activate <pipeline> <version>",
		Short: "Set the active version for a pipeline",
		Args:  cobra.ExactArgs(2),
		RunE:  cloudGate(runActivate),
	}
	cmd.Flags().Bool("json", false, "JSON output")
	return cmd
}

func runActivate(cmd *cobra.Command, args []string) error {
	pipeline := args[0]
	version, err := strconv.Atoi(args[1])
	if err != nil {
		return fmt.Errorf("invalid version number: %s", args[1])
	}

	ctx := context.Background()
	src, err := source.NewGCSVersionedSource(ctx, "", "")
	if err != nil {
		return fmt.Errorf("connecting to version store: %w", err)
	}

	if err := src.Activate(ctx, pipeline, version); err != nil {
		return fmt.Errorf("activate failed: %w", err)
	}

	fmt.Printf("Activated %s v%d\n", pipeline, version)
	return nil
}

// newVersionsCmd creates the granicus versions command.
func newVersionsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "versions <pipeline>",
		Short: "List all versions of a pipeline",
		Args:  cobra.ExactArgs(1),
		RunE:  cloudGate(runVersions),
	}
	cmd.Flags().Bool("json", false, "JSON output")
	return cmd
}

func runVersions(cmd *cobra.Command, args []string) error {
	pipeline := args[0]
	jsonOutput, _ := cmd.Flags().GetBool("json")

	ctx := context.Background()
	src, err := source.NewGCSVersionedSource(ctx, "", "")
	if err != nil {
		return fmt.Errorf("connecting to version store: %w", err)
	}

	versions, err := src.List(ctx, pipeline)
	if err != nil {
		return err
	}

	if jsonOutput {
		json.NewEncoder(os.Stdout).Encode(versions)
		return nil
	}

	for _, v := range versions {
		marker := "  "
		if v.Active {
			marker = color.GreenString("* ")
		}
		fmt.Printf("%sv%d  %s  %s  %s  %d files\n",
			marker, v.Number, v.ContentHash, v.PushedBy,
			v.PushedAt.Format("2006-01-02 15:04"), v.FileCount)
	}
	return nil
}

// newDiffCmd creates the granicus diff command.
func newDiffCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "diff <pipeline> <versionA> <versionB>",
		Short: "Show file-level diff between two versions",
		Args:  cobra.ExactArgs(3),
		RunE:  cloudGate(runDiff),
	}
	cmd.Flags().Bool("json", false, "JSON output")
	return cmd
}

func runDiff(cmd *cobra.Command, args []string) error {
	pipeline := args[0]
	vA, _ := strconv.Atoi(args[1])
	vB, _ := strconv.Atoi(args[2])
	jsonOutput, _ := cmd.Flags().GetBool("json")

	ctx := context.Background()
	src, err := source.NewGCSVersionedSource(ctx, "", "")
	if err != nil {
		return fmt.Errorf("connecting to version store: %w", err)
	}

	added, removed, modified, err := src.Diff(ctx, pipeline, vA, vB)
	if err != nil {
		return err
	}

	if jsonOutput {
		json.NewEncoder(os.Stdout).Encode(map[string]any{
			"added": added, "removed": removed, "modified": modified,
		})
		return nil
	}

	for _, f := range added {
		color.Green("+ %s", f)
	}
	for _, f := range removed {
		color.Red("- %s", f)
	}
	for _, f := range modified {
		color.Yellow("~ %s", f)
	}
	if len(added) == 0 && len(removed) == 0 && len(modified) == 0 {
		fmt.Println("No changes")
	}
	return nil
}

// newHistoryCmd creates the granicus history command.
func newHistoryCmd2() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cloud-history",
		Short: "Show pipeline run history from Firestore",
		RunE:  cloudGate(runHistory2),
	}
	cmd.Flags().String("pipeline", "", "Filter by pipeline name (required)")
	cmd.Flags().String("since", "7d", "Show runs since (e.g. 7d, 24h)")
	cmd.Flags().Int("limit", 20, "Max runs to show")
	cmd.Flags().Bool("json", false, "JSON output")
	cmd.MarkFlagRequired("pipeline")
	return cmd
}

func runHistory2(cmd *cobra.Command, args []string) error {
	pipeline, _ := cmd.Flags().GetString("pipeline")
	since, _ := cmd.Flags().GetString("since")
	limit, _ := cmd.Flags().GetInt("limit")
	jsonOutput, _ := cmd.Flags().GetBool("json")

	sinceTime, err := parseSince(since)
	if err != nil {
		return err
	}

	ctx := context.Background()
	backend, err := newFirestoreBackend(ctx, pipeline)
	if err != nil {
		return err
	}
	defer backend.Close()

	runs, err := backend.ListRuns(ctx, pipeline, nil, sinceTime, limit)
	if err != nil {
		return err
	}

	if jsonOutput {
		json.NewEncoder(os.Stdout).Encode(runs)
		return nil
	}

	for _, r := range runs {
		statusColor := color.GreenString(r.Status)
		if r.Status == "failed" || r.Status == "crashed" {
			statusColor = color.RedString(r.Status)
		}
		fmt.Printf("%s  %s  %s  %d/%d/%d (ok/fail/skip)  %s\n",
			r.RunID[:20], statusColor, r.StartedAt.Format("2006-01-02 15:04"),
			r.Succeeded, r.Failed, r.Skipped, r.TriggerContext)
	}
	return nil
}

// newEventsCmd2 creates the granicus events command for Firestore.
func newEventsCmd2() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cloud-events",
		Short: "Show run events from Firestore",
		RunE:  cloudGate(runEvents2),
	}
	cmd.Flags().String("run-id", "", "Run ID (required)")
	cmd.Flags().String("type", "", "Filter by event type (comma-separated)")
	cmd.Flags().Bool("json", false, "JSON output")
	cmd.MarkFlagRequired("run-id")
	return cmd
}

func runEvents2(cmd *cobra.Command, args []string) error {
	runID, _ := cmd.Flags().GetString("run-id")
	eventType, _ := cmd.Flags().GetString("type")
	jsonOutput, _ := cmd.Flags().GetBool("json")

	ctx := context.Background()
	backend, err := newFirestoreBackend(ctx, "")
	if err != nil {
		return err
	}
	defer backend.Close()

	var types []string
	if eventType != "" {
		types = strings.Split(eventType, ",")
	}

	events, err := backend.ListEvents(ctx, runID, types)
	if err != nil {
		return err
	}

	if jsonOutput {
		json.NewEncoder(os.Stdout).Encode(events)
		return nil
	}

	for _, e := range events {
		ts := e.Timestamp.Format("15:04:05")
		line := fmt.Sprintf("%s  %-16s  %-30s", ts, e.EventType, e.Node)
		if e.DurationMs > 0 {
			line += fmt.Sprintf("  %dms", e.DurationMs)
		}
		if e.Error != "" {
			line += fmt.Sprintf("  %s", color.RedString(e.Error))
		}
		fmt.Println(line)
	}
	return nil
}

// newFailuresCmd creates the granicus failures command.
func newFailuresCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "failures",
		Short: "Show recent failures across pipelines",
		RunE:  cloudGate(runFailures),
	}
	cmd.Flags().String("pipeline", "", "Filter by pipeline name")
	cmd.Flags().String("since", "7d", "Show failures since")
	cmd.Flags().Bool("json", false, "JSON output")
	return cmd
}

func runFailures(cmd *cobra.Command, args []string) error {
	pipeline, _ := cmd.Flags().GetString("pipeline")
	since, _ := cmd.Flags().GetString("since")
	jsonOutput, _ := cmd.Flags().GetBool("json")

	sinceTime, err := parseSince(since)
	if err != nil {
		return err
	}

	ctx := context.Background()
	backend, err := newFirestoreBackend(ctx, pipeline)
	if err != nil {
		return err
	}
	defer backend.Close()

	runs, err := backend.ListRuns(ctx, pipeline, []string{"failed", "crashed"}, sinceTime, 50)
	if err != nil {
		return err
	}

	type failureRecord struct {
		RunID          string                `json:"run_id"`
		Pipeline       string                `json:"pipeline"`
		Status         string                `json:"status"`
		StartedAt      time.Time             `json:"started_at"`
		ErrorSummary   string                `json:"error_summary"`
		TriggerContext string                `json:"trigger_context"`
		FailedEvents   []state.EventDoc      `json:"failed_events,omitempty"`
	}

	var records []failureRecord
	for _, r := range runs {
		events, _ := backend.ListEvents(ctx, r.RunID, []string{"asset_failed", "node_failed"})
		records = append(records, failureRecord{
			RunID:          r.RunID,
			Pipeline:       r.Pipeline,
			Status:         r.Status,
			StartedAt:      r.StartedAt,
			ErrorSummary:   r.ErrorSummary,
			TriggerContext: r.TriggerContext,
			FailedEvents:   events,
		})
	}

	if jsonOutput {
		json.NewEncoder(os.Stdout).Encode(records)
		return nil
	}

	for _, r := range records {
		fmt.Printf("%s  %s  %s  %s\n",
			color.RedString(r.Status), r.Pipeline,
			r.StartedAt.Format("2006-01-02 15:04"), r.ErrorSummary)
		for _, e := range r.FailedEvents {
			fmt.Printf("  %s: %s\n", e.Node, e.Error)
		}
	}
	return nil
}

// newStatsCmd creates the granicus stats command.
func newStatsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "stats",
		Short: "Show node reliability statistics",
		RunE:  cloudGate(runStats),
	}
	cmd.Flags().String("node", "", "Node name (required)")
	cmd.Flags().String("pipeline", "", "Pipeline name (required)")
	cmd.Flags().String("since", "30d", "Calculate stats since")
	cmd.Flags().Bool("json", false, "JSON output")
	cmd.MarkFlagRequired("node")
	cmd.MarkFlagRequired("pipeline")
	return cmd
}

func runStats(cmd *cobra.Command, args []string) error {
	node, _ := cmd.Flags().GetString("node")
	pipeline, _ := cmd.Flags().GetString("pipeline")
	since, _ := cmd.Flags().GetString("since")
	jsonOutput, _ := cmd.Flags().GetBool("json")

	sinceTime, err := parseSince(since)
	if err != nil {
		return err
	}

	ctx := context.Background()
	backend, err := newFirestoreBackend(ctx, pipeline)
	if err != nil {
		return err
	}
	defer backend.Close()

	runs, err := backend.ListRuns(ctx, pipeline, nil, sinceTime, 0)
	if err != nil {
		return err
	}

	var totalRuns, successes, failures int
	var totalDuration int64
	errorCounts := make(map[string]int)

	for _, r := range runs {
		events, _ := backend.ListEvents(ctx, r.RunID, nil)
		for _, e := range events {
			if e.Node != node {
				continue
			}
			switch e.EventType {
			case "asset_succeeded", "node_succeeded":
				totalRuns++
				successes++
				totalDuration += e.DurationMs
			case "asset_failed", "node_failed":
				totalRuns++
				failures++
				totalDuration += e.DurationMs
				errorCounts[e.Error]++
			}
		}
	}

	stats := map[string]any{
		"node":         node,
		"pipeline":     pipeline,
		"total_runs":   totalRuns,
		"successes":    successes,
		"failures":     failures,
		"success_rate": 0.0,
		"avg_duration_ms": 0,
		"top_errors":   errorCounts,
	}
	if totalRuns > 0 {
		stats["success_rate"] = float64(successes) / float64(totalRuns)
		stats["avg_duration_ms"] = totalDuration / int64(totalRuns)
	}

	if jsonOutput {
		json.NewEncoder(os.Stdout).Encode(stats)
		return nil
	}

	fmt.Printf("Node: %s (pipeline: %s)\n", node, pipeline)
	fmt.Printf("  Runs: %d  Success: %d  Failed: %d  Rate: %.1f%%\n",
		totalRuns, successes, failures, stats["success_rate"].(float64)*100)
	if totalRuns > 0 {
		fmt.Printf("  Avg duration: %dms\n", stats["avg_duration_ms"])
	}
	for errMsg, count := range errorCounts {
		fmt.Printf("  Error (%dx): %s\n", count, errMsg)
	}
	return nil
}

// newCloudStatusCmd creates the granicus cloud-status command.
func newCloudStatusCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cloud-status",
		Short: "Show current running pipelines",
		RunE:  cloudGate(runCloudStatus),
	}
	cmd.Flags().Bool("json", false, "JSON output")
	return cmd
}

func runCloudStatus(cmd *cobra.Command, args []string) error {
	jsonOutput, _ := cmd.Flags().GetBool("json")

	ctx := context.Background()
	backend, err := newFirestoreBackend(ctx, "")
	if err != nil {
		return err
	}
	defer backend.Close()

	runs, err := backend.ListRuns(ctx, "", []string{"running"}, time.Time{}, 50)
	if err != nil {
		return err
	}

	if jsonOutput {
		json.NewEncoder(os.Stdout).Encode(runs)
		return nil
	}

	if len(runs) == 0 {
		fmt.Println("No pipelines currently running")
		return nil
	}

	for _, r := range runs {
		dur := time.Since(r.StartedAt).Round(time.Second)
		fmt.Printf("%s  %s  running for %s  (%d nodes)\n",
			r.Pipeline, r.RunID[:20], dur, r.NodeCount)
	}
	return nil
}

// newIntervalsCmd creates the granicus intervals command.
func newIntervalsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "intervals",
		Short: "Show interval state for a pipeline asset",
		RunE:  cloudGate(runIntervals),
	}
	cmd.Flags().String("pipeline", "", "Pipeline name (required)")
	cmd.Flags().String("asset", "", "Asset name (required)")
	cmd.Flags().Bool("json", false, "JSON output")
	cmd.MarkFlagRequired("pipeline")
	cmd.MarkFlagRequired("asset")
	return cmd
}

func runIntervals(cmd *cobra.Command, args []string) error {
	pipeline, _ := cmd.Flags().GetString("pipeline")
	asset, _ := cmd.Flags().GetString("asset")
	jsonOutput, _ := cmd.Flags().GetBool("json")

	ctx := context.Background()
	backend, err := newFirestoreBackend(ctx, pipeline)
	if err != nil {
		return err
	}
	defer backend.Close()

	intervals, err := backend.GetIntervals(asset)
	if err != nil {
		return err
	}

	if jsonOutput {
		json.NewEncoder(os.Stdout).Encode(intervals)
		return nil
	}

	for _, iv := range intervals {
		statusStr := iv.Status
		switch iv.Status {
		case "complete":
			statusStr = color.GreenString(iv.Status)
		case "failed":
			statusStr = color.RedString(iv.Status)
		case "in_progress":
			statusStr = color.YellowString(iv.Status)
		}
		fmt.Printf("%s - %s  %s  run=%s\n",
			iv.IntervalStart, iv.IntervalEnd, statusStr, iv.RunID)
	}
	return nil
}

// --- Helpers ---

func newFirestoreBackend(ctx context.Context, pipeline string) (*state.FirestoreStateBackend, error) {
	return state.NewFirestoreStateBackend(ctx, "", pipeline)
}

func parseSince(s string) (time.Time, error) {
	if len(s) < 2 {
		return time.Time{}, fmt.Errorf("invalid since format: %s", s)
	}
	numStr := s[:len(s)-1]
	unit := s[len(s)-1]
	num := 0
	for _, c := range numStr {
		if c < '0' || c > '9' {
			return time.Time{}, fmt.Errorf("invalid since format: %s", s)
		}
		num = num*10 + int(c-'0')
	}
	switch unit {
	case 'h':
		return time.Now().Add(-time.Duration(num) * time.Hour), nil
	case 'd':
		return time.Now().AddDate(0, 0, -num), nil
	default:
		return time.Time{}, fmt.Errorf("invalid since unit: %c (use h or d)", unit)
	}
}

func inferPipelineName(dir string) string {
	// Try reading pipeline.yaml from the directory
	cfg, err := loadPipelineYAML(dir)
	if err == nil && cfg != "" {
		return cfg
	}
	// Fall back to directory name
	parts := strings.Split(strings.TrimRight(dir, "/"), "/")
	return parts[len(parts)-1]
}

func loadPipelineYAML(dir string) (string, error) {
	for _, name := range []string{"pipeline.yaml", "pipeline.yml"} {
		path := dir + "/" + name
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		// Quick extract pipeline name
		for _, line := range strings.Split(string(data), "\n") {
			if strings.HasPrefix(line, "pipeline:") {
				return strings.TrimSpace(strings.TrimPrefix(line, "pipeline:")), nil
			}
		}
	}
	return "", fmt.Errorf("no pipeline.yaml found")
}
