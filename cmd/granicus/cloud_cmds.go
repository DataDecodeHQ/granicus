package main

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/spf13/cobra"
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

	tmpFile, err := os.CreateTemp("", "granicus-push-*.tar.gz")
	if err != nil {
		return fmt.Errorf("creating temp file: %w", err)
	}
	defer os.Remove(tmpFile.Name())

	if err := createTarGz(tmpFile, sourceDir); err != nil {
		tmpFile.Close()
		return fmt.Errorf("creating archive: %w", err)
	}
	tmpFile.Close()

	fields := map[string]string{"pipeline": pipeline}
	if activate {
		fields["activate"] = "true"
	}

	data, err := cloudPostMultipart("/api/v1/registry/push", "archive", tmpFile.Name(), fields)
	if err != nil {
		return fmt.Errorf("push failed: %w", err)
	}

	if jsonOutput {
		fmt.Println(string(data))
		return nil
	}

	var ver struct {
		Pipeline    string    `json:"pipeline"`
		Number      int       `json:"number"`
		ContentHash string    `json:"content_hash"`
		FileCount   int       `json:"file_count"`
		SizeBytes   int64     `json:"size_bytes"`
		Active      bool      `json:"active"`
	}
	if err := json.Unmarshal(data, &ver); err != nil {
		return fmt.Errorf("parsing response: %w", err)
	}

	fmt.Printf("Pushed %s v%d (%s)\n", pipeline, ver.Number, ver.ContentHash)
	fmt.Printf("  Files: %d, Size: %d bytes\n", ver.FileCount, ver.SizeBytes)
	if ver.Active {
		color.Green("  Activated: yes")
	} else {
		fmt.Printf("  Activate with: granicus activate %s %d\n", pipeline, ver.Number)
	}

	return nil
}

// createTarGz creates a tar.gz archive of the given directory.
func createTarGz(w io.Writer, sourceDir string) error {
	gw := gzip.NewWriter(w)
	defer gw.Close()
	tw := tar.NewWriter(gw)
	defer tw.Close()

	return filepath.Walk(sourceDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		rel, err := filepath.Rel(sourceDir, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}

		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		header.Name = rel

		if err := tw.WriteHeader(header); err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = io.Copy(tw, f)
		return err
	})
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

	body := map[string]int{"version": version}
	_, err = cloudPost("/api/v1/registry/"+pipeline+"/activate", body)
	if err != nil {
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

	data, err := cloudGet("/api/v1/registry/" + pipeline + "/versions")
	if err != nil {
		return err
	}

	if jsonOutput {
		fmt.Println(string(data))
		return nil
	}

	var versions []struct {
		Number      int       `json:"number"`
		ContentHash string    `json:"content_hash"`
		PushedBy    string    `json:"pushed_by"`
		PushedAt    time.Time `json:"pushed_at"`
		FileCount   int       `json:"file_count"`
		Active      bool      `json:"active"`
	}
	if err := json.Unmarshal(data, &versions); err != nil {
		return fmt.Errorf("parsing response: %w", err)
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
	vA := args[1]
	vB := args[2]
	jsonOutput, _ := cmd.Flags().GetBool("json")

	path := fmt.Sprintf("/api/v1/registry/%s/diff?v1=%s&v2=%s", pipeline, url.QueryEscape(vA), url.QueryEscape(vB))
	data, err := cloudGet(path)
	if err != nil {
		return err
	}

	if jsonOutput {
		fmt.Println(string(data))
		return nil
	}

	var result struct {
		Added    []string `json:"added"`
		Removed  []string `json:"removed"`
		Modified []string `json:"modified"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return fmt.Errorf("parsing response: %w", err)
	}

	for _, f := range result.Added {
		color.Green("+ %s", f)
	}
	for _, f := range result.Removed {
		color.Red("- %s", f)
	}
	for _, f := range result.Modified {
		color.Yellow("~ %s", f)
	}
	if len(result.Added) == 0 && len(result.Removed) == 0 && len(result.Modified) == 0 {
		fmt.Println("No changes")
	}
	return nil
}

// newHistoryCmd creates the granicus history command.
func newHistoryCmd2() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cloud-history",
		Short: "Show pipeline run history from cloud",
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

	path := fmt.Sprintf("/api/v1/state/%s/history?since=%s&limit=%d", pipeline, url.QueryEscape(since), limit)
	data, err := cloudGet(path)
	if err != nil {
		return err
	}

	if jsonOutput {
		fmt.Println(string(data))
		return nil
	}

	var runs []runRecord
	if err := json.Unmarshal(data, &runs); err != nil {
		return fmt.Errorf("parsing response: %w", err)
	}

	for _, r := range runs {
		statusColor := color.GreenString(r.Status)
		if r.Status == "failed" || r.Status == "crashed" {
			statusColor = color.RedString(r.Status)
		}
		runIDDisplay := r.RunID
		if len(runIDDisplay) > 20 {
			runIDDisplay = runIDDisplay[:20]
		}
		fmt.Printf("%s  %s  %s  %d/%d/%d (ok/fail/skip)  %s\n",
			runIDDisplay, statusColor, r.StartedAt.Format("2006-01-02 15:04"),
			r.Succeeded, r.Failed, r.Skipped, r.TriggerContext)
	}
	return nil
}

// newEventsCmd2 creates the granicus events command.
func newEventsCmd2() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cloud-events",
		Short: "Show run events from cloud",
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

	// Events endpoint needs a pipeline in the path; use "_all" as convention
	path := fmt.Sprintf("/api/v1/state/_all/events?run_id=%s", url.QueryEscape(runID))
	if eventType != "" {
		path += "&type=" + url.QueryEscape(eventType)
	}

	data, err := cloudGet(path)
	if err != nil {
		return err
	}

	if jsonOutput {
		fmt.Println(string(data))
		return nil
	}

	var events []eventRecord
	if err := json.Unmarshal(data, &events); err != nil {
		return fmt.Errorf("parsing response: %w", err)
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

	target := pipeline
	if target == "" {
		target = "_all"
	}

	path := fmt.Sprintf("/api/v1/state/%s/failures?since=%s", target, url.QueryEscape(since))
	data, err := cloudGet(path)
	if err != nil {
		return err
	}

	if jsonOutput {
		fmt.Println(string(data))
		return nil
	}

	var records []failureRecord
	if err := json.Unmarshal(data, &records); err != nil {
		return fmt.Errorf("parsing response: %w", err)
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

	path := fmt.Sprintf("/api/v1/state/%s/stats?node=%s&since=%s",
		pipeline, url.QueryEscape(node), url.QueryEscape(since))
	data, err := cloudGet(path)
	if err != nil {
		return err
	}

	if jsonOutput {
		fmt.Println(string(data))
		return nil
	}

	var stats struct {
		Node          string         `json:"node"`
		Pipeline      string         `json:"pipeline"`
		TotalRuns     int            `json:"total_runs"`
		Successes     int            `json:"successes"`
		Failures      int            `json:"failures"`
		SuccessRate   float64        `json:"success_rate"`
		AvgDurationMs int64          `json:"avg_duration_ms"`
		TopErrors     map[string]int `json:"top_errors"`
	}
	if err := json.Unmarshal(data, &stats); err != nil {
		return fmt.Errorf("parsing response: %w", err)
	}

	fmt.Printf("Node: %s (pipeline: %s)\n", stats.Node, stats.Pipeline)
	fmt.Printf("  Runs: %d  Success: %d  Failed: %d  Rate: %.1f%%\n",
		stats.TotalRuns, stats.Successes, stats.Failures, stats.SuccessRate*100)
	if stats.TotalRuns > 0 {
		fmt.Printf("  Avg duration: %dms\n", stats.AvgDurationMs)
	}
	for errMsg, count := range stats.TopErrors {
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

	data, err := cloudGet("/api/v1/state/_all/status")
	if err != nil {
		return err
	}

	if jsonOutput {
		fmt.Println(string(data))
		return nil
	}

	var runs []runRecord
	if err := json.Unmarshal(data, &runs); err != nil {
		return fmt.Errorf("parsing response: %w", err)
	}

	if len(runs) == 0 {
		fmt.Println("No pipelines currently running")
		return nil
	}

	for _, r := range runs {
		dur := time.Since(r.StartedAt).Round(time.Second)
		runIDDisplay := r.RunID
		if len(runIDDisplay) > 20 {
			runIDDisplay = runIDDisplay[:20]
		}
		fmt.Printf("%s  %s  running for %s  (%d nodes)\n",
			r.Pipeline, runIDDisplay, dur, r.NodeCount)
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

	path := fmt.Sprintf("/api/v1/state/%s/intervals?asset=%s", pipeline, url.QueryEscape(asset))
	data, err := cloudGet(path)
	if err != nil {
		return err
	}

	if jsonOutput {
		fmt.Println(string(data))
		return nil
	}

	var intervals []intervalRecord
	if err := json.Unmarshal(data, &intervals); err != nil {
		return fmt.Errorf("parsing response: %w", err)
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

// --- Response types for JSON decoding ---

type runRecord struct {
	RunID          string    `json:"run_id"`
	Pipeline       string    `json:"pipeline"`
	Status         string    `json:"status"`
	StartedAt      time.Time `json:"started_at"`
	Succeeded      int       `json:"succeeded"`
	Failed         int       `json:"failed"`
	Skipped        int       `json:"skipped"`
	NodeCount      int       `json:"node_count"`
	TriggerContext string    `json:"trigger_context"`
	ErrorSummary   string    `json:"error_summary"`
}

type eventRecord struct {
	RunID      string    `json:"run_id"`
	Node       string    `json:"node"`
	EventType  string    `json:"event_type"`
	Error      string    `json:"error"`
	DurationMs int64     `json:"duration_ms"`
	Timestamp  time.Time `json:"timestamp"`
}

type failureRecord struct {
	RunID          string        `json:"run_id"`
	Pipeline       string        `json:"pipeline"`
	Status         string        `json:"status"`
	StartedAt      time.Time     `json:"started_at"`
	ErrorSummary   string        `json:"error_summary"`
	TriggerContext string        `json:"trigger_context"`
	FailedEvents   []eventRecord `json:"failed_events,omitempty"`
}

type intervalRecord struct {
	AssetName     string `json:"AssetName"`
	IntervalStart string `json:"IntervalStart"`
	IntervalEnd   string `json:"IntervalEnd"`
	Status        string `json:"Status"`
	RunID         string `json:"RunID"`
	StartedAt     string `json:"StartedAt"`
	CompletedAt   string `json:"CompletedAt"`
}

// --- Helpers ---

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
	cfg, err := loadPipelineYAML(dir)
	if err == nil && cfg != "" {
		return cfg
	}
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
		for _, line := range strings.Split(string(data), "\n") {
			if strings.HasPrefix(line, "pipeline:") {
				return strings.TrimSpace(strings.TrimPrefix(line, "pipeline:")), nil
			}
		}
	}
	return "", fmt.Errorf("no pipeline.yaml found")
}
