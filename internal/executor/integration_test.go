package executor

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/DataDecodeHQ/granicus/internal/config"
	"github.com/DataDecodeHQ/granicus/internal/events"
	"github.com/DataDecodeHQ/granicus/internal/graph"
	"github.com/DataDecodeHQ/granicus/internal/runner"
)

func newTestEventStore(t *testing.T) *events.Store {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "events.db")
	s, err := events.New(dbPath)
	if err != nil {
		t.Fatalf("creating store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestIntegration_10AssetPipeline(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	dir := t.TempDir()
	scriptsDir := filepath.Join(dir, "scripts")
	os.MkdirAll(scriptsDir, 0755)

	scripts := map[string]string{
		"root1.sh":   "#!/bin/bash\necho root1 done\nsleep 0.1",
		"root2.sh":   "#!/bin/bash\necho root2 done\nsleep 0.1",
		"root3.sh":   "#!/bin/bash\necho root3 done\nsleep 0.1",
		"mid1.sh":    "#!/bin/bash\n# depends_on: root1\necho mid1 done\nsleep 0.1",
		"mid2.sh":    "#!/bin/bash\n# depends_on: root1\necho mid2 done\nsleep 0.1",
		"fail1.sh":   "#!/bin/bash\n# depends_on: root2\necho failing >&2\nexit 1",
		"skip1.sh":   "#!/bin/bash\n# depends_on: fail1\necho skip1 done",
		"skip2.sh":   "#!/bin/bash\n# depends_on: skip1\necho skip2 done",
		"diamond.sh": "#!/bin/bash\n# depends_on: mid1\n# depends_on: mid2\necho diamond done",
		"final.sh":   "#!/bin/bash\n# depends_on: diamond\n# depends_on: root3\necho final done",
	}

	for name, content := range scripts {
		path := filepath.Join(scriptsDir, name)
		if err := os.WriteFile(path, []byte(content), 0755); err != nil {
			t.Fatal(err)
		}
	}

	configContent := `pipeline: integration_test
max_parallel: 5
assets:
  - name: root1
    type: shell
    source: scripts/root1.sh
  - name: root2
    type: shell
    source: scripts/root2.sh
  - name: root3
    type: shell
    source: scripts/root3.sh
  - name: mid1
    type: shell
    source: scripts/mid1.sh
  - name: mid2
    type: shell
    source: scripts/mid2.sh
  - name: fail1
    type: shell
    source: scripts/fail1.sh
  - name: skip1
    type: shell
    source: scripts/skip1.sh
  - name: skip2
    type: shell
    source: scripts/skip2.sh
  - name: diamond
    type: shell
    source: scripts/diamond.sh
  - name: final
    type: shell
    source: scripts/final.sh
`
	configPath := filepath.Join(dir, "config.yaml")
	os.WriteFile(configPath, []byte(configContent), 0644)

	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		t.Fatalf("config: %v", err)
	}

	deps, err := graph.ParseAllDependencies(cfg, dir)
	if err != nil {
		t.Fatalf("deps: %v", err)
	}

	inputs := graph.ConfigToAssetInputs(cfg)
	g, err := graph.BuildGraph(inputs, deps)
	if err != nil {
		t.Fatalf("graph: %v", err)
	}

	runID := events.GenerateRunID()
	eventStore := newTestEventStore(t)
	runnerFunc := func(asset *graph.Asset, pr string, rid string) NodeResult {
		start := time.Now()
		sub := runner.RunSubprocess(runner.SubprocessConfig{
			Command: []string{"bash", asset.Source},
			WorkDir: pr,
			Timeout: 5 * time.Minute,
		})
		r := runner.NodeResultFromSubprocess(asset.Name, start, sub)

		eventType := "asset_succeeded"
		if r.Status == "failed" {
			eventType = "asset_failed"
		}
		_ = eventStore.Emit(events.Event{
			RunID: runID, Pipeline: cfg.Pipeline, Asset: r.AssetName,
			EventType:  eventType,
			DurationMs: r.Duration.Milliseconds(),
			Severity:   "info",
			Details:    map[string]any{"error_message": r.Error, "metadata": r.Metadata},
		})

		return NodeResult{
			AssetName: r.AssetName,
			Status:    r.Status,
			StartTime: r.StartTime,
			EndTime:   r.EndTime,
			Duration:  r.Duration,
			Error:     r.Error,
			Stdout:    r.Stdout,
			Stderr:    r.Stderr,
			ExitCode:  r.ExitCode,
		}
	}

	start := time.Now()
	rr := Execute(g, RunConfig{
		MaxParallel: 5,
		ProjectRoot: dir,
		RunID:       runID,
	}, runnerFunc)
	elapsed := time.Since(start)

	// Emit skipped nodes
	for _, r := range rr.Results {
		if r.Status == "skipped" {
			_ = eventStore.Emit(events.Event{
				RunID: runID, Pipeline: cfg.Pipeline, Asset: r.AssetName,
				EventType: "asset_skipped", Severity: "warning",
			})
		}
	}

	rm := make(map[string]string)
	for _, r := range rr.Results {
		rm[r.AssetName] = r.Status
	}

	expected := map[string]string{
		"root1":   "success",
		"root2":   "success",
		"root3":   "success",
		"mid1":    "success",
		"mid2":    "success",
		"fail1":   "failed",
		"skip1":   "skipped",
		"skip2":   "skipped",
		"diamond": "success",
		"final":   "success",
	}

	for name, want := range expected {
		if got := rm[name]; got != want {
			t.Errorf("%s: expected %s, got %s", name, want, got)
		}
	}

	if elapsed > 3*time.Second {
		t.Errorf("too slow (%v), parallelism may be broken", elapsed)
	}

	// Verify event logs
	nodeResults, err := eventStore.GetNodeResults(runID)
	if err != nil {
		t.Fatalf("reading nodes: %v", err)
	}
	if len(nodeResults) != 10 {
		t.Errorf("expected 10 node entries in log, got %d", len(nodeResults))
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

	// Emit run events
	eventStore.Emit(events.Event{
		RunID: runID, Pipeline: cfg.Pipeline, EventType: "run_started",
		Severity: "info", Timestamp: rr.StartTime,
	})
	eventStore.Emit(events.Event{
		RunID: runID, Pipeline: cfg.Pipeline, EventType: "run_completed",
		Severity: "info", Timestamp: rr.EndTime,
		DurationMs: elapsed.Milliseconds(),
		Details: map[string]any{
			"status": "completed_with_failures", "succeeded": succeeded,
			"failed": failed, "skipped": skipped,
			"total_nodes": 10, "duration_seconds": elapsed.Seconds(),
		},
	})

	// Verify run summary
	summary, err := eventStore.GetRunSummary(runID)
	if err != nil {
		t.Fatalf("reading summary: %v", err)
	}
	if summary.Succeeded != 7 {
		t.Errorf("expected 7 succeeded, got %d", summary.Succeeded)
	}
	if summary.Failed != 1 {
		t.Errorf("expected 1 failed, got %d", summary.Failed)
	}
	if summary.Skipped != 2 {
		t.Errorf("expected 2 skipped, got %d", summary.Skipped)
	}
}
