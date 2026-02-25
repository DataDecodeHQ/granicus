package executor

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/analytehealth/granicus/internal/config"
	"github.com/analytehealth/granicus/internal/graph"
	"github.com/analytehealth/granicus/internal/logging"
	"github.com/analytehealth/granicus/internal/runner"
)

func TestIntegration_10AssetPipeline(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	dir := t.TempDir()
	scriptsDir := filepath.Join(dir, "scripts")
	os.MkdirAll(scriptsDir, 0755)

	// Create 10 assets with various dependency patterns:
	// root1, root2, root3 (no deps, parallel)
	// mid1 depends on root1 (succeeds)
	// mid2 depends on root1 (succeeds)
	// fail1 depends on root2 (fails)
	// skip1 depends on fail1 (skipped)
	// skip2 depends on skip1 (skipped)
	// diamond depends on mid1, mid2 (succeeds - diamond dependency)
	// final depends on diamond, root3 (succeeds)

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

	// Write config
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

	// Load config and build graph
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

	// Set up runner and log store
	runID := logging.GenerateRunID()
	store := logging.NewStore(dir)
	shellRunner := runner.NewShellRunner()

	runnerFunc := func(asset *graph.Asset, pr string, rid string) NodeResult {
		r := shellRunner.Run(&runner.Asset{
			Name:   asset.Name,
			Type:   asset.Type,
			Source: asset.Source,
		}, pr, rid)

		entry := logging.NodeEntry{
			Asset:      r.AssetName,
			Status:     r.Status,
			StartTime:  r.StartTime.Format(time.RFC3339),
			EndTime:    r.EndTime.Format(time.RFC3339),
			DurationMs: r.Duration.Milliseconds(),
			ExitCode:   r.ExitCode,
			Error:      r.Error,
			Stdout:     r.Stdout,
			Stderr:     r.Stderr,
		}
		_ = store.WriteNodeResult(runID, entry)

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

	// Write skipped nodes to log
	for _, r := range rr.Results {
		if r.Status == "skipped" {
			entry := logging.NodeEntry{
				Asset:    r.AssetName,
				Status:   "skipped",
				ExitCode: -1,
				Error:    r.Error,
			}
			_ = store.WriteNodeResult(runID, entry)
		}
	}

	// Build result map
	rm := make(map[string]string)
	for _, r := range rr.Results {
		rm[r.AssetName] = r.Status
	}

	// Verify correct statuses
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

	// Verify parallel execution: 3 roots run concurrently at 100ms each,
	// so total should be much less than serial sum (~1 second)
	if elapsed > 3*time.Second {
		t.Errorf("too slow (%v), parallelism may be broken", elapsed)
	}

	// Verify logs
	nodes, err := store.ReadNodeResults(runID)
	if err != nil {
		t.Fatalf("reading nodes: %v", err)
	}
	if len(nodes) != 10 {
		t.Errorf("expected 10 node entries in log, got %d", len(nodes))
	}

	// Write run summary
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

	summary := logging.RunSummary{
		RunID:           runID,
		Pipeline:        cfg.Pipeline,
		StartTime:       rr.StartTime,
		EndTime:         rr.EndTime,
		DurationSeconds: elapsed.Seconds(),
		TotalNodes:      10,
		Succeeded:       succeeded,
		Failed:          failed,
		Skipped:         skipped,
		Status:          "completed_with_failures",
	}
	store.WriteRunSummary(runID, summary)

	// Verify run summary
	readSummary, err := store.ReadRunSummary(runID)
	if err != nil {
		t.Fatalf("reading summary: %v", err)
	}
	if readSummary.Succeeded != 7 {
		t.Errorf("expected 7 succeeded, got %d", readSummary.Succeeded)
	}
	if readSummary.Failed != 1 {
		t.Errorf("expected 1 failed, got %d", readSummary.Failed)
	}
	if readSummary.Skipped != 2 {
		t.Errorf("expected 2 skipped, got %d", readSummary.Skipped)
	}
}
