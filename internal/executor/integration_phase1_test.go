package executor

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/DataDecodeHQ/granicus/internal/checker"
	"github.com/DataDecodeHQ/granicus/internal/config"
	"github.com/DataDecodeHQ/granicus/internal/events"
	"github.com/DataDecodeHQ/granicus/internal/graph"
	"github.com/DataDecodeHQ/granicus/internal/rerun"
	"github.com/DataDecodeHQ/granicus/internal/runner"
)

func TestIntegration_Phase1_MixedRunners(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	dir := t.TempDir()

	os.MkdirAll(filepath.Join(dir, "scripts"), 0755)
	os.MkdirAll(filepath.Join(dir, "python"), 0755)
	os.MkdirAll(filepath.Join(dir, "checks"), 0755)

	os.WriteFile(filepath.Join(dir, "scripts/extract.sh"), []byte("#!/bin/bash\necho extracted 100 rows"), 0755)

	os.WriteFile(filepath.Join(dir, "python/transform.py"), []byte(`import os, json
path = os.environ.get("GRANICUS_METADATA_PATH", "")
if path:
    with open(path, "w") as f:
        json.dump({"rows_processed": "100", "output_table": "transformed"}, f)
print("transform complete")
`), 0644)

	os.WriteFile(filepath.Join(dir, "python/dlt_load.py"), []byte(`import os, json
path = os.environ.get("GRANICUS_METADATA_PATH", "")
if path:
    with open(path, "w") as f:
        json.dump({"rows_loaded": "50", "tables_created": "1"}, f)
print("dlt load complete")
`), 0644)

	os.WriteFile(filepath.Join(dir, "checks/check_transform_rows.py"), []byte(`print("check passed: rows > 0")`), 0644)

	os.WriteFile(filepath.Join(dir, "checks/check_dlt_freshness.py"), []byte(`import sys
print("stale data detected")
sys.exit(1)
`), 0644)

	configContent := `pipeline: phase1_integration
max_parallel: 4
assets:
  - name: extract
    type: shell
    source: scripts/extract.sh
  - name: transform
    type: python
    source: python/transform.py
    checks:
      - source: checks/check_transform_rows.py
  - name: dlt_load
    type: dlt
    source: python/dlt_load.py
    checks:
      - source: checks/check_dlt_freshness.py
`
	os.WriteFile(filepath.Join(dir, "python/transform.py"),
		append([]byte("# depends_on: extract\n"), mustRead(t, filepath.Join(dir, "python/transform.py"))...), 0644)
	os.WriteFile(filepath.Join(dir, "python/dlt_load.py"),
		append([]byte("# depends_on: extract\n"), mustRead(t, filepath.Join(dir, "python/dlt_load.py"))...), 0644)

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

	checkNodes, checkDeps := checker.GenerateCheckNodes(cfg)
	inputs = append(inputs, checkNodes...)
	for k, v := range checkDeps {
		deps[k] = v
	}

	g, err := graph.BuildGraph(inputs, deps)
	if err != nil {
		t.Fatalf("graph: %v", err)
	}

	if _, ok := g.Assets["check:transform:check_transform_rows"]; !ok {
		t.Fatal("missing check node for transform")
	}
	if _, ok := g.Assets["check:dlt_load:check_dlt_freshness"]; !ok {
		t.Fatal("missing check node for dlt_load")
	}

	reg := runner.NewRunnerRegistry(nil)
	reg.Register("python", runner.NewPythonRunner(nil, nil, nil, ""))
	reg.Register("python_check", runner.NewPythonCheckRunner(nil, nil, nil, ""))
	reg.Register("dlt", runner.NewDLTRunner(nil, nil, nil, ""))

	runID := events.GenerateRunID()
	eventStore := newTestEventStore(t)

	runnerFunc := func(asset *graph.Asset, pr string, rid string) NodeResult {
		ra := &runner.Asset{
			Name:   asset.Name,
			Type:   asset.Type,
			Source: asset.Source,
		}

		r := reg.Run(ra, pr, rid)

		eventType := "asset_succeeded"
		severity := "info"
		if r.Status == "failed" {
			eventType = "asset_failed"
			severity = "error"
		}
		_ = eventStore.Emit(events.Event{
			RunID: runID, Pipeline: cfg.Pipeline, Asset: r.AssetName,
			EventType:  eventType,
			Severity:   severity,
			DurationMs: r.Duration.Milliseconds(),
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
			Metadata:  r.Metadata,
		}
	}

	rr := Execute(g, RunConfig{
		MaxParallel: 4,
		ProjectRoot: dir,
		RunID:       runID,
	}, runnerFunc)

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
		"extract":                               "success",
		"transform":                             "success",
		"dlt_load":                              "success",
		"check:transform:check_transform_rows":  "success",
		"check:dlt_load:check_dlt_freshness":    "failed",
	}

	for name, want := range expected {
		got, ok := rm[name]
		if !ok {
			t.Errorf("%s: not in results", name)
			continue
		}
		if got != want {
			t.Errorf("%s: expected %s, got %s", name, want, got)
		}
	}

	// Verify metadata was captured via event details
	nodeResults, err := eventStore.GetNodeResults(runID)
	if err != nil {
		t.Fatalf("reading nodes: %v", err)
	}

	for _, n := range nodeResults {
		switch n.Asset {
		case "transform":
			if n.Metadata["rows_processed"] != "100" {
				t.Errorf("transform metadata rows_processed: %q", n.Metadata["rows_processed"])
			}
		case "dlt_load":
			if n.Metadata["rows_loaded"] != "50" {
				t.Errorf("dlt_load metadata rows_loaded: %q", n.Metadata["rows_loaded"])
			}
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

	if failed != 1 {
		t.Errorf("expected 1 failed, got %d", failed)
	}
	if succeeded != 4 {
		t.Errorf("expected 4 succeeded, got %d", succeeded)
	}

	t.Logf("Phase 1 integration: %d succeeded, %d failed, %d skipped", succeeded, failed, skipped)
}

func TestIntegration_Phase1_FromFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	dir := t.TempDir()

	os.MkdirAll(filepath.Join(dir, "scripts"), 0755)

	os.WriteFile(filepath.Join(dir, "scripts/root.sh"), []byte("#!/bin/bash\necho root ok"), 0755)
	os.WriteFile(filepath.Join(dir, "scripts/mid.sh"), []byte("#!/bin/bash\n# depends_on: root\necho mid fail >&2\nexit 1"), 0755)
	os.WriteFile(filepath.Join(dir, "scripts/leaf.sh"), []byte("#!/bin/bash\n# depends_on: mid\necho leaf ok"), 0755)
	os.WriteFile(filepath.Join(dir, "scripts/independent.sh"), []byte("#!/bin/bash\necho independent ok"), 0755)

	configContent := `pipeline: rerun_test
max_parallel: 2
assets:
  - name: root
    type: shell
    source: scripts/root.sh
  - name: mid
    type: shell
    source: scripts/mid.sh
  - name: leaf
    type: shell
    source: scripts/leaf.sh
  - name: independent
    type: shell
    source: scripts/independent.sh
`
	configPath := filepath.Join(dir, "config.yaml")
	os.WriteFile(configPath, []byte(configContent), 0644)

	cfg, _ := config.LoadConfig(configPath)
	deps, _ := graph.ParseAllDependencies(cfg, dir)
	inputs := graph.ConfigToAssetInputs(cfg)
	g, _ := graph.BuildGraph(inputs, deps)

	eventStore := newTestEventStore(t)
	runID1 := "run-fail-001"

	makeRunnerFunc := func(rid string) func(*graph.Asset, string, string) NodeResult {
		return func(asset *graph.Asset, pr string, _ string) NodeResult {
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
				RunID: rid, Pipeline: cfg.Pipeline, Asset: r.AssetName,
				EventType:  eventType,
				Severity:   "info",
				DurationMs: r.Duration.Milliseconds(),
				Details:    map[string]any{"error_message": r.Error},
			})

			return NodeResult{
				AssetName: r.AssetName,
				Status:    r.Status,
				StartTime: r.StartTime,
				EndTime:   r.EndTime,
				Duration:  r.Duration,
				Error:     r.Error,
				ExitCode:  r.ExitCode,
			}
		}
	}

	// First run: mid fails, leaf gets skipped
	rr1 := Execute(g, RunConfig{
		MaxParallel: 2,
		ProjectRoot: dir,
		RunID:       runID1,
	}, makeRunnerFunc(runID1))

	for _, r := range rr1.Results {
		if r.Status == "skipped" {
			_ = eventStore.Emit(events.Event{
				RunID: runID1, Pipeline: cfg.Pipeline, Asset: r.AssetName,
				EventType: "asset_skipped", Severity: "warning",
			})
		}
	}

	rm1 := make(map[string]string)
	for _, r := range rr1.Results {
		rm1[r.AssetName] = r.Status
	}

	if rm1["root"] != "success" {
		t.Errorf("run1 root: %s", rm1["root"])
	}
	if rm1["mid"] != "failed" {
		t.Errorf("run1 mid: %s", rm1["mid"])
	}
	if rm1["leaf"] != "skipped" {
		t.Errorf("run1 leaf: %s", rm1["leaf"])
	}
	if rm1["independent"] != "success" {
		t.Errorf("run1 independent: %s", rm1["independent"])
	}

	// Compute rerun set from failed run
	rerunAssets, warnings, err := rerun.ComputeRerunSet(eventStore, runID1, g)
	if err != nil {
		t.Fatalf("ComputeRerunSet: %v", err)
	}
	if len(warnings) != 0 {
		t.Errorf("unexpected warnings: %v", warnings)
	}

	rerunMap := make(map[string]bool)
	for _, name := range rerunAssets {
		rerunMap[name] = true
	}

	if !rerunMap["mid"] {
		t.Error("mid should be in rerun set")
	}
	if !rerunMap["leaf"] {
		t.Error("leaf should be in rerun set")
	}
	if rerunMap["root"] {
		t.Error("root should NOT be in rerun set")
	}
	if rerunMap["independent"] {
		t.Error("independent should NOT be in rerun set")
	}

	// Fix the script so mid succeeds on re-run
	os.WriteFile(filepath.Join(dir, "scripts/mid.sh"), []byte("#!/bin/bash\n# depends_on: root\necho mid fixed"), 0755)

	runID2 := "run-rerun-002"
	rr2 := Execute(g, RunConfig{
		MaxParallel: 2,
		ProjectRoot: dir,
		RunID:       runID2,
		Assets:      rerunAssets,
	}, makeRunnerFunc(runID2))

	rm2 := make(map[string]string)
	for _, r := range rr2.Results {
		rm2[r.AssetName] = r.Status
	}

	if rm2["mid"] != "success" {
		t.Errorf("rerun mid: %s", rm2["mid"])
	}
	if rm2["leaf"] != "success" {
		t.Errorf("rerun leaf: %s", rm2["leaf"])
	}
	if rm2["root"] != "success" {
		t.Errorf("rerun root (dependency): %s", rm2["root"])
	}
	if _, ok := rm2["independent"]; ok {
		t.Error("independent should not be in rerun results")
	}

	t.Logf("--from-failure rerun: %d assets re-executed", len(rr2.Results))
}

func TestIntegration_Phase0_BackwardsCompat(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "scripts"), 0755)

	os.WriteFile(filepath.Join(dir, "scripts/a.sh"), []byte("#!/bin/bash\necho a"), 0755)
	os.WriteFile(filepath.Join(dir, "scripts/b.sh"), []byte("#!/bin/bash\n# depends_on: a\necho b"), 0755)

	configContent := `pipeline: phase0_compat
max_parallel: 2
assets:
  - name: a
    type: shell
    source: scripts/a.sh
  - name: b
    type: shell
    source: scripts/b.sh
`
	configPath := filepath.Join(dir, "config.yaml")
	os.WriteFile(configPath, []byte(configContent), 0644)

	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		t.Fatalf("config: %v", err)
	}

	deps, _ := graph.ParseAllDependencies(cfg, dir)
	inputs := graph.ConfigToAssetInputs(cfg)

	checkNodes, checkDeps := checker.GenerateCheckNodes(cfg)
	inputs = append(inputs, checkNodes...)
	for k, v := range checkDeps {
		deps[k] = v
	}

	g, err := graph.BuildGraph(inputs, deps)
	if err != nil {
		t.Fatalf("graph: %v", err)
	}

	if len(g.Assets) != 2 {
		t.Errorf("expected 2 assets, got %d", len(g.Assets))
	}

	reg := runner.NewRunnerRegistry(nil)

	runnerFunc := func(asset *graph.Asset, pr string, rid string) NodeResult {
		r := reg.Run(&runner.Asset{
			Name: asset.Name, Type: asset.Type, Source: asset.Source,
		}, pr, rid)
		return NodeResult{
			AssetName: r.AssetName, Status: r.Status,
			StartTime: r.StartTime, EndTime: r.EndTime,
			Duration: r.Duration, ExitCode: r.ExitCode,
		}
	}

	rr := Execute(g, RunConfig{
		MaxParallel: 2,
		ProjectRoot: dir,
		RunID:       "compat-run",
	}, runnerFunc)

	for _, r := range rr.Results {
		if r.Status != "success" {
			t.Errorf("%s: %s (%s)", r.AssetName, r.Status, r.Error)
		}
	}
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
