package executor

import (
	"testing"
	"time"

	"os"
	"path/filepath"

	"github.com/analytehealth/granicus/internal/checker"
	"github.com/analytehealth/granicus/internal/config"
	"github.com/analytehealth/granicus/internal/graph"
	"github.com/analytehealth/granicus/internal/logging"
	"github.com/analytehealth/granicus/internal/rerun"
	"github.com/analytehealth/granicus/internal/runner"
)

func TestIntegration_Phase1_MixedRunners(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	dir := t.TempDir()

	// Create directories
	os.MkdirAll(filepath.Join(dir, "scripts"), 0755)
	os.MkdirAll(filepath.Join(dir, "python"), 0755)
	os.MkdirAll(filepath.Join(dir, "checks"), 0755)

	// Shell script: root node
	os.WriteFile(filepath.Join(dir, "scripts/extract.sh"), []byte("#!/bin/bash\necho extracted 100 rows"), 0755)

	// Python script: depends on extract, writes metadata
	os.WriteFile(filepath.Join(dir, "python/transform.py"), []byte(`import os, json
path = os.environ.get("GRANICUS_METADATA_PATH", "")
if path:
    with open(path, "w") as f:
        json.dump({"rows_processed": "100", "output_table": "transformed"}, f)
print("transform complete")
`), 0644)

	// dlt script: depends on extract, writes metadata
	os.WriteFile(filepath.Join(dir, "python/dlt_load.py"), []byte(`import os, json
path = os.environ.get("GRANICUS_METADATA_PATH", "")
if path:
    with open(path, "w") as f:
        json.dump({"rows_loaded": "50", "tables_created": "1"}, f)
print("dlt load complete")
`), 0644)

	// Python check: pass (exit 0)
	os.WriteFile(filepath.Join(dir, "checks/check_transform_rows.py"), []byte(`print("check passed: rows > 0")`), 0644)

	// Python check: fail (exit 1)
	os.WriteFile(filepath.Join(dir, "checks/check_dlt_freshness.py"), []byte(`import sys
print("stale data detected")
sys.exit(1)
`), 0644)

	// Config YAML with mixed types and checks
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
	// Add dependency comments
	os.WriteFile(filepath.Join(dir, "python/transform.py"),
		append([]byte("# depends_on: extract\n"), mustRead(t, filepath.Join(dir, "python/transform.py"))...), 0644)
	os.WriteFile(filepath.Join(dir, "python/dlt_load.py"),
		append([]byte("# depends_on: extract\n"), mustRead(t, filepath.Join(dir, "python/dlt_load.py"))...), 0644)

	configPath := filepath.Join(dir, "config.yaml")
	os.WriteFile(configPath, []byte(configContent), 0644)

	// Load config and build graph (mirrors CLI loadAndBuild)
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		t.Fatalf("config: %v", err)
	}

	deps, err := graph.ParseAllDependencies(cfg, dir)
	if err != nil {
		t.Fatalf("deps: %v", err)
	}

	inputs := graph.ConfigToAssetInputs(cfg)

	// Generate check nodes (Phase 1 feature)
	checkNodes, checkDeps := checker.GenerateCheckNodes(cfg)
	inputs = append(inputs, checkNodes...)
	for k, v := range checkDeps {
		deps[k] = v
	}

	g, err := graph.BuildGraph(inputs, deps)
	if err != nil {
		t.Fatalf("graph: %v", err)
	}

	// Verify check nodes were created
	if _, ok := g.Assets["check:transform:check_transform_rows"]; !ok {
		t.Fatal("missing check node for transform")
	}
	if _, ok := g.Assets["check:dlt_load:check_dlt_freshness"]; !ok {
		t.Fatal("missing check node for dlt_load")
	}

	// Set up registry with all runner types
	reg := runner.NewRunnerRegistry(nil)
	reg.Register("python", runner.NewPythonRunner(nil, nil))
	reg.Register("python_check", runner.NewPythonCheckRunner(nil, nil))
	reg.Register("dlt", runner.NewDLTRunner(nil, nil))

	runID := logging.GenerateRunID()
	store := logging.NewStore(dir)

	runnerFunc := func(asset *graph.Asset, pr string, rid string) NodeResult {
		ra := &runner.Asset{
			Name:   asset.Name,
			Type:   asset.Type,
			Source: asset.Source,
		}

		r := reg.Run(ra, pr, rid)

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
			Metadata:   r.Metadata,
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
			Metadata:  r.Metadata,
		}
	}

	rr := Execute(g, RunConfig{
		MaxParallel: 4,
		ProjectRoot: dir,
		RunID:       runID,
	}, runnerFunc)

	// Write skipped nodes
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

	rm := make(map[string]string)
	for _, r := range rr.Results {
		rm[r.AssetName] = r.Status
	}

	// Verify statuses
	expected := map[string]string{
		"extract":                             "success",
		"transform":                           "success",
		"dlt_load":                            "success",
		"check:transform:check_transform_rows": "success",
		"check:dlt_load:check_dlt_freshness":   "failed",
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

	// Verify metadata was captured for python and dlt runners
	nodes, err := store.ReadNodeResults(runID)
	if err != nil {
		t.Fatalf("reading nodes: %v", err)
	}

	for _, n := range nodes {
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

	// Write run summary for rerun test
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
		DurationSeconds: rr.EndTime.Sub(rr.StartTime).Seconds(),
		TotalNodes:      len(rr.Results),
		Succeeded:       succeeded,
		Failed:          failed,
		Skipped:         skipped,
		Status:          "completed_with_failures",
	}
	store.WriteRunSummary(runID, summary)

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

	// Create scripts: root -> mid -> leaf, mid will fail on first run
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

	store := logging.NewStore(dir)
	runID1 := "run-fail-001"

	shellRunner := runner.NewShellRunner()
	makeRunnerFunc := func(rid string) func(*graph.Asset, string, string) NodeResult {
		return func(asset *graph.Asset, pr string, _ string) NodeResult {
			r := shellRunner.Run(&runner.Asset{
				Name: asset.Name, Type: asset.Type, Source: asset.Source,
			}, pr, rid)

			entry := logging.NodeEntry{
				Asset:      r.AssetName,
				Status:     r.Status,
				StartTime:  r.StartTime.Format(time.RFC3339),
				EndTime:    r.EndTime.Format(time.RFC3339),
				DurationMs: r.Duration.Milliseconds(),
				ExitCode:   r.ExitCode,
				Error:      r.Error,
			}
			_ = store.WriteNodeResult(rid, entry)

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
			entry := logging.NodeEntry{
				Asset: r.AssetName, Status: "skipped", ExitCode: -1, Error: r.Error,
			}
			_ = store.WriteNodeResult(runID1, entry)
		}
	}

	summary1 := logging.RunSummary{
		RunID: runID1, Pipeline: "rerun_test",
		StartTime: rr1.StartTime, EndTime: rr1.EndTime,
		DurationSeconds: rr1.EndTime.Sub(rr1.StartTime).Seconds(),
		TotalNodes: len(rr1.Results), Status: "completed_with_failures",
	}
	store.WriteRunSummary(runID1, summary1)

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
	rerunAssets, warnings, err := rerun.ComputeRerunSet(store, runID1, g)
	if err != nil {
		t.Fatalf("ComputeRerunSet: %v", err)
	}
	if len(warnings) != 0 {
		t.Errorf("unexpected warnings: %v", warnings)
	}

	// Should rerun mid + leaf (failed + descendants), NOT root or independent
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

	// Re-run with asset filter (simulates --from-failure)
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

	// mid and leaf should have succeeded (mid was fixed)
	if rm2["mid"] != "success" {
		t.Errorf("rerun mid: %s", rm2["mid"])
	}
	if rm2["leaf"] != "success" {
		t.Errorf("rerun leaf: %s", rm2["leaf"])
	}

	// root gets pulled in by Subgraph as a dependency of mid — this is correct
	if rm2["root"] != "success" {
		t.Errorf("rerun root (dependency): %s", rm2["root"])
	}

	// independent should NOT appear — it has no relationship to failed nodes
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

	// Phase 0 style config: no connections, no checks
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

	// Phase 1 code path: generate check nodes (should produce none)
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
