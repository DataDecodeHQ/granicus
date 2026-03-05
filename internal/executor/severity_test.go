package executor

import (
	"testing"

	"github.com/Andrew-DataDecode/Granicus/internal/graph"
)

// TestExecute_SeverityInfo verifies that an info-severity check failure is
// treated as a pass: downstream runs, no skip propagation.
func TestExecute_SeverityInfo(t *testing.T) {
	g := buildTestGraph(t,
		[]graph.AssetInput{
			{Name: "A", Type: "shell", Source: "a.sh"},
			{Name: "check:A:info_check", Type: "sql_check", Blocking: false, Severity: "info"},
			{Name: "B", Type: "shell", Source: "b.sh"},
		},
		map[string][]string{
			"check:A:info_check": {"A"},
			"B":                  {"A"},
		},
	)

	runner := func(asset *graph.Asset, pr string, rid string) NodeResult {
		if asset.Name == "check:A:info_check" {
			return NodeResult{AssetName: asset.Name, Status: "failed", Error: "check failed", ExitCode: 1}
		}
		return NodeResult{AssetName: asset.Name, Status: "success", ExitCode: 0}
	}

	rr := Execute(g, RunConfig{MaxParallel: 1}, runner)
	rm := resultMap(rr)

	if rm["A"] != "success" {
		t.Errorf("A should succeed, got %s", rm["A"])
	}
	if rm["check:A:info_check"] != "success" {
		t.Errorf("info check failure should be treated as success, got %s", rm["check:A:info_check"])
	}
	if rm["B"] != "success" {
		t.Errorf("B should succeed (info check does not block), got %s", rm["B"])
	}
}

// TestExecute_SeverityWarning verifies that a warning-severity check failure is
// treated as a pass: downstream runs, no skip propagation.
func TestExecute_SeverityWarning(t *testing.T) {
	g := buildTestGraph(t,
		[]graph.AssetInput{
			{Name: "A", Type: "shell", Source: "a.sh"},
			{Name: "check:A:warn_check", Type: "sql_check", Blocking: false, Severity: "warning"},
			{Name: "B", Type: "shell", Source: "b.sh"},
		},
		map[string][]string{
			"check:A:warn_check": {"A"},
			"B":                  {"A"},
		},
	)

	runner := func(asset *graph.Asset, pr string, rid string) NodeResult {
		if asset.Name == "check:A:warn_check" {
			return NodeResult{AssetName: asset.Name, Status: "failed", Error: "check failed", ExitCode: 1}
		}
		return NodeResult{AssetName: asset.Name, Status: "success", ExitCode: 0}
	}

	rr := Execute(g, RunConfig{MaxParallel: 1}, runner)
	rm := resultMap(rr)

	if rm["A"] != "success" {
		t.Errorf("A should succeed, got %s", rm["A"])
	}
	if rm["check:A:warn_check"] != "success" {
		t.Errorf("warning check failure should be treated as success, got %s", rm["check:A:warn_check"])
	}
	if rm["B"] != "success" {
		t.Errorf("B should succeed (warning check does not block), got %s", rm["B"])
	}
}

// TestExecute_SeverityError verifies that an error-severity check failure
// behaves like the existing behavior: blocking field controls downstream skipping.
func TestExecute_SeverityError_Blocking(t *testing.T) {
	g := buildTestGraph(t,
		[]graph.AssetInput{
			{Name: "A", Type: "shell", Source: "a.sh"},
			{Name: "check:A:err_check", Type: "sql_check", Blocking: true, Severity: "error"},
			{Name: "B", Type: "shell", Source: "b.sh"},
		},
		map[string][]string{
			"check:A:err_check": {"A"},
			"B":                 {"A"},
		},
	)

	runner := func(asset *graph.Asset, pr string, rid string) NodeResult {
		if asset.Name == "check:A:err_check" {
			return NodeResult{AssetName: asset.Name, Status: "failed", Error: "check failed", ExitCode: 1}
		}
		return NodeResult{AssetName: asset.Name, Status: "success", ExitCode: 0}
	}

	rr := Execute(g, RunConfig{MaxParallel: 1}, runner)
	rm := resultMap(rr)

	if rm["check:A:err_check"] != "failed" {
		t.Errorf("error check should stay failed, got %s", rm["check:A:err_check"])
	}
	if rm["B"] != "skipped" {
		t.Errorf("B should be skipped (blocking error check), got %s", rm["B"])
	}
}

// TestExecute_SeverityError_NonBlocking verifies that a non-blocking error check
// does not skip downstream.
func TestExecute_SeverityError_NonBlocking(t *testing.T) {
	g := buildTestGraph(t,
		[]graph.AssetInput{
			{Name: "A", Type: "shell", Source: "a.sh"},
			{Name: "check:A:err_check", Type: "sql_check", Blocking: false, Severity: "error"},
			{Name: "B", Type: "shell", Source: "b.sh"},
		},
		map[string][]string{
			"check:A:err_check": {"A"},
			"B":                 {"A"},
		},
	)

	runner := func(asset *graph.Asset, pr string, rid string) NodeResult {
		if asset.Name == "check:A:err_check" {
			return NodeResult{AssetName: asset.Name, Status: "failed", Error: "check failed", ExitCode: 1}
		}
		return NodeResult{AssetName: asset.Name, Status: "success", ExitCode: 0}
	}

	rr := Execute(g, RunConfig{MaxParallel: 1}, runner)
	rm := resultMap(rr)

	if rm["check:A:err_check"] != "failed" {
		t.Errorf("error check should stay failed, got %s", rm["check:A:err_check"])
	}
	if rm["B"] != "success" {
		t.Errorf("B should succeed (non-blocking error check), got %s", rm["B"])
	}
}

// TestExecute_SeverityCritical_HaltsRun verifies that a critical-severity check
// failure triggers a run halt (Interrupted=true) and blocks downstream regardless
// of the Blocking field.
func TestExecute_SeverityCritical_HaltsRun(t *testing.T) {
	// A -> check:A:crit (critical, non-blocking field but should still block)
	// A -> B -> C (should be skipped due to critical)
	g := buildTestGraph(t,
		[]graph.AssetInput{
			{Name: "A", Type: "shell", Source: "a.sh"},
			{Name: "check:A:crit", Type: "sql_check", Blocking: false, Severity: "critical"},
			{Name: "B", Type: "shell", Source: "b.sh"},
			{Name: "C", Type: "shell", Source: "c.sh"},
		},
		map[string][]string{
			"check:A:crit": {"A"},
			"B":            {"A"},
			"C":            {"B"},
		},
	)

	runner := func(asset *graph.Asset, pr string, rid string) NodeResult {
		if asset.Name == "check:A:crit" {
			return NodeResult{AssetName: asset.Name, Status: "failed", Error: "critical check failed", ExitCode: 1}
		}
		return NodeResult{AssetName: asset.Name, Status: "success", ExitCode: 0}
	}

	rr := Execute(g, RunConfig{MaxParallel: 10}, runner)
	rm := resultMap(rr)

	if rm["A"] != "success" {
		t.Errorf("A should succeed, got %s", rm["A"])
	}
	if rm["check:A:crit"] != "failed" {
		t.Errorf("critical check should stay failed, got %s", rm["check:A:crit"])
	}
	// Downstream of A must be skipped (critical always blocks)
	if rm["B"] != "skipped" {
		t.Errorf("B should be skipped (critical check always blocks), got %s", rm["B"])
	}
	if rm["C"] != "skipped" {
		t.Errorf("C should be skipped, got %s", rm["C"])
	}
	// Run should be interrupted/halted
	if !rr.Interrupted {
		t.Error("critical check failure should halt the run (Interrupted=true)")
	}
}

// TestExecute_SeverityCritical_AlwaysBlocks verifies that critical blocks
// even when Blocking=true (consistent with error+blocking) and that it halts.
func TestExecute_SeverityCritical_ExplicitBlocking(t *testing.T) {
	g := buildTestGraph(t,
		[]graph.AssetInput{
			{Name: "A", Type: "shell", Source: "a.sh"},
			{Name: "check:A:crit", Type: "sql_check", Blocking: true, Severity: "critical"},
			{Name: "B", Type: "shell", Source: "b.sh"},
		},
		map[string][]string{
			"check:A:crit": {"A"},
			"B":            {"A"},
		},
	)

	runner := func(asset *graph.Asset, pr string, rid string) NodeResult {
		if asset.Name == "check:A:crit" {
			return NodeResult{AssetName: asset.Name, Status: "failed", Error: "critical check failed", ExitCode: 1}
		}
		return NodeResult{AssetName: asset.Name, Status: "success", ExitCode: 0}
	}

	rr := Execute(g, RunConfig{MaxParallel: 10}, runner)
	rm := resultMap(rr)

	if rm["check:A:crit"] != "failed" {
		t.Errorf("critical check should stay failed, got %s", rm["check:A:crit"])
	}
	if rm["B"] != "skipped" {
		t.Errorf("B should be skipped, got %s", rm["B"])
	}
	if !rr.Interrupted {
		t.Error("critical check failure should halt the run")
	}
}

// TestExecute_SeverityCritical_PassDoesNotHalt verifies that a passing critical
// check does not interrupt the run and allows downstream to proceed.
func TestExecute_SeverityCritical_PassDoesNotHalt(t *testing.T) {
	g := buildTestGraph(t,
		[]graph.AssetInput{
			{Name: "A", Type: "shell", Source: "a.sh"},
			{Name: "check:A:crit", Type: "sql_check", Blocking: false, Severity: "critical"},
			{Name: "B", Type: "shell", Source: "b.sh"},
		},
		map[string][]string{
			"check:A:crit": {"A"},
			"B":            {"A"},
		},
	)

	rr := Execute(g, RunConfig{MaxParallel: 10}, successRunner(0))
	rm := resultMap(rr)

	if rm["A"] != "success" {
		t.Errorf("A should succeed, got %s", rm["A"])
	}
	if rm["check:A:crit"] != "success" {
		t.Errorf("passing critical check should succeed, got %s", rm["check:A:crit"])
	}
	if rm["B"] != "success" {
		t.Errorf("B should succeed when critical check passes, got %s", rm["B"])
	}
	if rr.Interrupted {
		t.Error("passing critical check should not interrupt the run")
	}
}

// TestExecute_SeverityInfo_BlockingFieldIgnored verifies that even if Blocking=true
// is set on an info check, failure still doesn't block downstream (severity wins).
func TestExecute_SeverityInfo_BlockingFieldIgnored(t *testing.T) {
	g := buildTestGraph(t,
		[]graph.AssetInput{
			{Name: "A", Type: "shell", Source: "a.sh"},
			{Name: "check:A:info_block", Type: "sql_check", Blocking: true, Severity: "info"},
			{Name: "B", Type: "shell", Source: "b.sh"},
		},
		map[string][]string{
			"check:A:info_block": {"A"},
			"B":                  {"A"},
		},
	)

	runner := func(asset *graph.Asset, pr string, rid string) NodeResult {
		if asset.Name == "check:A:info_block" {
			return NodeResult{AssetName: asset.Name, Status: "failed", Error: "check failed", ExitCode: 1}
		}
		return NodeResult{AssetName: asset.Name, Status: "success", ExitCode: 0}
	}

	rr := Execute(g, RunConfig{MaxParallel: 1}, runner)
	rm := resultMap(rr)

	if rm["check:A:info_block"] != "success" {
		t.Errorf("info check failure should be treated as success, got %s", rm["check:A:info_block"])
	}
	if rm["B"] != "success" {
		t.Errorf("B should succeed (info severity overrides blocking field), got %s", rm["B"])
	}
}

// TestExecute_SeverityDefault_BackwardCompat verifies that checks with no severity
// (empty string, which config defaults to "error") behave identically to the
// previous error+blocking behavior.
func TestExecute_SeverityDefault_BackwardCompat(t *testing.T) {
	g := buildTestGraph(t,
		[]graph.AssetInput{
			{Name: "A", Type: "shell", Source: "a.sh"},
			{Name: "check:A:default_check", Type: "sql_check", Blocking: true, Severity: ""},
			{Name: "B", Type: "shell", Source: "b.sh"},
		},
		map[string][]string{
			"check:A:default_check": {"A"},
			"B":                     {"A"},
		},
	)

	runner := func(asset *graph.Asset, pr string, rid string) NodeResult {
		if asset.Name == "check:A:default_check" {
			return NodeResult{AssetName: asset.Name, Status: "failed", Error: "check failed", ExitCode: 1}
		}
		return NodeResult{AssetName: asset.Name, Status: "success", ExitCode: 0}
	}

	rr := Execute(g, RunConfig{MaxParallel: 1}, runner)
	rm := resultMap(rr)

	if rm["check:A:default_check"] != "failed" {
		t.Errorf("default check should fail, got %s", rm["check:A:default_check"])
	}
	if rm["B"] != "skipped" {
		t.Errorf("B should be skipped (default/error severity with blocking=true), got %s", rm["B"])
	}
	if rr.Interrupted {
		t.Error("default/error check should not interrupt the run")
	}
}
