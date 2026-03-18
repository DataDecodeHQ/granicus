package executor

import (
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/DataDecodeHQ/granicus/internal/graph"
	"github.com/DataDecodeHQ/granicus/internal/state"
)

func newTestState(t *testing.T) *state.Store {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), ".granicus", "state.db")
	s, err := state.New(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

type mockCall struct {
	AssetName     string
	IntervalStart string
	IntervalEnd   string
}

func mockRunner(calls *[]mockCall, failOn map[string]bool) RunnerFunc {
	var mu sync.Mutex
	return func(asset *graph.Asset, projectRoot string, runID string) NodeResult {
		call := mockCall{
			AssetName:     asset.Name,
			IntervalStart: asset.IntervalStart,
			IntervalEnd:   asset.IntervalEnd,
		}
		mu.Lock()
		*calls = append(*calls, call)
		mu.Unlock()

		key := asset.Name + ":" + asset.IntervalStart
		if failOn[key] {
			return NodeResult{
				AssetName: asset.Name,
				Status:    "failed",
				Error:     "mock failure at " + asset.IntervalStart,
				ExitCode:  1,
				StartTime: time.Now(),
				EndTime:   time.Now(),
			}
		}

		return NodeResult{
			AssetName: asset.Name,
			Status:    "success",
			ExitCode:  0,
			StartTime: time.Now(),
			EndTime:   time.Now(),
		}
	}
}

func buildSimpleGraph(t *testing.T, assets map[string]graph.Asset) *graph.Graph {
	t.Helper()
	inputs := make([]graph.AssetInput, 0)
	deps := make(map[string][]string)
	for _, a := range assets {
		inputs = append(inputs, graph.AssetInput{
			Name:         a.Name,
			Type:         a.Type,
			Source:        a.Source,
			TimeColumn:   a.TimeColumn,
			IntervalUnit: a.IntervalUnit,
			StartDate:    a.StartDate,
			BatchSize:    a.BatchSize,
			Lookback:     a.Lookback,
			SourceAsset:  a.SourceAsset,
		})
		if len(a.DependsOn) > 0 {
			deps[a.Name] = a.DependsOn
		}
	}
	g, err := graph.BuildGraph(inputs, deps)
	if err != nil {
		t.Fatal(err)
	}
	return g
}

// Test 1: Initial backfill processes all intervals
func TestInterval_InitialBackfill(t *testing.T) {
	store := newTestState(t)
	var calls []mockCall

	g := buildSimpleGraph(t, map[string]graph.Asset{
		"asset1": {Name: "asset1", Type: "shell", Source: "a.sh", TimeColumn: "dt", IntervalUnit: "day", StartDate: "2025-01-01"},
	})

	rr := Execute(g, RunConfig{
		MaxParallel: 1,
		ProjectRoot: t.TempDir(),
		RunID:       "r1",
		ToDate:      "2025-01-04",
		StateStore:  store,
	}, mockRunner(&calls, nil))

	// Should process 3 intervals: Jan 1, 2, 3
	if len(calls) != 3 {
		t.Fatalf("expected 3 calls, got %d", len(calls))
	}
	if calls[0].IntervalStart != "2025-01-01" {
		t.Errorf("first call: %v", calls[0])
	}
	if calls[2].IntervalStart != "2025-01-03" {
		t.Errorf("last call: %v", calls[2])
	}

	rm := resultMap(rr)
	if rm["asset1"] != "success" {
		t.Errorf("asset1: %s", rm["asset1"])
	}

	// Verify state store
	intervals, _ := store.GetIntervals("asset1")
	if len(intervals) != 3 {
		t.Errorf("expected 3 intervals in state, got %d", len(intervals))
	}
	for _, iv := range intervals {
		if iv.Status != "complete" {
			t.Errorf("interval %s status: %s", iv.IntervalStart, iv.Status)
		}
	}
}

// Test 2: Incremental run — only new intervals processed
func TestInterval_IncrementalRun(t *testing.T) {
	store := newTestState(t)

	g := buildSimpleGraph(t, map[string]graph.Asset{
		"a": {Name: "a", Type: "shell", Source: "a.sh", TimeColumn: "dt", IntervalUnit: "day", StartDate: "2025-01-01"},
	})

	// First run: backfill Jan 1-3
	var calls1 []mockCall
	Execute(g, RunConfig{
		MaxParallel: 1, ProjectRoot: t.TempDir(), RunID: "r1",
		ToDate: "2025-01-04", StateStore: store,
	}, mockRunner(&calls1, nil))

	if len(calls1) != 3 {
		t.Fatalf("backfill: expected 3, got %d", len(calls1))
	}

	// Second run: extend to Jan 6
	var calls2 []mockCall
	Execute(g, RunConfig{
		MaxParallel: 1, ProjectRoot: t.TempDir(), RunID: "r2",
		ToDate: "2025-01-07", StateStore: store,
	}, mockRunner(&calls2, nil))

	// Should only process Jan 4, 5, 6
	if len(calls2) != 3 {
		t.Fatalf("incremental: expected 3, got %d", len(calls2))
	}
	if calls2[0].IntervalStart != "2025-01-04" {
		t.Errorf("first new: %v", calls2[0])
	}
}

// Test 3: Gap fill
func TestInterval_GapFill(t *testing.T) {
	store := newTestState(t)

	g := buildSimpleGraph(t, map[string]graph.Asset{
		"a": {Name: "a", Type: "shell", Source: "a.sh", TimeColumn: "dt", IntervalUnit: "day", StartDate: "2025-01-01"},
	})

	// Backfill all 5 days
	var calls1 []mockCall
	Execute(g, RunConfig{
		MaxParallel: 1, ProjectRoot: t.TempDir(), RunID: "r1",
		ToDate: "2025-01-06", StateStore: store,
	}, mockRunner(&calls1, nil))

	// Delete Jan 3 from state (simulate gap)
	store.InvalidateAll("a") // clear all
	// Re-add all except Jan 3
	for _, c := range calls1 {
		if c.IntervalStart != "2025-01-03" {
			store.MarkInProgress("a", c.IntervalStart, c.IntervalEnd, "r1")
			store.MarkComplete("a", c.IntervalStart, c.IntervalEnd)
		}
	}

	// Run again
	var calls2 []mockCall
	Execute(g, RunConfig{
		MaxParallel: 1, ProjectRoot: t.TempDir(), RunID: "r2",
		ToDate: "2025-01-06", StateStore: store,
	}, mockRunner(&calls2, nil))

	// Should only process Jan 3 (the gap)
	if len(calls2) != 1 {
		t.Fatalf("gap fill: expected 1, got %d: %v", len(calls2), calls2)
	}
	if calls2[0].IntervalStart != "2025-01-03" {
		t.Errorf("gap: %v", calls2[0])
	}
}

// Test 4: Lookback reprocessing
func TestInterval_Lookback(t *testing.T) {
	store := newTestState(t)

	g := buildSimpleGraph(t, map[string]graph.Asset{
		"a": {Name: "a", Type: "shell", Source: "a.sh", TimeColumn: "dt", IntervalUnit: "day", StartDate: "2025-01-01", Lookback: 2},
	})

	// Backfill Jan 1-5
	var calls1 []mockCall
	Execute(g, RunConfig{
		MaxParallel: 1, ProjectRoot: t.TempDir(), RunID: "r1",
		ToDate: "2025-01-06", StateStore: store,
	}, mockRunner(&calls1, nil))
	if len(calls1) != 5 {
		t.Fatalf("backfill: expected 5, got %d", len(calls1))
	}

	// Run again with same date range
	var calls2 []mockCall
	Execute(g, RunConfig{
		MaxParallel: 1, ProjectRoot: t.TempDir(), RunID: "r2",
		ToDate: "2025-01-06", StateStore: store,
	}, mockRunner(&calls2, nil))

	// lookback=2 should reprocess last 2 intervals (Jan 4, Jan 5)
	if len(calls2) != 2 {
		t.Fatalf("lookback: expected 2, got %d: %v", len(calls2), calls2)
	}
}

// Test 5: Batch size limits intervals per run
func TestInterval_BatchSize(t *testing.T) {
	store := newTestState(t)

	g := buildSimpleGraph(t, map[string]graph.Asset{
		"a": {Name: "a", Type: "shell", Source: "a.sh", TimeColumn: "dt", IntervalUnit: "day", StartDate: "2025-01-01", BatchSize: 3},
	})

	// 10 intervals available (Jan 1-10), batch_size=3
	var calls []mockCall
	Execute(g, RunConfig{
		MaxParallel: 1, ProjectRoot: t.TempDir(), RunID: "r1",
		ToDate: "2025-01-11", StateStore: store,
	}, mockRunner(&calls, nil))

	if len(calls) != 3 {
		t.Fatalf("batch: expected 3, got %d", len(calls))
	}
}

// Test 6: Full refresh alongside incremental
func TestInterval_FullRefreshAssetAlongsideIncremental(t *testing.T) {
	store := newTestState(t)
	var calls []mockCall

	g := buildSimpleGraph(t, map[string]graph.Asset{
		"incremental": {Name: "incremental", Type: "shell", Source: "a.sh", TimeColumn: "dt", IntervalUnit: "day", StartDate: "2025-01-01"},
		"fullrefresh": {Name: "fullrefresh", Type: "shell", Source: "b.sh"},
	})

	Execute(g, RunConfig{
		MaxParallel: 2, ProjectRoot: t.TempDir(), RunID: "r1",
		ToDate: "2025-01-03", StateStore: store,
	}, mockRunner(&calls, nil))

	// incremental: 2 intervals, fullrefresh: 1 call
	incrementalCalls := 0
	fullRefreshCalls := 0
	for _, c := range calls {
		if c.AssetName == "incremental" {
			incrementalCalls++
		} else if c.AssetName == "fullrefresh" {
			fullRefreshCalls++
		}
	}
	if incrementalCalls != 2 {
		t.Errorf("incremental: expected 2, got %d", incrementalCalls)
	}
	if fullRefreshCalls != 1 {
		t.Errorf("fullrefresh: expected 1, got %d", fullRefreshCalls)
	}
}

// Test 7: --from-date override
func TestInterval_FromDateOverride(t *testing.T) {
	store := newTestState(t)
	var calls []mockCall

	g := buildSimpleGraph(t, map[string]graph.Asset{
		"a": {Name: "a", Type: "shell", Source: "a.sh", TimeColumn: "dt", IntervalUnit: "day", StartDate: "2025-01-01"},
	})

	Execute(g, RunConfig{
		MaxParallel: 1, ProjectRoot: t.TempDir(), RunID: "r1",
		FromDate: "2025-01-08", ToDate: "2025-01-11", StateStore: store,
	}, mockRunner(&calls, nil))

	// Should start from Jan 8, not Jan 1
	if len(calls) != 3 {
		t.Fatalf("from-date: expected 3, got %d", len(calls))
	}
	if calls[0].IntervalStart != "2025-01-08" {
		t.Errorf("first: %v", calls[0])
	}
}

// Test 12: Interval failure stops sequence
func TestInterval_FailureStopsSequence(t *testing.T) {
	store := newTestState(t)
	var calls []mockCall

	g := buildSimpleGraph(t, map[string]graph.Asset{
		"a": {Name: "a", Type: "shell", Source: "a.sh", TimeColumn: "dt", IntervalUnit: "day", StartDate: "2025-01-01"},
	})

	failOn := map[string]bool{"a:2025-01-03": true}

	rr := Execute(g, RunConfig{
		MaxParallel: 1, ProjectRoot: t.TempDir(), RunID: "r1",
		ToDate: "2025-01-06", StateStore: store,
	}, mockRunner(&calls, failOn))

	// 5 pending, interval 3 fails: 1-2 executed, 3 failed, 4-5 not attempted
	if len(calls) != 3 {
		t.Fatalf("expected 3 calls (2 success + 1 fail), got %d", len(calls))
	}

	rm := resultMap(rr)
	if rm["a"] != "failed" {
		t.Errorf("asset should be failed: %s", rm["a"])
	}

	// Verify state: 1-2 complete, 3 failed, 4-5 absent
	intervals, _ := store.GetIntervals("a")
	statusMap := make(map[string]string)
	for _, iv := range intervals {
		statusMap[iv.IntervalStart] = iv.Status
	}
	if statusMap["2025-01-01"] != "complete" {
		t.Errorf("jan 1: %s", statusMap["2025-01-01"])
	}
	if statusMap["2025-01-02"] != "complete" {
		t.Errorf("jan 2: %s", statusMap["2025-01-02"])
	}
	if statusMap["2025-01-03"] != "failed" {
		t.Errorf("jan 3: %s", statusMap["2025-01-03"])
	}
	if _, ok := statusMap["2025-01-04"]; ok {
		t.Error("jan 4 should not be in state")
	}
}

// Test 13: Failed interval re-run
func TestInterval_FailedIntervalRerun(t *testing.T) {
	store := newTestState(t)

	g := buildSimpleGraph(t, map[string]graph.Asset{
		"a": {Name: "a", Type: "shell", Source: "a.sh", TimeColumn: "dt", IntervalUnit: "day", StartDate: "2025-01-01"},
	})

	// First run: fail at Jan 3
	var calls1 []mockCall
	failOn := map[string]bool{"a:2025-01-03": true}
	Execute(g, RunConfig{
		MaxParallel: 1, ProjectRoot: t.TempDir(), RunID: "r1",
		ToDate: "2025-01-06", StateStore: store,
	}, mockRunner(&calls1, failOn))

	// Re-run: no failures
	var calls2 []mockCall
	Execute(g, RunConfig{
		MaxParallel: 1, ProjectRoot: t.TempDir(), RunID: "r2",
		ToDate: "2025-01-06", StateStore: store,
	}, mockRunner(&calls2, nil))

	// Should process Jan 3 (failed) + Jan 4, 5 (never attempted)
	if len(calls2) != 3 {
		t.Fatalf("rerun: expected 3, got %d: %v", len(calls2), calls2)
	}
	if calls2[0].IntervalStart != "2025-01-03" {
		t.Errorf("first rerun: %v", calls2[0])
	}
}

// Test 14: Cross-asset independence
func TestInterval_CrossAssetIndependence(t *testing.T) {
	store := newTestState(t)
	var calls []mockCall

	g := buildSimpleGraph(t, map[string]graph.Asset{
		"upstream":   {Name: "upstream", Type: "shell", Source: "a.sh", TimeColumn: "dt", IntervalUnit: "day", StartDate: "2025-01-01"},
		"downstream": {Name: "downstream", Type: "shell", Source: "b.sh", TimeColumn: "dt", IntervalUnit: "day", StartDate: "2025-01-01", DependsOn: []string{"upstream"}},
		"independent": {Name: "independent", Type: "shell", Source: "c.sh", TimeColumn: "dt", IntervalUnit: "day", StartDate: "2025-01-01"},
	})

	failOn := map[string]bool{"upstream:2025-01-02": true}

	rr := Execute(g, RunConfig{
		MaxParallel: 2, ProjectRoot: t.TempDir(), RunID: "r1",
		ToDate: "2025-01-04", StateStore: store,
	}, mockRunner(&calls, failOn))

	rm := resultMap(rr)
	// upstream fails, downstream skipped, independent succeeds
	if rm["upstream"] != "failed" {
		t.Errorf("upstream: %s", rm["upstream"])
	}
	if rm["downstream"] != "skipped" {
		t.Errorf("downstream: %s", rm["downstream"])
	}
	if rm["independent"] != "success" {
		t.Errorf("independent: %s", rm["independent"])
	}
}

// Test 15: Crash recovery — in_progress treated as unprocessed
func TestInterval_CrashRecovery(t *testing.T) {
	store := newTestState(t)

	// Simulate crash: mark interval in_progress
	store.MarkInProgress("a", "2025-01-01", "2025-01-02", "crashed-run")

	g := buildSimpleGraph(t, map[string]graph.Asset{
		"a": {Name: "a", Type: "shell", Source: "a.sh", TimeColumn: "dt", IntervalUnit: "day", StartDate: "2025-01-01"},
	})

	var calls []mockCall
	Execute(g, RunConfig{
		MaxParallel: 1, ProjectRoot: t.TempDir(), RunID: "r2",
		ToDate: "2025-01-03", StateStore: store,
	}, mockRunner(&calls, nil))

	// Should re-execute Jan 1 (was in_progress) + Jan 2 (new)
	if len(calls) != 2 {
		t.Fatalf("crash recovery: expected 2, got %d", len(calls))
	}
	if calls[0].IntervalStart != "2025-01-01" {
		t.Errorf("first: %v", calls[0])
	}
}

// Test 16: Full refresh on incremental asset
func TestInterval_FullRefreshOnIncremental(t *testing.T) {
	store := newTestState(t)

	g := buildSimpleGraph(t, map[string]graph.Asset{
		"a": {Name: "a", Type: "shell", Source: "a.sh", TimeColumn: "dt", IntervalUnit: "day", StartDate: "2025-01-01"},
	})

	// First: backfill 5 days
	var calls1 []mockCall
	Execute(g, RunConfig{
		MaxParallel: 1, ProjectRoot: t.TempDir(), RunID: "r1",
		ToDate: "2025-01-06", StateStore: store,
	}, mockRunner(&calls1, nil))
	if len(calls1) != 5 {
		t.Fatalf("backfill: %d", len(calls1))
	}

	// Full refresh: should re-execute all 5
	var calls2 []mockCall
	Execute(g, RunConfig{
		MaxParallel: 1, ProjectRoot: t.TempDir(), RunID: "r2",
		ToDate: "2025-01-06", StateStore: store,
		FullRefresh: true,
	}, mockRunner(&calls2, nil))

	if len(calls2) != 5 {
		t.Fatalf("full refresh: expected 5, got %d", len(calls2))
	}
}

// Test 17: Full refresh with failure
func TestInterval_FullRefreshWithFailure(t *testing.T) {
	store := newTestState(t)

	g := buildSimpleGraph(t, map[string]graph.Asset{
		"a": {Name: "a", Type: "shell", Source: "a.sh", TimeColumn: "dt", IntervalUnit: "day", StartDate: "2025-01-01"},
	})

	// Backfill 5 days
	var calls1 []mockCall
	Execute(g, RunConfig{
		MaxParallel: 1, ProjectRoot: t.TempDir(), RunID: "r1",
		ToDate: "2025-01-06", StateStore: store,
	}, mockRunner(&calls1, nil))

	// Full refresh, fail at interval 3
	var calls2 []mockCall
	failOn := map[string]bool{"a:2025-01-03": true}
	Execute(g, RunConfig{
		MaxParallel: 1, ProjectRoot: t.TempDir(), RunID: "r2",
		ToDate: "2025-01-06", StateStore: store,
		FullRefresh: true,
	}, mockRunner(&calls2, failOn))

	// 1-2 complete, 3 failed, 4-5 not attempted
	if len(calls2) != 3 {
		t.Fatalf("expected 3 calls, got %d", len(calls2))
	}

	intervals, _ := store.GetIntervals("a")
	statusMap := make(map[string]string)
	for _, iv := range intervals {
		statusMap[iv.IntervalStart] = iv.Status
	}
	if statusMap["2025-01-01"] != "complete" {
		t.Errorf("jan 1: %s", statusMap["2025-01-01"])
	}
	if statusMap["2025-01-03"] != "failed" {
		t.Errorf("jan 3: %s", statusMap["2025-01-03"])
	}
}

// Test 18: dlt/full-refresh asset ignores intervals
func TestInterval_FullRefreshAssetNoState(t *testing.T) {
	store := newTestState(t)
	var calls []mockCall

	g := buildSimpleGraph(t, map[string]graph.Asset{
		"dlt_asset": {Name: "dlt_asset", Type: "dlt", Source: "load.py"},
	})

	Execute(g, RunConfig{
		MaxParallel: 1, ProjectRoot: t.TempDir(), RunID: "r1",
		StateStore: store,
	}, mockRunner(&calls, nil))

	if len(calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(calls))
	}
	if calls[0].IntervalStart != "" {
		t.Errorf("should have empty interval: %v", calls[0])
	}

	// No state rows
	intervals, _ := store.GetIntervals("dlt_asset")
	if len(intervals) != 0 {
		t.Errorf("expected no state for full-refresh asset, got %d", len(intervals))
	}
}

// Test 19: Multi-output failure
func TestInterval_MultiOutputFailure(t *testing.T) {
	store := newTestState(t)
	var calls []mockCall

	g := buildSimpleGraph(t, map[string]graph.Asset{
		"out_a":      {Name: "out_a", Type: "python", Source: "extract.py", SourceAsset: "extract"},
		"out_b":      {Name: "out_b", Type: "python", Source: "extract.py", SourceAsset: "extract"},
		"downstream": {Name: "downstream", Type: "shell", Source: "d.sh", DependsOn: []string{"out_a", "out_b"}},
	})

	// Fail when executing extract.py (either output may run first due to map ordering)
	failOn := map[string]bool{"out_a:": true, "out_b:": true}

	rr := Execute(g, RunConfig{
		MaxParallel: 1, ProjectRoot: t.TempDir(), RunID: "r1",
		StateStore: store,
	}, mockRunner(&calls, failOn))

	rm := resultMap(rr)
	// out_a fails, out_b gets same result via dedup, downstream skipped
	if rm["out_a"] != "failed" {
		t.Errorf("out_a: %s", rm["out_a"])
	}
	if rm["out_b"] != "failed" {
		t.Errorf("out_b: %s (should get same result via dedup)", rm["out_b"])
	}
	if rm["downstream"] != "skipped" {
		t.Errorf("downstream: %s", rm["downstream"])
	}
}

