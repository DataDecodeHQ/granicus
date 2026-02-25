package events

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestIntegration_FullEventSequence(t *testing.T) {
	store := newTestStore(t)

	runID := GenerateRunID()
	pipeline := "test_pipe"

	// Simulate: run_started -> 3 nodes (a success, b fail, c skipped) -> run_completed
	store.Emit(Event{
		RunID: runID, Pipeline: pipeline, EventType: "run_started", Severity: "info",
		Summary: "Pipeline started",
		Details: map[string]any{"asset_count": 3, "max_parallel": 2},
	})

	// Node A started + succeeded
	store.Emit(Event{
		RunID: runID, Pipeline: pipeline, Asset: "a",
		EventType: "node_started", Severity: "info",
		Summary: "Node a started",
	})
	store.Emit(Event{
		RunID: runID, Pipeline: pipeline, Asset: "a",
		EventType: "node_succeeded", Severity: "info",
		DurationMs: 150,
		Summary:    "Node a succeeded",
		Details:    map[string]any{"metadata": map[string]any{"rows": "100"}},
	})

	// Node B started + failed
	store.Emit(Event{
		RunID: runID, Pipeline: pipeline, Asset: "b",
		EventType: "node_started", Severity: "info",
		Summary: "Node b started",
	})
	store.Emit(Event{
		RunID: runID, Pipeline: pipeline, Asset: "b",
		EventType: "node_failed", Severity: "error",
		DurationMs: 200,
		Summary:    "Node b failed: exit 1",
		Details:    map[string]any{"error_message": "exit 1", "exit_code": 1},
	})

	// Node C skipped (depends on B)
	store.Emit(Event{
		RunID: runID, Pipeline: pipeline, Asset: "c",
		EventType: "node_skipped", Severity: "warning",
		Summary: "Node c skipped: dependency failed",
	})

	// Run completed
	store.Emit(Event{
		RunID: runID, Pipeline: pipeline, EventType: "run_completed", Severity: "info",
		DurationMs: 500,
		Summary:    "Run completed_with_failures: 1 succeeded, 1 failed, 1 skipped",
		Details: map[string]any{
			"status": "completed_with_failures", "succeeded": 1, "failed": 1, "skipped": 1,
			"total_nodes": 3, "duration_seconds": 0.5,
		},
	})

	// Verify event sequence
	allEvents, err := store.Query(QueryFilters{RunID: runID})
	if err != nil {
		t.Fatal(err)
	}
	if len(allEvents) != 7 {
		t.Fatalf("expected 7 events, got %d", len(allEvents))
	}

	expectedTypes := []string{
		"run_started", "node_started", "node_succeeded",
		"node_started", "node_failed", "node_skipped", "run_completed",
	}
	for i, want := range expectedTypes {
		if allEvents[i].EventType != want {
			t.Errorf("event %d: expected %s, got %s", i, want, allEvents[i].EventType)
		}
	}

	// Verify query by type
	succeeded, _ := store.Query(QueryFilters{RunID: runID, EventType: "node_succeeded"})
	if len(succeeded) != 1 || succeeded[0].Asset != "a" {
		t.Errorf("node_succeeded query: %v", succeeded)
	}

	failed, _ := store.Query(QueryFilters{RunID: runID, EventType: "node_failed"})
	if len(failed) != 1 || failed[0].Asset != "b" {
		t.Errorf("node_failed query: %v", failed)
	}

	skipped, _ := store.Query(QueryFilters{RunID: runID, EventType: "node_skipped"})
	if len(skipped) != 1 || skipped[0].Asset != "c" {
		t.Errorf("node_skipped query: %v", skipped)
	}

	// Verify query by asset
	assetB, _ := store.Query(QueryFilters{RunID: runID, Asset: "b"})
	if len(assetB) != 2 { // started + failed
		t.Errorf("asset=b query: expected 2 events, got %d", len(assetB))
	}

	// Verify run summary
	summary, err := store.GetRunSummary(runID)
	if err != nil {
		t.Fatalf("GetRunSummary: %v", err)
	}
	if summary.Status != "completed_with_failures" {
		t.Errorf("status: %q", summary.Status)
	}
	if summary.Succeeded != 1 {
		t.Errorf("succeeded: %d", summary.Succeeded)
	}
	if summary.Failed != 1 {
		t.Errorf("failed: %d", summary.Failed)
	}
	if summary.Skipped != 1 {
		t.Errorf("skipped: %d", summary.Skipped)
	}

	// Verify GetFailedNodes
	failedNodes, _ := store.GetFailedNodes(runID)
	if len(failedNodes) != 1 || failedNodes[0] != "b" {
		t.Errorf("GetFailedNodes: %v", failedNodes)
	}

	// Verify GetNodeResults
	nodeResults, _ := store.GetNodeResults(runID)
	if len(nodeResults) != 3 {
		t.Fatalf("GetNodeResults: expected 3, got %d", len(nodeResults))
	}

	statusMap := make(map[string]string)
	for _, nr := range nodeResults {
		statusMap[nr.Asset] = nr.Status
	}
	if statusMap["a"] != "success" {
		t.Errorf("node a status: %q", statusMap["a"])
	}
	if statusMap["b"] != "failed" {
		t.Errorf("node b status: %q", statusMap["b"])
	}
	if statusMap["c"] != "skipped" {
		t.Errorf("node c status: %q", statusMap["c"])
	}

	// Verify severity filter
	errors, _ := store.Query(QueryFilters{RunID: runID, Severity: "error"})
	if len(errors) != 1 {
		t.Errorf("severity=error: expected 1, got %d", len(errors))
	}
	warnings, _ := store.Query(QueryFilters{RunID: runID, Severity: "warning"})
	if len(warnings) != 2 { // error + warning
		t.Errorf("severity>=warning: expected 2, got %d", len(warnings))
	}
}

func TestIntegration_ModelVersionTracking(t *testing.T) {
	store := newTestStore(t)

	dir := t.TempDir()
	srcFile := filepath.Join(dir, "model.sql")
	os.WriteFile(srcFile, []byte("SELECT 1"), 0644)
	hash1 := HashBytes([]byte("SELECT 1"))

	runID1 := "run_model_001"

	// First run: model should be registered
	changed, version, err := store.RecordModelVersion("asset1", srcFile, hash1, runID1)
	if err != nil {
		t.Fatal(err)
	}
	if !changed || version != 1 {
		t.Errorf("first record: changed=%v, version=%d", changed, version)
	}

	// Verify model_registered event
	registered, _ := store.Query(QueryFilters{EventType: "model_registered"})
	if len(registered) != 1 {
		t.Fatalf("expected 1 model_registered event, got %d", len(registered))
	}
	if registered[0].Asset != "asset1" {
		t.Errorf("model_registered asset: %q", registered[0].Asset)
	}

	// Second run with same hash: no new event
	changed, version, _ = store.RecordModelVersion("asset1", srcFile, hash1, "run_model_002")
	if changed {
		t.Error("same hash should not trigger change")
	}
	if version != 1 {
		t.Errorf("version should still be 1, got %d", version)
	}

	// No model_changed event should exist
	changed2, _ := store.Query(QueryFilters{EventType: "model_changed"})
	if len(changed2) != 0 {
		t.Errorf("expected 0 model_changed events, got %d", len(changed2))
	}

	// Third run with different source: model_changed event
	os.WriteFile(srcFile, []byte("SELECT 2"), 0644)
	hash2 := HashBytes([]byte("SELECT 2"))

	changed, version, _ = store.RecordModelVersion("asset1", srcFile, hash2, "run_model_003")
	if !changed || version != 2 {
		t.Errorf("hash change: changed=%v, version=%d", changed, version)
	}

	changedEvents, _ := store.Query(QueryFilters{EventType: "model_changed"})
	if len(changedEvents) != 1 {
		t.Fatalf("expected 1 model_changed event, got %d", len(changedEvents))
	}
	if changedEvents[0].Details["version"] != float64(2) {
		t.Errorf("model_changed version: %v", changedEvents[0].Details["version"])
	}

	// Verify history
	history, _ := store.GetModelHistory("asset1")
	if len(history) != 2 {
		t.Fatalf("expected 2 versions, got %d", len(history))
	}
	if history[0].Version != 2 || history[1].Version != 1 {
		t.Error("history should be newest-first")
	}
	if history[1].ReplacedAt == "" {
		t.Error("v1 should have replaced_at set")
	}
}

func TestIntegration_CheckEvents(t *testing.T) {
	store := newTestStore(t)

	runID := GenerateRunID()

	// Emit check results
	store.Emit(Event{
		RunID: runID, Pipeline: "p", Asset: "check:orders:not_null",
		EventType: "check_passed", Severity: "info",
		Summary: "Check passed: 0 rows",
		Details: map[string]any{"check_total_rows": "0"},
	})
	store.Emit(Event{
		RunID: runID, Pipeline: "p", Asset: "check:orders:unique_id",
		EventType: "check_failed", Severity: "error",
		Summary: "Check failed: 5 rows",
		Details: map[string]any{
			"check_total_rows":  "5",
			"check_sample_rows": `[{"id":1},{"id":2}]`,
		},
	})

	// Verify check query
	checks, err := store.GetCheckResults(runID, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(checks) != 2 {
		t.Fatalf("expected 2 check events, got %d", len(checks))
	}

	// Filter by asset
	orderChecks, _ := store.GetCheckResults(runID, "check:orders:unique_id")
	if len(orderChecks) != 1 {
		t.Errorf("expected 1 check for unique_id, got %d", len(orderChecks))
	}
}

func TestIntegration_GCDeletesOldEvents(t *testing.T) {
	store := newTestStore(t)

	old := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	recent := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)

	// Emit old and recent events
	for i := 0; i < 5; i++ {
		store.Emit(Event{
			RunID: "old_run", Pipeline: "p", EventType: "node_succeeded",
			Asset: fmt.Sprintf("old_%d", i), Timestamp: old,
		})
	}
	for i := 0; i < 3; i++ {
		store.Emit(Event{
			RunID: "new_run", Pipeline: "p", EventType: "node_succeeded",
			Asset: fmt.Sprintf("new_%d", i), Timestamp: recent,
		})
	}

	// Also register a model (should survive GC)
	store.RecordModelVersion("kept_model", "", HashBytes([]byte("test")), "old_run")

	cutoff := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	deleted, err := store.DeleteBefore(cutoff)
	if err != nil {
		t.Fatal(err)
	}
	if deleted < 5 {
		t.Errorf("expected at least 5 deleted, got %d", deleted)
	}

	// Old events gone
	oldEvents, _ := store.Query(QueryFilters{RunID: "old_run"})
	for _, e := range oldEvents {
		if e.EventType == "node_succeeded" {
			t.Error("old node_succeeded events should be deleted")
		}
	}

	// Recent events preserved
	newEvents, _ := store.Query(QueryFilters{RunID: "new_run"})
	if len(newEvents) != 3 {
		t.Errorf("expected 3 recent events, got %d", len(newEvents))
	}

	// Model registry preserved
	v, _, err := store.GetModelVersion("kept_model")
	if err != nil {
		t.Fatal("model should survive GC")
	}
	if v != 1 {
		t.Errorf("model version: %d", v)
	}
}

func TestIntegration_MultiRunListAndQuery(t *testing.T) {
	store := newTestStore(t)

	// Create 3 runs
	for i, rid := range []string{"run_001", "run_002", "run_003"} {
		ts := time.Date(2026, 2, 1+i, 0, 0, 0, 0, time.UTC)
		store.Emit(Event{
			RunID: rid, Pipeline: "p", EventType: "run_started",
			Severity: "info", Timestamp: ts,
		})
		store.Emit(Event{
			RunID: rid, Pipeline: "p", EventType: "run_completed",
			Severity: "info", Timestamp: ts.Add(time.Minute),
			Details: map[string]any{
				"status": "success", "succeeded": i + 1, "failed": 0, "skipped": 0,
				"total_nodes": i + 1, "duration_seconds": 60.0,
			},
		})
	}

	// List runs with limit
	runs, err := store.ListRuns(2)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 2 {
		t.Fatalf("expected 2 runs, got %d", len(runs))
	}
	if runs[0].RunID != "run_003" {
		t.Errorf("expected run_003 first, got %s", runs[0].RunID)
	}

	// Query by pipeline
	pipeEvents, _ := store.Query(QueryFilters{Pipeline: "p"})
	if len(pipeEvents) != 6 { // 3 started + 3 completed
		t.Errorf("expected 6 pipeline events, got %d", len(pipeEvents))
	}

	// Query with time range
	since := time.Date(2026, 2, 2, 0, 0, 0, 0, time.UTC)
	filtered, _ := store.Query(QueryFilters{Pipeline: "p", Since: since})
	if len(filtered) != 4 { // run_002 + run_003
		t.Errorf("expected 4 events since Feb 2, got %d", len(filtered))
	}
}
