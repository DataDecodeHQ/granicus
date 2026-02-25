package events

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "events.db")
	s, err := New(dbPath)
	if err != nil {
		t.Fatalf("creating store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestEmit_RoundTrip(t *testing.T) {
	s := newTestStore(t)
	event := Event{
		RunID:     "run_001",
		Pipeline:  "test_pipe",
		Asset:     "my_asset",
		EventType: "node_succeeded",
		Severity:  "info",
		Summary:   "completed",
		Details:   map[string]any{"rows": 42, "note": "ok"},
	}

	if err := s.Emit(event); err != nil {
		t.Fatal(err)
	}

	events, err := s.Query(QueryFilters{RunID: "run_001"})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}

	e := events[0]
	if e.EventID == "" {
		t.Error("expected auto-generated event ID")
	}
	if e.Pipeline != "test_pipe" {
		t.Errorf("pipeline: got %q", e.Pipeline)
	}
	if e.Asset != "my_asset" {
		t.Errorf("asset: got %q", e.Asset)
	}
	if e.Details["note"] != "ok" {
		t.Errorf("details.note: got %v", e.Details["note"])
	}
	// JSON numbers deserialize as float64
	if v, ok := e.Details["rows"].(float64); !ok || v != 42 {
		t.Errorf("details.rows: got %v", e.Details["rows"])
	}
}

func TestEmit_AutoTimestamp(t *testing.T) {
	s := newTestStore(t)
	before := time.Now().Add(-time.Second)

	s.Emit(Event{RunID: "r", Pipeline: "p", EventType: "test"})

	events, _ := s.Query(QueryFilters{RunID: "r"})
	if len(events) != 1 {
		t.Fatalf("expected 1 event")
	}
	if events[0].Timestamp.Before(before) {
		t.Error("timestamp should be auto-set to now")
	}
}

func TestEmit_ULIDUniqueness(t *testing.T) {
	s := newTestStore(t)
	for i := 0; i < 10; i++ {
		s.Emit(Event{RunID: "r", Pipeline: "p", EventType: "test", Summary: "event"})
	}

	events, _ := s.Query(QueryFilters{RunID: "r"})
	seen := make(map[string]bool)
	for _, e := range events {
		if seen[e.EventID] {
			t.Errorf("duplicate ULID: %s", e.EventID)
		}
		seen[e.EventID] = true
		if len(e.EventID) != 26 {
			t.Errorf("ULID wrong length: %q (%d)", e.EventID, len(e.EventID))
		}
	}
}

func TestEmit_ConcurrentSafety(t *testing.T) {
	s := newTestStore(t)
	var wg sync.WaitGroup
	n := 50
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			s.Emit(Event{RunID: "r", Pipeline: "p", EventType: "test"})
		}()
	}
	wg.Wait()

	events, err := s.Query(QueryFilters{RunID: "r"})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != n {
		t.Errorf("expected %d events, got %d", n, len(events))
	}
}

func TestEmitBatch(t *testing.T) {
	s := newTestStore(t)
	batch := []Event{
		{RunID: "r1", Pipeline: "p", EventType: "a"},
		{RunID: "r1", Pipeline: "p", EventType: "b"},
		{RunID: "r1", Pipeline: "p", EventType: "c"},
	}
	if err := s.EmitBatch(batch); err != nil {
		t.Fatal(err)
	}

	events, _ := s.Query(QueryFilters{RunID: "r1"})
	if len(events) != 3 {
		t.Fatalf("expected 3, got %d", len(events))
	}
}

func TestQuery_FilterByEventType(t *testing.T) {
	s := newTestStore(t)
	s.Emit(Event{RunID: "r", Pipeline: "p", EventType: "node_succeeded"})
	s.Emit(Event{RunID: "r", Pipeline: "p", EventType: "node_failed"})
	s.Emit(Event{RunID: "r", Pipeline: "p", EventType: "run_started"})

	events, _ := s.Query(QueryFilters{RunID: "r", EventType: "node_succeeded,node_failed"})
	if len(events) != 2 {
		t.Errorf("expected 2, got %d", len(events))
	}
}

func TestQuery_FilterBySeverity(t *testing.T) {
	s := newTestStore(t)
	s.Emit(Event{RunID: "r", Pipeline: "p", EventType: "a", Severity: "info"})
	s.Emit(Event{RunID: "r", Pipeline: "p", EventType: "b", Severity: "warning"})
	s.Emit(Event{RunID: "r", Pipeline: "p", EventType: "c", Severity: "error"})

	events, _ := s.Query(QueryFilters{RunID: "r", Severity: "warning"})
	if len(events) != 2 {
		t.Errorf("expected 2 (warning+error), got %d", len(events))
	}

	events, _ = s.Query(QueryFilters{RunID: "r", Severity: "error"})
	if len(events) != 1 {
		t.Errorf("expected 1 (error only), got %d", len(events))
	}
}

func TestQuery_FilterBySinceUntil(t *testing.T) {
	s := newTestStore(t)
	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	t3 := time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC)

	s.Emit(Event{RunID: "r", Pipeline: "p", EventType: "a", Timestamp: t1})
	s.Emit(Event{RunID: "r", Pipeline: "p", EventType: "b", Timestamp: t2})
	s.Emit(Event{RunID: "r", Pipeline: "p", EventType: "c", Timestamp: t3})

	events, _ := s.Query(QueryFilters{RunID: "r", Since: t2})
	if len(events) != 2 {
		t.Errorf("expected 2, got %d", len(events))
	}

	events, _ = s.Query(QueryFilters{RunID: "r", Until: t2})
	if len(events) != 2 {
		t.Errorf("expected 2, got %d", len(events))
	}
}

func TestQuery_LimitOffset(t *testing.T) {
	s := newTestStore(t)
	for i := 0; i < 5; i++ {
		s.Emit(Event{RunID: "r", Pipeline: "p", EventType: "test"})
	}

	events, _ := s.Query(QueryFilters{RunID: "r", Limit: 3})
	if len(events) != 3 {
		t.Errorf("expected 3, got %d", len(events))
	}

	events, _ = s.Query(QueryFilters{RunID: "r", Limit: 3, Offset: 3})
	if len(events) != 2 {
		t.Errorf("expected 2, got %d", len(events))
	}
}

func TestGetRunSummary(t *testing.T) {
	s := newTestStore(t)
	s.Emit(Event{
		RunID: "r1", Pipeline: "pipe", EventType: "run_started",
		Timestamp: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	})
	s.Emit(Event{
		RunID: "r1", Pipeline: "pipe", EventType: "run_completed",
		Timestamp: time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC),
		Details: map[string]any{
			"status": "success", "succeeded": 5, "failed": 0, "skipped": 0,
			"total_nodes": 5, "duration_seconds": 60.0,
		},
	})

	summary, err := s.GetRunSummary("r1")
	if err != nil {
		t.Fatal(err)
	}
	if summary.Pipeline != "pipe" {
		t.Errorf("pipeline: %q", summary.Pipeline)
	}
	if summary.Succeeded != 5 {
		t.Errorf("succeeded: %d", summary.Succeeded)
	}
	if summary.Status != "success" {
		t.Errorf("status: %q", summary.Status)
	}
}

func TestGetRunSummary_NotFound(t *testing.T) {
	s := newTestStore(t)
	_, err := s.GetRunSummary("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent run")
	}
}

func TestListRuns(t *testing.T) {
	s := newTestStore(t)
	for i, rid := range []string{"r1", "r2", "r3"} {
		ts := time.Date(2026, 1, 1+i, 0, 0, 0, 0, time.UTC)
		s.Emit(Event{RunID: rid, Pipeline: "p", EventType: "run_started", Timestamp: ts})
		s.Emit(Event{RunID: rid, Pipeline: "p", EventType: "run_completed", Timestamp: ts.Add(time.Minute),
			Details: map[string]any{"status": "success", "succeeded": 1, "failed": 0, "skipped": 0, "total_nodes": 1, "duration_seconds": 60.0},
		})
	}

	runs, err := s.ListRuns(2)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 2 {
		t.Fatalf("expected 2, got %d", len(runs))
	}
	if runs[0].RunID != "r3" {
		t.Errorf("expected r3 first, got %s", runs[0].RunID)
	}
}

func TestGetFailedNodes(t *testing.T) {
	s := newTestStore(t)
	s.Emit(Event{RunID: "r1", Pipeline: "p", Asset: "a", EventType: "node_succeeded"})
	s.Emit(Event{RunID: "r1", Pipeline: "p", Asset: "b", EventType: "node_failed"})
	s.Emit(Event{RunID: "r1", Pipeline: "p", Asset: "c", EventType: "node_failed"})

	names, err := s.GetFailedNodes("r1")
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 2 {
		t.Errorf("expected 2 failed, got %d", len(names))
	}
}

func TestGetNodeResults(t *testing.T) {
	s := newTestStore(t)
	s.Emit(Event{RunID: "r1", Pipeline: "p", Asset: "a", EventType: "node_succeeded", DurationMs: 100})
	s.Emit(Event{RunID: "r1", Pipeline: "p", Asset: "b", EventType: "node_failed", DurationMs: 200,
		Details: map[string]any{"error_message": "timeout"}})
	s.Emit(Event{RunID: "r1", Pipeline: "p", Asset: "c", EventType: "node_skipped"})

	results, err := s.GetNodeResults("r1")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3, got %d", len(results))
	}
	if results[1].Error != "timeout" {
		t.Errorf("expected timeout error, got %q", results[1].Error)
	}
}

func TestDeleteBefore(t *testing.T) {
	s := newTestStore(t)
	old := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	recent := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	s.Emit(Event{RunID: "old", Pipeline: "p", EventType: "test", Timestamp: old})
	s.Emit(Event{RunID: "new", Pipeline: "p", EventType: "test", Timestamp: recent})

	// Also add model registry data to verify it's preserved
	s.RecordModelVersion("asset1", "", HashBytes([]byte("test")), "old")

	cutoff := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	n, err := s.DeleteBefore(cutoff)
	if err != nil {
		t.Fatal(err)
	}
	if n < 1 {
		t.Error("expected at least 1 deleted")
	}

	events, _ := s.Query(QueryFilters{})
	// Should have the recent event plus any model events
	hasRecent := false
	for _, e := range events {
		if e.RunID == "new" {
			hasRecent = true
		}
		if e.RunID == "old" && e.EventType == "test" {
			t.Error("old event should have been deleted")
		}
	}
	if !hasRecent {
		t.Error("recent event should be preserved")
	}

	// Model registry should be preserved
	v, _, err := s.GetModelVersion("asset1")
	if err != nil {
		t.Fatal("model registry should be preserved after DeleteBefore")
	}
	if v != 1 {
		t.Errorf("expected version 1, got %d", v)
	}
}

func TestModelVersionTracking(t *testing.T) {
	s := newTestStore(t)

	// Create temp source file
	dir := t.TempDir()
	srcFile := filepath.Join(dir, "model.sql")
	os.WriteFile(srcFile, []byte("SELECT 1"), 0644)
	hash := HashBytes([]byte("SELECT 1"))

	// First registration
	changed, version, err := s.RecordModelVersion("asset1", srcFile, hash, "run1")
	if err != nil {
		t.Fatal(err)
	}
	if !changed || version != 1 {
		t.Errorf("expected changed=true, v=1; got changed=%v, v=%d", changed, version)
	}

	// Same hash — no change
	changed, version, err = s.RecordModelVersion("asset1", srcFile, hash, "run2")
	if err != nil {
		t.Fatal(err)
	}
	if changed {
		t.Error("expected changed=false for same hash")
	}
	if version != 1 {
		t.Errorf("expected v=1, got %d", version)
	}

	// Different hash — new version
	os.WriteFile(srcFile, []byte("SELECT 2"), 0644)
	newHash := HashBytes([]byte("SELECT 2"))
	changed, version, err = s.RecordModelVersion("asset1", srcFile, newHash, "run3")
	if err != nil {
		t.Fatal(err)
	}
	if !changed || version != 2 {
		t.Errorf("expected changed=true, v=2; got changed=%v, v=%d", changed, version)
	}

	// Check history
	history, err := s.GetModelHistory("asset1")
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 2 {
		t.Fatalf("expected 2 versions, got %d", len(history))
	}
	if history[0].Version != 2 {
		t.Error("history should be newest first")
	}
	if history[1].ReplacedAt == "" {
		t.Error("v1 should have replaced_at set")
	}
}

func TestGenerateRunID(t *testing.T) {
	id := GenerateRunID()
	if len(id) < 20 {
		t.Errorf("run ID too short: %q", id)
	}
	if id[:4] != "run_" {
		t.Errorf("expected run_ prefix: %q", id)
	}
}

func TestCountLines(t *testing.T) {
	tests := []struct {
		input string
		want  int
	}{
		{"", 0},
		{"hello", 1},
		{"a\nb", 2},
		{"a\nb\nc", 3},
	}
	for _, tt := range tests {
		if got := CountLines(tt.input); got != tt.want {
			t.Errorf("CountLines(%q) = %d, want %d", tt.input, got, tt.want)
		}
	}
}
