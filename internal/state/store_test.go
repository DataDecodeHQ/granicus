package state

import (
	"path/filepath"
	"testing"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), ".granicus", "state.db")
	s, err := New(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestStore_MarkAndGet(t *testing.T) {
	s := newTestStore(t)

	if err := s.MarkInProgress("asset1", "2025-01-01", "2025-01-02", "run1"); err != nil {
		t.Fatal(err)
	}

	intervals, err := s.GetIntervals("asset1")
	if err != nil {
		t.Fatal(err)
	}
	if len(intervals) != 1 {
		t.Fatalf("expected 1 interval, got %d", len(intervals))
	}
	if intervals[0].Status != "in_progress" {
		t.Errorf("status: %q", intervals[0].Status)
	}
	if intervals[0].IntervalEnd != "2025-01-02" {
		t.Errorf("end: %q", intervals[0].IntervalEnd)
	}

	if err := s.MarkComplete("asset1", "2025-01-01", "2025-01-02"); err != nil {
		t.Fatal(err)
	}

	intervals, _ = s.GetIntervals("asset1")
	if intervals[0].Status != "complete" {
		t.Errorf("status after complete: %q", intervals[0].Status)
	}
	if intervals[0].CompletedAt == "" {
		t.Error("completed_at should be set")
	}
}

func TestStore_MarkFailed(t *testing.T) {
	s := newTestStore(t)

	s.MarkInProgress("a", "2025-01-01", "2025-01-02", "r1")
	s.MarkFailed("a", "2025-01-01", "2025-01-02")

	intervals, _ := s.GetIntervals("a")
	if intervals[0].Status != "failed" {
		t.Errorf("status: %q", intervals[0].Status)
	}
}

func TestStore_CrashRecovery(t *testing.T) {
	s := newTestStore(t)

	// Simulate crash: mark in_progress, never complete
	s.MarkInProgress("asset1", "2025-01-01", "2025-01-02", "run1")

	// in_progress intervals should be treated as unprocessed by callers
	intervals, _ := s.GetIntervals("asset1")
	if len(intervals) != 1 || intervals[0].Status != "in_progress" {
		t.Errorf("crash recovery: expected in_progress, got %v", intervals)
	}

	// Re-marking in_progress with new run should work (upsert)
	if err := s.MarkInProgress("asset1", "2025-01-01", "2025-01-02", "run2"); err != nil {
		t.Fatal(err)
	}
	intervals, _ = s.GetIntervals("asset1")
	if intervals[0].RunID != "run2" {
		t.Errorf("expected run2, got %q", intervals[0].RunID)
	}
}

func TestStore_InvalidateAll(t *testing.T) {
	s := newTestStore(t)

	s.MarkInProgress("asset1", "2025-01-01", "2025-01-02", "r1")
	s.MarkComplete("asset1", "2025-01-01", "2025-01-02")
	s.MarkInProgress("asset1", "2025-01-02", "2025-01-03", "r1")
	s.MarkComplete("asset1", "2025-01-02", "2025-01-03")

	// Also add another asset to verify isolation
	s.MarkInProgress("asset2", "2025-01-01", "2025-01-02", "r1")
	s.MarkComplete("asset2", "2025-01-01", "2025-01-02")

	if err := s.InvalidateAll("asset1"); err != nil {
		t.Fatal(err)
	}

	intervals1, _ := s.GetIntervals("asset1")
	if len(intervals1) != 0 {
		t.Errorf("expected 0 intervals for asset1 after invalidate, got %d", len(intervals1))
	}

	intervals2, _ := s.GetIntervals("asset2")
	if len(intervals2) != 1 {
		t.Errorf("expected 1 interval for asset2 (untouched), got %d", len(intervals2))
	}
}

func TestStore_MultipleAssets(t *testing.T) {
	s := newTestStore(t)

	s.MarkInProgress("a", "2025-01-01", "2025-01-02", "r1")
	s.MarkInProgress("b", "2025-01-01", "2025-01-02", "r1")
	s.MarkComplete("a", "2025-01-01", "2025-01-02")

	ai, _ := s.GetIntervals("a")
	bi, _ := s.GetIntervals("b")

	if ai[0].Status != "complete" {
		t.Errorf("a: %q", ai[0].Status)
	}
	if bi[0].Status != "in_progress" {
		t.Errorf("b: %q", bi[0].Status)
	}
}

func TestStore_OrderedByStart(t *testing.T) {
	s := newTestStore(t)

	// Insert out of order
	s.MarkInProgress("a", "2025-01-03", "2025-01-04", "r1")
	s.MarkInProgress("a", "2025-01-01", "2025-01-02", "r1")
	s.MarkInProgress("a", "2025-01-02", "2025-01-03", "r1")

	intervals, _ := s.GetIntervals("a")
	if len(intervals) != 3 {
		t.Fatalf("expected 3, got %d", len(intervals))
	}
	if intervals[0].IntervalStart != "2025-01-01" || intervals[1].IntervalStart != "2025-01-02" || intervals[2].IntervalStart != "2025-01-03" {
		t.Errorf("not ordered: %v, %v, %v", intervals[0].IntervalStart, intervals[1].IntervalStart, intervals[2].IntervalStart)
	}
}

func TestStore_EmptyAsset(t *testing.T) {
	s := newTestStore(t)

	intervals, err := s.GetIntervals("nonexistent")
	if err != nil {
		t.Fatal(err)
	}
	if len(intervals) != 0 {
		t.Errorf("expected 0 for nonexistent asset, got %d", len(intervals))
	}
}
