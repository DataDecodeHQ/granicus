package state

import (
	"path/filepath"
	"testing"
	"time"
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

func TestStore_RecoverOrphans_None(t *testing.T) {
	s := newTestStore(t)

	// No intervals at all
	recovered, err := s.RecoverOrphans(2 * time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(recovered) != 0 {
		t.Errorf("expected 0 recovered, got %d", len(recovered))
	}
}

func TestStore_RecoverOrphans_RecentInProgress(t *testing.T) {
	s := newTestStore(t)

	// Mark in_progress just now — should NOT be recovered (not old enough)
	s.MarkInProgress("a", "2025-01-01", "2025-01-02", "r1")

	recovered, err := s.RecoverOrphans(2 * time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(recovered) != 0 {
		t.Errorf("recent in_progress should not be recovered, got %d", len(recovered))
	}

	// Status should remain in_progress
	intervals, _ := s.GetIntervals("a")
	if intervals[0].Status != "in_progress" {
		t.Errorf("status should still be in_progress, got %q", intervals[0].Status)
	}
}

func TestStore_RecoverOrphans_OldInProgress(t *testing.T) {
	s := newTestStore(t)

	// Insert an interval with an old started_at directly to simulate a crash
	_, err := s.db.Exec(`
		INSERT INTO interval_state (asset_name, interval_start, interval_end, status, run_id, started_at, completed_at)
		VALUES ('asset1', '2025-01-01', '2025-01-02', 'in_progress', 'old-run', '2020-01-01T00:00:00Z', '')
	`)
	if err != nil {
		t.Fatal(err)
	}

	recovered, err := s.RecoverOrphans(2 * time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(recovered) != 1 {
		t.Fatalf("expected 1 recovered, got %d", len(recovered))
	}
	if recovered[0].AssetName != "asset1" {
		t.Errorf("asset: %q", recovered[0].AssetName)
	}
	if recovered[0].IntervalStart != "2025-01-01" {
		t.Errorf("interval_start: %q", recovered[0].IntervalStart)
	}
	if recovered[0].RunID != "old-run" {
		t.Errorf("run_id: %q", recovered[0].RunID)
	}

	// Status should now be pending
	intervals, _ := s.GetIntervals("asset1")
	if intervals[0].Status != "pending" {
		t.Errorf("expected pending after recovery, got %q", intervals[0].Status)
	}
}

func TestStore_RecoverOrphans_CompleteNotRecovered(t *testing.T) {
	s := newTestStore(t)

	// Insert old complete interval — should NOT be recovered
	_, err := s.db.Exec(`
		INSERT INTO interval_state (asset_name, interval_start, interval_end, status, run_id, started_at, completed_at)
		VALUES ('a', '2025-01-01', '2025-01-02', 'complete', 'r1', '2020-01-01T00:00:00Z', '2020-01-01T01:00:00Z')
	`)
	if err != nil {
		t.Fatal(err)
	}

	recovered, err := s.RecoverOrphans(2 * time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(recovered) != 0 {
		t.Errorf("complete intervals should not be recovered, got %d", len(recovered))
	}
}

func TestStore_RecoverOrphans_DefaultTimeout(t *testing.T) {
	s := newTestStore(t)

	// threshold=0 should use DefaultOrphanTimeout
	recovered, err := s.RecoverOrphans(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(recovered) != 0 {
		t.Errorf("expected 0 recovered (empty store), got %d", len(recovered))
	}
}

func TestStore_RecoverOrphans_PendingCanBeRestarted(t *testing.T) {
	s := newTestStore(t)

	// Simulate old orphan
	_, err := s.db.Exec(`
		INSERT INTO interval_state (asset_name, interval_start, interval_end, status, run_id, started_at, completed_at)
		VALUES ('a', '2025-01-01', '2025-01-02', 'in_progress', 'crashed-run', '2020-01-01T00:00:00Z', '')
	`)
	if err != nil {
		t.Fatal(err)
	}

	// Recover it
	recovered, err := s.RecoverOrphans(2 * time.Hour)
	if err != nil || len(recovered) != 1 {
		t.Fatalf("recover: err=%v recovered=%d", err, len(recovered))
	}

	// Executor can restart it — MarkInProgress should succeed and set status back to in_progress
	if err := s.MarkInProgress("a", "2025-01-01", "2025-01-02", "new-run"); err != nil {
		t.Fatal(err)
	}
	intervals, _ := s.GetIntervals("a")
	if intervals[0].Status != "in_progress" {
		t.Errorf("expected in_progress after restart, got %q", intervals[0].Status)
	}
	if intervals[0].RunID != "new-run" {
		t.Errorf("expected new-run, got %q", intervals[0].RunID)
	}
}
