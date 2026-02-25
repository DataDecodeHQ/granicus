package scheduler

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func newTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	db.Exec("PRAGMA journal_mode=WAL")
	t.Cleanup(func() { db.Close() })
	return db
}

func TestLock_AcquireAndRelease(t *testing.T) {
	db := newTestDB(t)
	store, err := NewLockStore(db)
	if err != nil {
		t.Fatal(err)
	}

	acquired, err := store.AcquireLock("pipeline1", "run1")
	if err != nil {
		t.Fatal(err)
	}
	if !acquired {
		t.Error("should acquire lock")
	}

	// Second acquire should fail
	acquired2, err := store.AcquireLock("pipeline1", "run2")
	if err != nil {
		t.Fatal(err)
	}
	if acquired2 {
		t.Error("should not acquire lock while running")
	}

	// Release
	if err := store.ReleaseLock("pipeline1", "run1"); err != nil {
		t.Fatal(err)
	}

	// Now should be acquirable
	acquired3, err := store.AcquireLock("pipeline1", "run3")
	if err != nil {
		t.Fatal(err)
	}
	if !acquired3 {
		t.Error("should acquire after release")
	}
}

func TestLock_IsLocked(t *testing.T) {
	db := newTestDB(t)
	store, _ := NewLockStore(db)

	locked, _, _ := store.IsLocked("pipeline1")
	if locked {
		t.Error("should not be locked initially")
	}

	store.AcquireLock("pipeline1", "run1")

	locked, runID, _ := store.IsLocked("pipeline1")
	if !locked {
		t.Error("should be locked")
	}
	if runID != "run1" {
		t.Errorf("run_id: %q", runID)
	}
}

func TestLock_IndependentPipelines(t *testing.T) {
	db := newTestDB(t)
	store, _ := NewLockStore(db)

	store.AcquireLock("p1", "r1")
	acquired, _ := store.AcquireLock("p2", "r2")
	if !acquired {
		t.Error("different pipelines should lock independently")
	}
}

func TestLock_RecoverStaleLocks(t *testing.T) {
	db := newTestDB(t)
	store, _ := NewLockStore(db)

	// Insert a stale lock manually (7 hours ago)
	staleTime := time.Now().UTC().Add(-7 * time.Hour).Format(time.RFC3339)
	db.Exec(`INSERT INTO pipeline_locks (pipeline_name, run_id, started_at, status) VALUES (?, ?, ?, 'running')`,
		"stale_pipeline", "old_run", staleTime)

	// Insert a fresh lock
	store.AcquireLock("fresh_pipeline", "new_run")

	// Recover locks older than 6 hours
	recovered, err := store.RecoverStaleLocks(6 * time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if recovered != 1 {
		t.Errorf("expected 1 recovered, got %d", recovered)
	}

	// Stale pipeline should now be acquirable
	acquired, _ := store.AcquireLock("stale_pipeline", "retry_run")
	if !acquired {
		t.Error("stale pipeline should be acquirable after recovery")
	}

	// Fresh pipeline should still be locked
	acquired2, _ := store.AcquireLock("fresh_pipeline", "another_run")
	if acquired2 {
		t.Error("fresh pipeline should still be locked")
	}
}

func TestLock_RecoverNoStaleLocks(t *testing.T) {
	db := newTestDB(t)
	store, _ := NewLockStore(db)

	recovered, _ := store.RecoverStaleLocks(6 * time.Hour)
	if recovered != 0 {
		t.Errorf("expected 0, got %d", recovered)
	}
}
