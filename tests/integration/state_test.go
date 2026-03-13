package integration

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Andrew-DataDecode/Granicus/internal/state"
)

func TestStateDBCreatedAtPipelineDir(t *testing.T) {
	tmpRoot := t.TempDir()

	pipelineDir := filepath.Join(tmpRoot, "project", "granicus_pipeline", "test_pipeline")
	granicusDir := filepath.Join(pipelineDir, ".granicus")
	if err := os.MkdirAll(granicusDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Engine directory (state must NOT appear here)
	engineDir := filepath.Join(tmpRoot, "granicus")
	if err := os.MkdirAll(engineDir, 0o755); err != nil {
		t.Fatal(err)
	}

	dbPath := filepath.Join(granicusDir, "state.db")
	store, err := state.New(dbPath)
	if err != nil {
		t.Fatalf("creating state store: %v", err)
	}
	defer store.Close()

	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Errorf("state.db not found at pipeline dir: %s", dbPath)
	}

	engineStatePath := filepath.Join(engineDir, "state.db")
	if _, err := os.Stat(engineStatePath); !os.IsNotExist(err) {
		t.Errorf("state.db should not exist at engine dir: %s", engineStatePath)
	}
	engineGranicusStatePath := filepath.Join(engineDir, ".granicus", "state.db")
	if _, err := os.Stat(engineGranicusStatePath); !os.IsNotExist(err) {
		t.Errorf("state.db should not exist at engine .granicus dir: %s", engineGranicusStatePath)
	}
}

func TestTestStateDBCreatedAtPipelineDir(t *testing.T) {
	tmpRoot := t.TempDir()

	pipelineDir := filepath.Join(tmpRoot, "project", "granicus_pipeline", "test_pipeline")
	granicusDir := filepath.Join(pipelineDir, ".granicus")
	if err := os.MkdirAll(granicusDir, 0o755); err != nil {
		t.Fatal(err)
	}

	engineDir := filepath.Join(tmpRoot, "granicus")
	if err := os.MkdirAll(engineDir, 0o755); err != nil {
		t.Fatal(err)
	}

	dbPath := filepath.Join(granicusDir, "test-state.db")
	store, err := state.New(dbPath)
	if err != nil {
		t.Fatalf("creating test state store: %v", err)
	}
	defer store.Close()

	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Errorf("test-state.db not found at pipeline dir: %s", dbPath)
	}

	engineTestStatePath := filepath.Join(engineDir, "test-state.db")
	if _, err := os.Stat(engineTestStatePath); !os.IsNotExist(err) {
		t.Errorf("test-state.db should not exist at engine dir: %s", engineTestStatePath)
	}
}

func TestStateMarkCompleteWritesToPipelineDir(t *testing.T) {
	tmpRoot := t.TempDir()

	pipelineDir := filepath.Join(tmpRoot, "project", "granicus_pipeline", "test_pipeline")
	granicusDir := filepath.Join(pipelineDir, ".granicus")
	if err := os.MkdirAll(granicusDir, 0o755); err != nil {
		t.Fatal(err)
	}

	dbPath := filepath.Join(granicusDir, "state.db")
	store, err := state.New(dbPath)
	if err != nil {
		t.Fatalf("creating state store: %v", err)
	}
	defer store.Close()

	if err := store.MarkInProgress("stg_orders", "2024-01-01", "2024-01-02", "run-001"); err != nil {
		t.Fatalf("marking in progress: %v", err)
	}
	if err := store.MarkComplete("stg_orders", "2024-01-01", "2024-01-02"); err != nil {
		t.Fatalf("marking complete: %v", err)
	}

	intervals, err := store.GetIntervals("stg_orders")
	if err != nil {
		t.Fatalf("getting intervals: %v", err)
	}
	if len(intervals) != 1 {
		t.Fatalf("expected 1 interval, got %d", len(intervals))
	}
	if intervals[0].Status != "complete" {
		t.Errorf("interval status = %q, want complete", intervals[0].Status)
	}
	if intervals[0].IntervalStart != "2024-01-01" {
		t.Errorf("interval start = %q, want 2024-01-01", intervals[0].IntervalStart)
	}

	info, err := os.Stat(dbPath)
	if err != nil {
		t.Fatalf("stat state.db: %v", err)
	}
	if info.Size() == 0 {
		t.Error("state.db should not be empty after writes")
	}
}

func TestStateGetIntervalsAtPipelineDir(t *testing.T) {
	tmpRoot := t.TempDir()

	pipelineDir := filepath.Join(tmpRoot, "project", "granicus_pipeline", "test_pipeline")
	granicusDir := filepath.Join(pipelineDir, ".granicus")
	if err := os.MkdirAll(granicusDir, 0o755); err != nil {
		t.Fatal(err)
	}

	dbPath := filepath.Join(granicusDir, "state.db")
	store, err := state.New(dbPath)
	if err != nil {
		t.Fatalf("creating state store: %v", err)
	}
	defer store.Close()

	intervals := []struct {
		asset string
		start string
		end   string
		runID string
	}{
		{"stg_orders", "2024-01-01", "2024-01-02", "run-001"},
		{"stg_orders", "2024-01-02", "2024-01-03", "run-002"},
		{"stg_accounts", "2024-01-01", "2024-01-02", "run-003"},
	}

	for _, iv := range intervals {
		if err := store.MarkInProgress(iv.asset, iv.start, iv.end, iv.runID); err != nil {
			t.Fatalf("marking in progress: %v", err)
		}
	}

	if err := store.MarkComplete("stg_orders", "2024-01-01", "2024-01-02"); err != nil {
		t.Fatalf("marking complete: %v", err)
	}

	got, err := store.GetIntervals("stg_orders")
	if err != nil {
		t.Fatalf("getting intervals: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 intervals for stg_orders, got %d", len(got))
	}
	if got[0].Status != "complete" {
		t.Errorf("first interval status = %q, want complete", got[0].Status)
	}
	if got[1].Status != "in_progress" {
		t.Errorf("second interval status = %q, want in_progress", got[1].Status)
	}

	got, err = store.GetIntervals("stg_accounts")
	if err != nil {
		t.Fatalf("getting intervals: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 interval for stg_accounts, got %d", len(got))
	}
}

func TestStateCreatesGranicusDirIfMissing(t *testing.T) {
	tmpRoot := t.TempDir()

	pipelineDir := filepath.Join(tmpRoot, "project", "granicus_pipeline", "test_pipeline")
	granicusDir := filepath.Join(pipelineDir, ".granicus")
	dbPath := filepath.Join(granicusDir, "state.db")

	store, err := state.New(dbPath)
	if err != nil {
		t.Fatalf("creating state store: %v", err)
	}
	defer store.Close()

	if _, err := os.Stat(granicusDir); os.IsNotExist(err) {
		t.Errorf(".granicus directory not created: %s", granicusDir)
	}
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Errorf("state.db not created: %s", dbPath)
	}
}
