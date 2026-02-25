package gc

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCollect_DeletesOldRuns(t *testing.T) {
	dir := t.TempDir()
	runsDir := filepath.Join(dir, ".granicus", "runs")

	// Create an old run
	oldRun := filepath.Join(runsDir, "run_20200101_000000_abcd")
	os.MkdirAll(oldRun, 0755)
	os.WriteFile(filepath.Join(oldRun, "run.json"), []byte(`{"run_id":"old"}`), 0644)
	os.Chtimes(oldRun, time.Now().AddDate(0, 0, -60), time.Now().AddDate(0, 0, -60))

	// Create a recent run
	newRun := filepath.Join(runsDir, "run_20260225_000000_efgh")
	os.MkdirAll(newRun, 0755)
	os.WriteFile(filepath.Join(newRun, "run.json"), []byte(`{"run_id":"new"}`), 0644)

	result, err := Collect(dir, 30)
	if err != nil {
		t.Fatal(err)
	}

	if result.RunsDeleted != 1 {
		t.Errorf("expected 1 deleted, got %d", result.RunsDeleted)
	}

	// Old should be gone
	if _, err := os.Stat(oldRun); !os.IsNotExist(err) {
		t.Error("old run should be deleted")
	}

	// New should remain
	if _, err := os.Stat(newRun); err != nil {
		t.Error("new run should still exist")
	}
}

func TestCollect_NoRunsDir(t *testing.T) {
	dir := t.TempDir()
	result, err := Collect(dir, 30)
	if err != nil {
		t.Fatal(err)
	}
	if result.RunsDeleted != 0 {
		t.Errorf("expected 0 deleted, got %d", result.RunsDeleted)
	}
}

func TestFormatBytes(t *testing.T) {
	if got := FormatBytes(500); got != "500 B" {
		t.Errorf("got %q", got)
	}
	if got := FormatBytes(2048); got != "2.0 KB" {
		t.Errorf("got %q", got)
	}
	if got := FormatBytes(5 * 1024 * 1024); got != "5.0 MB" {
		t.Errorf("got %q", got)
	}
}
