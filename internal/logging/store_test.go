package logging

import (
	"strings"
	"sync"
	"testing"
	"time"
)

func TestWriteAndReadNodeResult(t *testing.T) {
	dir := t.TempDir()
	store := NewStore(dir)
	runID := "run_20260225_120000_abcd"

	entry := NodeEntry{
		Asset:      "test_asset",
		Status:     "success",
		StartTime:  "2026-02-25T12:00:00Z",
		EndTime:    "2026-02-25T12:00:01Z",
		DurationMs: 1000,
		ExitCode:   0,
		Stdout:     "hello\n",
		StdoutLines: 1,
	}

	if err := store.WriteNodeResult(runID, entry); err != nil {
		t.Fatal(err)
	}

	results, err := store.ReadNodeResults(runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Asset != "test_asset" || results[0].Status != "success" {
		t.Errorf("result mismatch: %+v", results[0])
	}
}

func TestWriteAndReadRunSummary(t *testing.T) {
	dir := t.TempDir()
	store := NewStore(dir)
	runID := "run_20260225_120000_abcd"

	summary := RunSummary{
		RunID:           runID,
		Pipeline:        "test_pipeline",
		StartTime:       time.Date(2026, 2, 25, 12, 0, 0, 0, time.UTC),
		EndTime:         time.Date(2026, 2, 25, 12, 0, 10, 0, time.UTC),
		DurationSeconds: 10,
		TotalNodes:      5,
		Succeeded:       4,
		Failed:          1,
		Skipped:         0,
		Status:          "completed_with_failures",
		Config:          RunConfig{MaxParallel: 10},
	}

	if err := store.WriteRunSummary(runID, summary); err != nil {
		t.Fatal(err)
	}

	got, err := store.ReadRunSummary(runID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Pipeline != "test_pipeline" || got.Succeeded != 4 || got.Failed != 1 {
		t.Errorf("summary mismatch: %+v", got)
	}
}

func TestConcurrentWrites(t *testing.T) {
	dir := t.TempDir()
	store := NewStore(dir)
	runID := "run_20260225_120000_conc"

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			entry := NodeEntry{
				Asset:  "asset_" + string(rune('A'+idx)),
				Status: "success",
			}
			if err := store.WriteNodeResult(runID, entry); err != nil {
				t.Errorf("write %d failed: %v", idx, err)
			}
		}(i)
	}
	wg.Wait()

	results, err := store.ReadNodeResults(runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 20 {
		t.Errorf("expected 20 results, got %d", len(results))
	}
}

func TestListRuns(t *testing.T) {
	dir := t.TempDir()
	store := NewStore(dir)

	ids := []string{
		"run_20260225_100000_aaaa",
		"run_20260225_110000_bbbb",
		"run_20260225_120000_cccc",
		"run_20260225_130000_dddd",
		"run_20260225_140000_eeee",
	}

	for _, id := range ids {
		summary := RunSummary{RunID: id, Pipeline: "test", Status: "success"}
		if err := store.WriteRunSummary(id, summary); err != nil {
			t.Fatal(err)
		}
	}

	runs, err := store.ListRuns()
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 5 {
		t.Fatalf("expected 5 runs, got %d", len(runs))
	}
	// Should be reverse chronological
	if runs[0].RunID != ids[4] || runs[4].RunID != ids[0] {
		t.Errorf("wrong order: first=%s last=%s", runs[0].RunID, runs[4].RunID)
	}
}

func TestRunDirectory_CreatedAutomatically(t *testing.T) {
	dir := t.TempDir()
	store := NewStore(dir)
	runID := "run_20260225_999999_auto"

	entry := NodeEntry{Asset: "x", Status: "success"}
	if err := store.WriteNodeResult(runID, entry); err != nil {
		t.Fatal(err)
	}

	// Verify we can read it back (directory was created)
	results, err := store.ReadNodeResults(runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Errorf("expected 1, got %d", len(results))
	}
}

func TestTruncField(t *testing.T) {
	// Small string stays as-is
	small := "hello"
	if got := truncField(small); got != small {
		t.Errorf("small string changed")
	}

	// Large string gets truncated
	large := strings.Repeat("A", MaxOutputBytes+1000)
	got := truncField(large)
	if len(got) != MaxOutputBytes+len(TruncMarker) {
		t.Errorf("expected %d bytes, got %d", MaxOutputBytes+len(TruncMarker), len(got))
	}
	if !strings.HasSuffix(got, TruncMarker) {
		t.Error("missing truncation marker")
	}
}
