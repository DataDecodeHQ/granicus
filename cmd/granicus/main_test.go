package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/DataDecodeHQ/granicus/internal/events"
)

// makeModelsCmd returns a fresh cobra.Command wired to runModels so each test
// gets its own flag state.
func makeModelsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:  "models [asset_name]",
		Args: cobra.MaximumNArgs(1),
		RunE: runModels,
	}
	cmd.Flags().String("project-root", ".", "Project root directory")
	cmd.Flags().String("diff", "", "Show diff between two versions (e.g., 1,2)")
	cmd.Flags().String("output", "", "Output format (json)")
	return cmd
}

// captureStdout runs fn and returns everything it prints to os.Stdout.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stdout = w

	fn()

	w.Close()
	os.Stdout = old
	var buf bytes.Buffer
	io.Copy(&buf, r)
	return buf.String()
}

// seedStore opens (or creates) events.db under projectRoot and calls fn with it.
func seedStore(t *testing.T, projectRoot string, fn func(*events.Store)) {
	t.Helper()
	dir := filepath.Join(projectRoot, ".granicus")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	s, err := events.New(filepath.Join(dir, "events.db"))
	if err != nil {
		t.Fatalf("events.New: %v", err)
	}
	fn(s)
	s.Close()
}

// TestRunModels_ListEmpty verifies that listing models with an empty store
// prints "No models registered." and exits cleanly.
func TestRunModels_ListEmpty(t *testing.T) {
	dir := t.TempDir()
	seedStore(t, dir, func(_ *events.Store) {})

	cmd := makeModelsCmd()
	cmd.SetArgs([]string{"--project-root", dir})

	out := captureStdout(t, func() {
		if err := cmd.Execute(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if !strings.Contains(out, "No models registered.") {
		t.Errorf("expected 'No models registered.', got: %q", out)
	}
}

// TestRunModels_ListPopulated verifies that listing with registered models
// prints each asset name.
func TestRunModels_ListPopulated(t *testing.T) {
	dir := t.TempDir()
	seedStore(t, dir, func(s *events.Store) {
		hash1 := events.HashBytes([]byte("SELECT 1"))
		hash2 := events.HashBytes([]byte("SELECT 2"))
		if _, _, err := s.RecordModelVersion("asset_alpha", "", hash1, "run_001"); err != nil {
			t.Fatalf("RecordModelVersion: %v", err)
		}
		if _, _, err := s.RecordModelVersion("asset_beta", "", hash2, "run_002"); err != nil {
			t.Fatalf("RecordModelVersion: %v", err)
		}
	})

	cmd := makeModelsCmd()
	cmd.SetArgs([]string{"--project-root", dir})

	out := captureStdout(t, func() {
		if err := cmd.Execute(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if !strings.Contains(out, "asset_alpha") {
		t.Errorf("expected 'asset_alpha' in output, got: %q", out)
	}
	if !strings.Contains(out, "asset_beta") {
		t.Errorf("expected 'asset_beta' in output, got: %q", out)
	}
}

// TestRunModels_ListPopulated_JSON verifies the JSON output shape when models exist.
func TestRunModels_ListPopulated_JSON(t *testing.T) {
	dir := t.TempDir()
	seedStore(t, dir, func(s *events.Store) {
		hash := events.HashBytes([]byte("SELECT 1"))
		if _, _, err := s.RecordModelVersion("my_model", "", hash, "run_001"); err != nil {
			t.Fatalf("RecordModelVersion: %v", err)
		}
	})

	cmd := makeModelsCmd()
	cmd.SetArgs([]string{"--project-root", dir, "--output", "json"})

	out := captureStdout(t, func() {
		if err := cmd.Execute(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	var result jsonModelsListOutput
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatalf("invalid JSON: %v\noutput: %q", err, out)
	}
	if len(result.Models) != 1 {
		t.Fatalf("expected 1 model, got %d", len(result.Models))
	}
	if result.Models[0].AssetName != "my_model" {
		t.Errorf("expected asset_name 'my_model', got %q", result.Models[0].AssetName)
	}
}

// TestRunModels_DiffVersions verifies that --diff N,M prints a unified-style diff header
// when both versions exist.
func TestRunModels_DiffVersions(t *testing.T) {
	dir := t.TempDir()

	// Write two distinct SQL source files so snapshots differ.
	srcFile := filepath.Join(dir, "model.sql")
	os.WriteFile(srcFile, []byte("SELECT 1\nFROM a"), 0644)
	hash1 := events.HashBytes([]byte("SELECT 1\nFROM a"))

	seedStore(t, dir, func(s *events.Store) {
		if _, _, err := s.RecordModelVersion("my_asset", srcFile, hash1, "run_001"); err != nil {
			t.Fatalf("v1: %v", err)
		}
		// Change the file, record v2
		os.WriteFile(srcFile, []byte("SELECT 2\nFROM b"), 0644)
		hash2 := events.HashBytes([]byte("SELECT 2\nFROM b"))
		if _, _, err := s.RecordModelVersion("my_asset", srcFile, hash2, "run_002"); err != nil {
			t.Fatalf("v2: %v", err)
		}
	})

	cmd := makeModelsCmd()
	cmd.SetArgs([]string{"--project-root", dir, "--diff", "1,2", "my_asset"})

	out := captureStdout(t, func() {
		if err := cmd.Execute(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	// Expect a diff header line for the asset.
	if !strings.Contains(out, "--- my_asset v1") {
		t.Errorf("expected '--- my_asset v1' in diff output, got: %q", out)
	}
	if !strings.Contains(out, "+++ my_asset v2") {
		t.Errorf("expected '+++ my_asset v2' in diff output, got: %q", out)
	}
	// Lines that changed should appear prefixed with - / +
	if !strings.Contains(out, "-SELECT 1") {
		t.Errorf("expected '-SELECT 1' in diff output, got: %q", out)
	}
	if !strings.Contains(out, "+SELECT 2") {
		t.Errorf("expected '+SELECT 2' in diff output, got: %q", out)
	}
}

// TestRunModels_DiffVersions_NotFound verifies that asking for a version that
// does not exist returns an error.
func TestRunModels_DiffVersions_NotFound(t *testing.T) {
	dir := t.TempDir()
	seedStore(t, dir, func(s *events.Store) {
		hash := events.HashBytes([]byte("SELECT 1"))
		s.RecordModelVersion("my_asset", "", hash, "run_001")
	})

	cmd := makeModelsCmd()
	cmd.SetArgs([]string{"--project-root", dir, "--diff", "1,99", "my_asset"})

	var cmdErr error
	captureStdout(t, func() {
		cmdErr = cmd.Execute()
	})

	if cmdErr == nil {
		t.Fatal("expected error for missing version, got nil")
	}
	if !strings.Contains(cmdErr.Error(), "version not found") {
		t.Errorf("expected 'version not found' error, got: %v", cmdErr)
	}
}

// TestRunModels_ShowHistory_Text verifies the text output for show history
// (asset_name as arg, no --diff, no --output json).
func TestRunModels_ShowHistory_Text(t *testing.T) {
	dir := t.TempDir()
	seedStore(t, dir, func(s *events.Store) {
		hash1 := events.HashBytes([]byte("SELECT 1"))
		s.RecordModelVersion("hist_asset", "", hash1, "run_001")
		hash2 := events.HashBytes([]byte("SELECT 2"))
		s.RecordModelVersion("hist_asset", "", hash2, "run_002")
	})

	cmd := makeModelsCmd()
	cmd.SetArgs([]string{"--project-root", dir, "hist_asset"})

	out := captureStdout(t, func() {
		if err := cmd.Execute(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if !strings.Contains(out, "hist_asset") {
		t.Errorf("expected asset name in output, got: %q", out)
	}
	// Two versions should appear (v1 and v2)
	if !strings.Contains(out, fmt.Sprintf("v%-7d", 1)) && !strings.Contains(out, "v1") {
		t.Errorf("expected v1 in history output, got: %q", out)
	}
	if !strings.Contains(out, fmt.Sprintf("v%-7d", 2)) && !strings.Contains(out, "v2") {
		t.Errorf("expected v2 in history output, got: %q", out)
	}
}

// TestRunModels_ShowHistory_JSON verifies the JSON output for show history.
func TestRunModels_ShowHistory_JSON(t *testing.T) {
	dir := t.TempDir()
	seedStore(t, dir, func(s *events.Store) {
		hash1 := events.HashBytes([]byte("SELECT 1"))
		s.RecordModelVersion("json_asset", "", hash1, "run_001")
		hash2 := events.HashBytes([]byte("SELECT 2"))
		s.RecordModelVersion("json_asset", "", hash2, "run_002")
	})

	cmd := makeModelsCmd()
	cmd.SetArgs([]string{"--project-root", dir, "--output", "json", "json_asset"})

	out := captureStdout(t, func() {
		if err := cmd.Execute(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	var result jsonModelsHistoryOutput
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatalf("invalid JSON: %v\noutput: %q", err, out)
	}
	if result.Asset != "json_asset" {
		t.Errorf("expected asset 'json_asset', got %q", result.Asset)
	}
	if len(result.History) != 2 {
		t.Fatalf("expected 2 history entries, got %d", len(result.History))
	}
	// History is newest first.
	if result.History[0].Version != 2 {
		t.Errorf("expected newest version first (v2), got v%d", result.History[0].Version)
	}
}

// TestRunModels_ShowHistory_NotFound verifies that requesting history for an
// unknown asset returns an error (text mode) or a NOT_FOUND JSON error (json mode).
func TestRunModels_ShowHistory_NotFound(t *testing.T) {
	dir := t.TempDir()
	seedStore(t, dir, func(_ *events.Store) {})

	// Text mode — should return an error.
	cmd := makeModelsCmd()
	cmd.SetArgs([]string{"--project-root", dir, "nonexistent_asset"})

	var cmdErr error
	captureStdout(t, func() {
		cmdErr = cmd.Execute()
	})
	if cmdErr == nil {
		t.Fatal("expected error for unknown asset in text mode, got nil")
	}
	if !strings.Contains(cmdErr.Error(), "no history for nonexistent_asset") {
		t.Errorf("unexpected error: %v", cmdErr)
	}

	// JSON mode — should print NOT_FOUND and return nil.
	cmd2 := makeModelsCmd()
	cmd2.SetArgs([]string{"--project-root", dir, "--output", "json", "nonexistent_asset"})

	var out2 string
	var err2 error
	out2 = captureStdout(t, func() {
		err2 = cmd2.Execute()
	})
	if err2 != nil {
		t.Fatalf("expected nil error in JSON not-found mode, got: %v", err2)
	}
	if !strings.Contains(out2, "NOT_FOUND") {
		t.Errorf("expected NOT_FOUND in JSON error output, got: %q", out2)
	}
}

// TestRunModels_NoEventsDB verifies behavior when events.db does not exist at all.
func TestRunModels_NoEventsDB(t *testing.T) {
	dir := t.TempDir() // No .granicus/events.db created

	// Text mode — prints message, no error.
	cmd := makeModelsCmd()
	cmd.SetArgs([]string{"--project-root", dir})

	out := captureStdout(t, func() {
		if err := cmd.Execute(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
	if !strings.Contains(out, "No models found") {
		t.Errorf("expected 'No models found' message, got: %q", out)
	}

	// JSON mode — prints NO_EVENTS_DB error JSON, no error return.
	cmd2 := makeModelsCmd()
	cmd2.SetArgs([]string{"--project-root", dir, "--output", "json"})

	out2 := captureStdout(t, func() {
		if err := cmd2.Execute(); err != nil {
			t.Fatalf("unexpected error in JSON no-db mode: %v", err)
		}
	})
	if !strings.Contains(out2, "NO_EVENTS_DB") {
		t.Errorf("expected NO_EVENTS_DB in JSON output, got: %q", out2)
	}
}
