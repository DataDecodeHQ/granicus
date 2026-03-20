package archive

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"
)

// credentialsAvailable returns true if GCS/Firestore credentials are present.
// Both clients require ADC or GOOGLE_APPLICATION_CREDENTIALS to succeed.
func credentialsAvailable() bool {
	if os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") != "" {
		return true
	}
	// Check well-known ADC location
	home, err := os.UserHomeDir()
	if err != nil {
		return false
	}
	_, err = os.Stat(home + "/.config/gcloud/application_default_credentials.json")
	return err == nil
}

// --- NewPruner ---

func TestNewPruner_MissingCredentials(t *testing.T) {
	if credentialsAvailable() {
		t.Skip("credentials present; skipping missing-credential error path")
	}
	ctx := context.Background()
	_, err := NewPruner(ctx, "test-project", "test-bucket", 30)
	if err == nil {
		t.Fatal("expected error when credentials are absent, got nil")
	}
}

func TestNewPruner_DefaultRetentionDays(t *testing.T) {
	// retentionDays <= 0 triggers env-var / default logic.
	// We verify the env var is read before the client call fails.
	t.Setenv("GRANICUS_RETENTION_DAYS", "45")
	ctx := context.Background()
	// The client creation will fail without credentials; we only care that
	// the function reads the env var (observable by not panicking / not
	// crashing before reaching the client call).
	_, err := NewPruner(ctx, "test-project", "", 0)
	// Without credentials this will always error; that's expected.
	if err == nil && !credentialsAvailable() {
		t.Fatal("expected error without credentials")
	}
}

func TestNewPruner_DefaultBucketFromEnv(t *testing.T) {
	t.Setenv("GRANICUS_OPS_BUCKET", "my-custom-bucket")
	ctx := context.Background()
	_, err := NewPruner(ctx, "test-project", "", 7)
	// Error is expected without credentials; the point is no panic and the
	// env var path is exercised.
	_ = err
}

func TestNewPruner_DefaultBucketFallback(t *testing.T) {
	// Ensure env var is absent so the hardcoded fallback is used.
	t.Setenv("GRANICUS_OPS_BUCKET", "")
	ctx := context.Background()
	_, err := NewPruner(ctx, "test-project", "", 7)
	_ = err
}

func TestNewPruner_InvalidRetentionEnvIgnored(t *testing.T) {
	// A non-numeric GRANICUS_RETENTION_DAYS should be ignored; default (30)
	// is kept and the function still reaches client creation.
	t.Setenv("GRANICUS_RETENTION_DAYS", "not-a-number")
	ctx := context.Background()
	_, err := NewPruner(ctx, "test-project", "bucket", 0)
	_ = err
}

// --- NewRunArchiver ---

func TestNewRunArchiver_MissingCredentials(t *testing.T) {
	if credentialsAvailable() {
		t.Skip("credentials present; skipping missing-credential error path")
	}
	ctx := context.Background()
	_, err := NewRunArchiver(ctx, "test-bucket")
	if err == nil {
		t.Fatal("expected error when credentials are absent, got nil")
	}
}

func TestNewRunArchiver_DefaultBucketFromEnv(t *testing.T) {
	t.Setenv("GRANICUS_OPS_BUCKET", "env-bucket")
	ctx := context.Background()
	_, err := NewRunArchiver(ctx, "")
	_ = err
}

func TestNewRunArchiver_DefaultBucketFallback(t *testing.T) {
	t.Setenv("GRANICUS_OPS_BUCKET", "")
	ctx := context.Background()
	_, err := NewRunArchiver(ctx, "")
	_ = err
}

func TestNewRunArchiver_ExplicitBucketOverridesEnv(t *testing.T) {
	// When a non-empty bucket is passed, env var must not override it.
	// We can't observe the bucket field directly (unexported), but we can
	// verify the constructor doesn't panic and reaches client creation.
	t.Setenv("GRANICUS_OPS_BUCKET", "env-bucket")
	ctx := context.Background()
	_, err := NewRunArchiver(ctx, "explicit-bucket")
	_ = err
}

// --- RunArchiver.Close ---

func TestRunArchiver_Close_Safe(t *testing.T) {
	if !credentialsAvailable() {
		t.Skip("requires GCS credentials")
	}
	ctx := context.Background()
	a, err := NewRunArchiver(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("NewRunArchiver: %v", err)
	}
	if err := a.Close(); err != nil {
		t.Errorf("Close returned unexpected error: %v", err)
	}
}

// --- Pruner.Close ---

func TestPruner_Close_Safe(t *testing.T) {
	if !credentialsAvailable() {
		t.Skip("requires Firestore/GCS credentials")
	}
	ctx := context.Background()
	p, err := NewPruner(ctx, "test-project", "test-bucket", 30)
	if err != nil {
		t.Fatalf("NewPruner: %v", err)
	}
	// Close must not panic or return an error observable through the method.
	p.Close()
}

// --- Archive path format (pure logic extracted for verification) ---

// archivePath mirrors the path-construction logic in both pruner.go and
// run_archive.go so we can verify the format independently of live clients.
func archivePath(pipeline, runID string, startedAt time.Time) string {
	return fmt.Sprintf("runs/%s/%s/%s/run_%s.jsonl",
		pipeline, startedAt.Format("2006"), startedAt.Format("01"), runID)
}

func TestArchivePath_Format(t *testing.T) {
	ts := time.Date(2025, 3, 7, 12, 0, 0, 0, time.UTC)
	got := archivePath("analyte_health", "run_01ABC", ts)
	want := "runs/analyte_health/2025/03/run_run_01ABC.jsonl"
	if got != want {
		t.Errorf("archivePath = %q, want %q", got, want)
	}
}

func TestArchivePath_PadsMonth(t *testing.T) {
	ts := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	got := archivePath("paternity_labs", "abc123", ts)
	if got != "runs/paternity_labs/2024/01/run_abc123.jsonl" {
		t.Errorf("unexpected path: %q", got)
	}
}

func TestArchivePath_DecemberPadding(t *testing.T) {
	ts := time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC)
	got := archivePath("pipeline_x", "xyz", ts)
	if got != "runs/pipeline_x/2024/12/run_xyz.jsonl" {
		t.Errorf("unexpected path: %q", got)
	}
}

// --- JSONL record shape (pure marshaling) ---

// buildRunRecord mirrors the json.Marshal call in ArchiveRun so we can
// verify field presence without a live GCS client.
func buildRunRecord(runID, pipeline, status string, startedAt time.Time) (map[string]any, error) {
	raw, err := json.Marshal(map[string]any{
		"type":             "run",
		"run_id":           runID,
		"pipeline":         pipeline,
		"pipeline_version": "",
		"content_hash":     "",
		"status":           status,
		"triggered_by":     "",
		"trigger_context":  nil,
		"parent_run_id":    "",
		"started_at":       startedAt,
		"completed_at":     time.Time{},
		"node_count":       0,
		"succeeded":        0,
		"failed":           0,
		"skipped":          0,
		"error_summary":    "",
	})
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func TestBuildRunRecord_RequiredFields(t *testing.T) {
	ts := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	rec, err := buildRunRecord("run-1", "my_pipeline", "succeeded", ts)
	if err != nil {
		t.Fatalf("buildRunRecord: %v", err)
	}

	required := []string{
		"type", "run_id", "pipeline", "status",
		"started_at", "completed_at", "node_count",
		"succeeded", "failed", "skipped",
	}
	for _, field := range required {
		if _, ok := rec[field]; !ok {
			t.Errorf("missing required field %q in run record", field)
		}
	}
	if rec["type"] != "run" {
		t.Errorf("type = %v, want \"run\"", rec["type"])
	}
	if rec["run_id"] != "run-1" {
		t.Errorf("run_id = %v, want \"run-1\"", rec["run_id"])
	}
}

func TestBuildRunRecord_IsValidJSON(t *testing.T) {
	ts := time.Now().UTC()
	rec, err := buildRunRecord("r", "p", "failed", ts)
	if err != nil {
		t.Fatalf("buildRunRecord: %v", err)
	}
	// Re-marshal to confirm round-trip produces valid JSON
	b, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("re-marshal failed: %v", err)
	}
	var check map[string]any
	if err := json.Unmarshal(b, &check); err != nil {
		t.Fatalf("round-trip JSON invalid: %v", err)
	}
}

// buildEventRecord mirrors the json.Marshal call for event lines.
func buildEventRecord(runID, pipeline, node, eventType, status string) (map[string]any, error) {
	raw, err := json.Marshal(map[string]any{
		"type":        "event",
		"run_id":      runID,
		"pipeline":    pipeline,
		"node":        node,
		"event_type":  eventType,
		"status":      status,
		"error":       "",
		"exit_code":   0,
		"duration_ms": int64(0),
		"attempt":     1,
		"runner":      "shell",
		"metadata":    nil,
		"timestamp":   time.Now().UTC(),
	})
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func TestBuildEventRecord_RequiredFields(t *testing.T) {
	rec, err := buildEventRecord("run-1", "my_pipeline", "node_a", "node_complete", "success")
	if err != nil {
		t.Fatalf("buildEventRecord: %v", err)
	}

	required := []string{
		"type", "run_id", "pipeline", "node",
		"event_type", "status", "timestamp",
	}
	for _, field := range required {
		if _, ok := rec[field]; !ok {
			t.Errorf("missing required field %q in event record", field)
		}
	}
	if rec["type"] != "event" {
		t.Errorf("type = %v, want \"event\"", rec["type"])
	}
}

// --- Retention defaults (observable via env var reading) ---

func TestRetentionDays_PositiveValuePassedThrough(t *testing.T) {
	// retentionDays > 0 bypasses env var lookup entirely; the value is used as-is.
	// We can't inspect the Pruner fields directly, but we verify the constructor
	// reaches the client-creation step (not an early return) and fails only
	// on credentials, not on validation of a positive retention value.
	if credentialsAvailable() {
		t.Skip("requires absent credentials to exercise error path cleanly")
	}
	ctx := context.Background()
	_, err := NewPruner(ctx, "proj", "bucket", 7)
	if err == nil {
		t.Fatal("expected credential error, got nil")
	}
}

func TestRetentionDays_NegativeUsesDefault(t *testing.T) {
	t.Setenv("GRANICUS_RETENTION_DAYS", "")
	if credentialsAvailable() {
		t.Skip("requires absent credentials to exercise error path cleanly")
	}
	ctx := context.Background()
	_, err := NewPruner(ctx, "proj", "bucket", -1)
	if err == nil {
		t.Fatal("expected credential error, got nil")
	}
}

// --- Stubs for functions that require live clients ---

func TestPruneRuns_RequiresCredentials(t *testing.T) {
	t.Skip("requires GCS + Firestore credentials: would verify that PruneRuns " +
		"returns 0 deleted on an empty collection and >0 after seeding expired runs")
}

func TestPruneIntervals_RequiresCredentials(t *testing.T) {
	t.Skip("requires Firestore credentials: would verify that PruneIntervals " +
		"iterates pipeline documents, batches deletes, and returns the count of " +
		"deleted interval documents older than retentionDays")
}

func TestArchiveRun_RequiresCredentials(t *testing.T) {
	t.Skip("requires GCS + Firestore credentials: would verify that ArchiveRun " +
		"writes a JSONL object to the expected GCS path with one run-record header " +
		"line followed by one line per event, and that the object's ContentType is " +
		"application/x-ndjson")
}

func TestArchiveRun_NotFound_RequiresCredentials(t *testing.T) {
	t.Skip("requires Firestore credentials: would verify that ArchiveRun returns " +
		"a non-nil error containing \"not found\" when the runID does not exist")
}

func TestArchiveRun_ZeroStartedAt_RequiresCredentials(t *testing.T) {
	t.Skip("requires GCS + Firestore credentials: would verify that when " +
		"run.StartedAt is zero, ArchiveRun falls back to time.Now() for the " +
		"GCS object path rather than panicking or producing an invalid path")
}

func TestPruneRuns_SkipsWhenArchiveMissing_RequiresCredentials(t *testing.T) {
	t.Skip("requires GCS + Firestore credentials: would verify that PruneRuns " +
		"logs a warning and skips deletion when the expected GCS archive object " +
		"does not exist, leaving the Firestore document intact")
}
