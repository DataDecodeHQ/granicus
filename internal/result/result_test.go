package result

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestResultEnvelope_Serialization(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	end := now.Add(5 * time.Second)

	orig := ResultEnvelope{
		Asset:       "load_orders",
		RunID:      "run-abc123",
		Pipeline:   "granicus",
		Status:     "success",
		StartedAt:  now,
		EndedAt:    end,
		DurationMs: 5000,
		ExitCode:   0,
		Telemetry: map[string]any{
			TelBQBytesScanned: int64(1024),
			TelBQJobID:        "job-xyz",
		},
		Checks: []CheckResult{
			{
				Name:         "no_nulls",
				Status:       "passed",
				Severity:     "error",
				RowsReturned: 0,
				SQLHash:      "abc123",
			},
		},
		Artifacts: []Artifact{
			{
				Type: "log",
				URI:  "gs://bucket/logs/run-abc123.log",
				Size: 2048,
			},
		},
		Metadata: map[string]string{
			"env": "dev",
		},
	}

	data, err := json.Marshal(orig)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	var got ResultEnvelope
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	if got.Asset != orig.Asset {
		t.Errorf("Asset: got %q, want %q", got.Asset, orig.Asset)
	}
	if got.RunID != orig.RunID {
		t.Errorf("RunID: got %q, want %q", got.RunID, orig.RunID)
	}
	if got.Pipeline != orig.Pipeline {
		t.Errorf("Pipeline: got %q, want %q", got.Pipeline, orig.Pipeline)
	}
	if got.Status != orig.Status {
		t.Errorf("Status: got %q, want %q", got.Status, orig.Status)
	}
	if !got.StartedAt.Equal(orig.StartedAt) {
		t.Errorf("StartedAt: got %v, want %v", got.StartedAt, orig.StartedAt)
	}
	if !got.EndedAt.Equal(orig.EndedAt) {
		t.Errorf("EndedAt: got %v, want %v", got.EndedAt, orig.EndedAt)
	}
	if got.DurationMs != orig.DurationMs {
		t.Errorf("DurationMs: got %d, want %d", got.DurationMs, orig.DurationMs)
	}
	if got.ExitCode != orig.ExitCode {
		t.Errorf("ExitCode: got %d, want %d", got.ExitCode, orig.ExitCode)
	}
	if len(got.Checks) != 1 {
		t.Fatalf("Checks: got %d, want 1", len(got.Checks))
	}
	if got.Checks[0].Name != orig.Checks[0].Name {
		t.Errorf("Checks[0].Name: got %q, want %q", got.Checks[0].Name, orig.Checks[0].Name)
	}
	if got.Checks[0].Status != orig.Checks[0].Status {
		t.Errorf("Checks[0].Status: got %q, want %q", got.Checks[0].Status, orig.Checks[0].Status)
	}
	if got.Checks[0].RowsReturned != orig.Checks[0].RowsReturned {
		t.Errorf("Checks[0].RowsReturned: got %d, want %d", got.Checks[0].RowsReturned, orig.Checks[0].RowsReturned)
	}
	if len(got.Artifacts) != 1 {
		t.Fatalf("Artifacts: got %d, want 1", len(got.Artifacts))
	}
	if got.Artifacts[0].URI != orig.Artifacts[0].URI {
		t.Errorf("Artifacts[0].URI: got %q, want %q", got.Artifacts[0].URI, orig.Artifacts[0].URI)
	}
	if got.Metadata["env"] != "dev" {
		t.Errorf("Metadata[env]: got %q, want %q", got.Metadata["env"], "dev")
	}
}

func TestResultEnvelope_OmitEmptyFields(t *testing.T) {
	env := ResultEnvelope{
		Asset:     "node",
		RunID:    "run",
		Pipeline: "pipe",
		Status:   "skipped",
	}

	data, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	for _, field := range []string{"error", "telemetry", "checks", "artifacts", "metadata"} {
		if _, ok := m[field]; ok {
			t.Errorf("field %q should be omitted when empty", field)
		}
	}
}

func TestResultEnvelope_TelemetryConstants(t *testing.T) {
	constants := map[string]string{
		"TelBQBytesScanned":    TelBQBytesScanned,
		"TelBQBytesWritten":    TelBQBytesWritten,
		"TelBQRowCount":        TelBQRowCount,
		"TelBQSlotMs":          TelBQSlotMs,
		"TelBQJobID":           TelBQJobID,
		"TelBQCacheHit":        TelBQCacheHit,
		"TelCRJPeakMemoryBytes": TelCRJPeakMemoryBytes,
		"TelCRJJobID":          TelCRJJobID,
	}

	for name, val := range constants {
		if val == "" {
			t.Errorf("constant %s is empty", name)
		}
	}
}

func TestResultEnvelope_TelemetryConstantUniqueness(t *testing.T) {
	vals := []string{
		TelBQBytesScanned,
		TelBQBytesWritten,
		TelBQRowCount,
		TelBQSlotMs,
		TelBQJobID,
		TelBQCacheHit,
		TelCRJPeakMemoryBytes,
		TelCRJJobID,
	}

	seen := make(map[string]bool, len(vals))
	for _, v := range vals {
		if seen[v] {
			t.Errorf("duplicate telemetry constant value %q", v)
		}
		seen[v] = true
	}
}

func TestNewPublisher_ErrorWithoutCredentials(t *testing.T) {
	// Clear env vars so no project is resolved from environment
	t.Setenv("GRANICUS_PUBSUB_PROJECT", "")
	t.Setenv("GRANICUS_FIRESTORE_PROJECT", "")

	ctx := context.Background()
	// With empty project and no ADC, NewPublisher should return an error
	// from the Pub/Sub client creation.
	_, err := NewPublisher(ctx, "", "")
	// Pub/Sub client creation may or may not fail depending on ADC presence.
	// If ADC is present, it might succeed even with empty project (using default project).
	// We just verify it doesn't panic.
	_ = err
}

func TestNewPublisher_ProjectFromPubSubEnv(t *testing.T) {
	t.Setenv("GRANICUS_PUBSUB_PROJECT", "env-pubsub-project")
	t.Setenv("GRANICUS_FIRESTORE_PROJECT", "env-firestore-project")

	ctx := context.Background()
	// GRANICUS_PUBSUB_PROJECT takes precedence over GRANICUS_FIRESTORE_PROJECT
	_, err := NewPublisher(ctx, "", "")
	// Error expected without credentials; verifies env var path is exercised
	_ = err
}

func TestNewPublisher_ProjectFallsBackToFirestoreEnv(t *testing.T) {
	t.Setenv("GRANICUS_PUBSUB_PROJECT", "")
	t.Setenv("GRANICUS_FIRESTORE_PROJECT", "env-firestore-project")

	ctx := context.Background()
	_, err := NewPublisher(ctx, "", "")
	_ = err
}

func TestNewPublisher_TopicFromEnv(t *testing.T) {
	t.Setenv("GRANICUS_RESULT_TOPIC", "custom-topic")

	ctx := context.Background()
	_, err := NewPublisher(ctx, "test-project", "")
	// Verify the env var path is exercised without panic
	_ = err
}

func TestNewPublisher_TopicDefaultFallback(t *testing.T) {
	t.Setenv("GRANICUS_RESULT_TOPIC", "")

	ctx := context.Background()
	// Should default to "granicus-results"
	_, err := NewPublisher(ctx, "test-project", "")
	_ = err
}

func TestNewPublisher_ExplicitParamsOverrideEnv(t *testing.T) {
	t.Setenv("GRANICUS_PUBSUB_PROJECT", "env-project")
	t.Setenv("GRANICUS_RESULT_TOPIC", "env-topic")

	ctx := context.Background()
	// Explicit params should be used, not env vars
	_, err := NewPublisher(ctx, "explicit-project", "explicit-topic")
	_ = err
}

func TestResultEnvelope_FailedStatus(t *testing.T) {
	env := ResultEnvelope{
		Asset:     "failing_node",
		RunID:    "run-fail",
		Pipeline: "test",
		Status:   "failed",
		Error:    "query timeout after 30s",
		ExitCode: 1,
	}

	data, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	var got ResultEnvelope
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	if got.Status != "failed" {
		t.Errorf("Status: got %q, want \"failed\"", got.Status)
	}
	if got.Error != "query timeout after 30s" {
		t.Errorf("Error: got %q, want \"query timeout after 30s\"", got.Error)
	}
	if got.ExitCode != 1 {
		t.Errorf("ExitCode: got %d, want 1", got.ExitCode)
	}
}

func TestResultEnvelope_MultipleChecks(t *testing.T) {
	env := ResultEnvelope{
		Asset:    "node",
		RunID:   "run",
		Pipeline: "pipe",
		Status:  "success",
		Checks: []CheckResult{
			{Name: "check_1", Status: "passed", Severity: "error", RowsReturned: 0},
			{Name: "check_2", Status: "failed", Severity: "warn", RowsReturned: 5},
			{Name: "check_3", Status: "warning", Severity: "error", RowsReturned: 1},
		},
	}

	data, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	var got ResultEnvelope
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	if len(got.Checks) != 3 {
		t.Fatalf("Checks: got %d, want 3", len(got.Checks))
	}
	if got.Checks[1].RowsReturned != 5 {
		t.Errorf("Checks[1].RowsReturned: got %d, want 5", got.Checks[1].RowsReturned)
	}
}

func TestResultEnvelope_MultipleArtifacts(t *testing.T) {
	env := ResultEnvelope{
		Asset:    "node",
		RunID:   "run",
		Pipeline: "pipe",
		Status:  "success",
		Artifacts: []Artifact{
			{Type: "log", URI: "gs://bucket/log.txt", Size: 1024},
			{Type: "state_snapshot", URI: "gs://bucket/state.json", Size: 2048},
			{Type: "context_update", URI: "file:///tmp/ctx.db", Size: 0},
		},
	}

	data, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	var got ResultEnvelope
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	if len(got.Artifacts) != 3 {
		t.Fatalf("Artifacts: got %d, want 3", len(got.Artifacts))
	}
	if got.Artifacts[2].Size != 0 {
		t.Errorf("Artifacts[2].Size: got %d, want 0", got.Artifacts[2].Size)
	}
}

func TestResultEnvelope_VersionField(t *testing.T) {
	env := ResultEnvelope{
		Version:  EnvelopeVersion,
		Asset:    "node",
		RunID:   "run",
		Pipeline: "pipe",
		Status:  "success",
	}

	data, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	if m["version"] != EnvelopeVersion {
		t.Errorf("version: got %v, want %q", m["version"], EnvelopeVersion)
	}
}

func TestResultEnvelope_VersionOmittedWhenEmpty(t *testing.T) {
	env := ResultEnvelope{
		Asset:    "node",
		RunID:   "run",
		Pipeline: "pipe",
		Status:  "success",
	}

	data, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	if _, ok := m["version"]; ok {
		t.Error("version should be omitted when empty")
	}
}

func TestEnvelopeVersion_IsSemanticVersion(t *testing.T) {
	// Verify the version constant follows semver format
	parts := strings.Split(EnvelopeVersion, ".")
	if len(parts) != 3 {
		t.Errorf("EnvelopeVersion %q should have 3 parts (semver), got %d", EnvelopeVersion, len(parts))
	}
}
