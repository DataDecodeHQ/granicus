package result

import (
	"context"
	"encoding/json"
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

func TestNewPublisher_MissingProject(t *testing.T) {
	t.Skip("requires Pub/Sub credentials")

	ctx := context.Background()
	_, err := NewPublisher(ctx, "", "")
	if err == nil {
		t.Error("expected error with empty project and no env vars set, got nil")
	}
}

func TestPublisher_Publish(t *testing.T) {
	t.Skip("requires Pub/Sub credentials")

	ctx := context.Background()
	p, err := NewPublisher(ctx, "test-project", "test-topic")
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	defer p.Close()

	env := ResultEnvelope{
		Asset:     "test-node",
		RunID:    "test-run",
		Pipeline: "test-pipeline",
		Status:   "success",
	}
	if err := p.Publish(ctx, env); err != nil {
		t.Errorf("Publish: %v", err)
	}
}

func TestPublisher_Close(t *testing.T) {
	t.Skip("requires Pub/Sub credentials")

	ctx := context.Background()
	p, err := NewPublisher(ctx, "test-project", "test-topic")
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}
