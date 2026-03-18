package archive

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"cloud.google.com/go/storage"
	"github.com/DataDecodeHQ/granicus/internal/state"
)

// RunArchiver writes run archives to GCS as immutable JSONL files.
type RunArchiver struct {
	gcs    *storage.Client
	bucket string
}

// NewRunArchiver creates a GCS-backed run archiver.
func NewRunArchiver(ctx context.Context, bucket string) (*RunArchiver, error) {
	if bucket == "" {
		bucket = os.Getenv("GRANICUS_OPS_BUCKET")
		if bucket == "" {
			bucket = "granicus-ops"
		}
	}

	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating GCS client: %w", err)
	}

	return &RunArchiver{gcs: client, bucket: bucket}, nil
}

// ArchiveRun writes a run and its events as JSONL to GCS.
// Path: gs://<bucket>/runs/<pipeline>/YYYY/MM/run_<id>.jsonl
func (a *RunArchiver) ArchiveRun(ctx context.Context, fsBackend *state.FirestoreStateBackend, runID string) error {
	run, err := fsBackend.GetRun(ctx, runID)
	if err != nil {
		return fmt.Errorf("getting run: %w", err)
	}
	if run == nil {
		return fmt.Errorf("run %s not found", runID)
	}

	events, err := fsBackend.ListEvents(ctx, runID, nil)
	if err != nil {
		return fmt.Errorf("listing events: %w", err)
	}

	// Build object path
	startedAt := run.StartedAt
	if startedAt.IsZero() {
		startedAt = time.Now()
	}
	objectName := fmt.Sprintf("runs/%s/%s/%s/run_%s.jsonl",
		run.Pipeline,
		startedAt.Format("2006"),
		startedAt.Format("01"),
		runID,
	)

	writer := a.gcs.Bucket(a.bucket).Object(objectName).NewWriter(ctx)
	writer.ContentType = "application/x-ndjson"

	// First line: run record
	runLine, err := json.Marshal(map[string]any{
		"type":             "run",
		"run_id":           run.RunID,
		"pipeline":         run.Pipeline,
		"pipeline_version": run.PipelineVersion,
		"content_hash":     run.ContentHash,
		"status":           run.Status,
		"triggered_by":     run.TriggeredBy,
		"trigger_context":  run.TriggerContext,
		"parent_run_id":    run.ParentRunID,
		"started_at":       run.StartedAt,
		"completed_at":     run.CompletedAt,
		"node_count":       run.NodeCount,
		"succeeded":        run.Succeeded,
		"failed":           run.Failed,
		"skipped":          run.Skipped,
		"error_summary":    run.ErrorSummary,
	})
	if err != nil {
		writer.Close()
		return fmt.Errorf("marshaling run: %w", err)
	}
	writer.Write(append(runLine, '\n'))

	// Subsequent lines: events with telemetry
	for _, event := range events {
		eventLine, err := json.Marshal(map[string]any{
			"type":        "event",
			"run_id":      event.RunID,
			"pipeline":    event.Pipeline,
			"node":        event.Node,
			"event_type":  event.EventType,
			"status":      event.Status,
			"error":       event.Error,
			"exit_code":   event.ExitCode,
			"duration_ms": event.DurationMs,
			"attempt":     event.Attempt,
			"runner":      event.Runner,
			"metadata":    event.Metadata,
			"timestamp":   event.Timestamp,
		})
		if err != nil {
			continue
		}
		writer.Write(append(eventLine, '\n'))
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("writing archive to GCS: %w", err)
	}

	return nil
}

// Close releases resources.
func (a *RunArchiver) Close() error {
	return a.gcs.Close()
}
