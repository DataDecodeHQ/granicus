package state

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// FirestoreStateBackend implements StateBackend using Cloud Firestore.
// Interval state is stored at pipelines/{pipeline}/intervals/{asset}.
// Run state and events are stored at runs/{run_id} and runs/{run_id}/events/.
type FirestoreStateBackend struct {
	client   *firestore.Client
	pipeline string
}

// NewFirestoreStateBackend creates a Firestore-backed state backend.
// The project is read from GRANICUS_FIRESTORE_PROJECT env var if not provided.
func NewFirestoreStateBackend(ctx context.Context, project, pipeline string) (*FirestoreStateBackend, error) {
	if project == "" {
		project = os.Getenv("GRANICUS_FIRESTORE_PROJECT")
	}
	if project == "" {
		return nil, fmt.Errorf("Firestore project not configured (set GRANICUS_FIRESTORE_PROJECT)")
	}

	client, err := firestore.NewClient(ctx, project)
	if err != nil {
		return nil, fmt.Errorf("creating Firestore client: %w", err)
	}

	return &FirestoreStateBackend{
		client:   client,
		pipeline: pipeline,
	}, nil
}

// dag:boundary
func (f *FirestoreStateBackend) intervalsCol() *firestore.CollectionRef {
	return f.client.Collection("pipelines").Doc(f.pipeline).Collection("intervals")
}

func (f *FirestoreStateBackend) runsCol() *firestore.CollectionRef {
	return f.client.Collection("runs")
}

// dag:boundary
func (f *FirestoreStateBackend) MarkInProgress(asset, start, end, runID string) error {
	ctx := context.Background()
	docID := asset + ":" + start
	_, err := f.intervalsCol().Doc(docID).Set(ctx, map[string]interface{}{
		"asset_name":     asset,
		"pipeline":       f.pipeline,
		"interval_start": start,
		"interval_end":   end,
		"status":         "in_progress",
		"run_id":         runID,
		"started_at":     time.Now().UTC(),
		"completed_at":   nil,
		"error":          "",
		"attempt":        1,
	})
	return err
}

// dag:boundary
func (f *FirestoreStateBackend) MarkComplete(asset, start, end string) error {
	ctx := context.Background()
	docID := asset + ":" + start
	_, err := f.intervalsCol().Doc(docID).Update(ctx, []firestore.Update{
		{Path: "status", Value: "complete"},
		{Path: "completed_at", Value: time.Now().UTC()},
	})
	return err
}

// dag:boundary
func (f *FirestoreStateBackend) MarkFailed(asset, start, end string) error {
	ctx := context.Background()
	docID := asset + ":" + start
	_, err := f.intervalsCol().Doc(docID).Update(ctx, []firestore.Update{
		{Path: "status", Value: "failed"},
		{Path: "completed_at", Value: time.Now().UTC()},
	})
	return err
}

// GetIntervals returns all interval states for the given asset, ordered by start time.
func (f *FirestoreStateBackend) GetIntervals(asset string) ([]IntervalState, error) {
	ctx := context.Background()
	iter := f.intervalsCol().
		Where("asset_name", "==", asset).
		OrderBy("interval_start", firestore.Asc).
		Documents(ctx)
	defer iter.Stop()

	var result []IntervalState
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("iterating intervals: %w", err)
		}
		data := doc.Data()
		is := IntervalState{
			AssetName:     strVal(data, "asset_name"),
			IntervalStart: strVal(data, "interval_start"),
			IntervalEnd:   strVal(data, "interval_end"),
			Status:        strVal(data, "status"),
			RunID:         strVal(data, "run_id"),
			StartedAt:     timeStr(data, "started_at"),
			CompletedAt:   timeStr(data, "completed_at"),
		}
		result = append(result, is)
	}
	return result, nil
}

// InvalidateAll deletes all interval state documents for the given asset.
func (f *FirestoreStateBackend) InvalidateAll(asset string) error {
	ctx := context.Background()
	iter := f.intervalsCol().
		Where("asset_name", "==", asset).
		Documents(ctx)
	defer iter.Stop()

	batch := f.client.Batch()
	count := 0
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("querying intervals to invalidate: %w", err)
		}
		batch.Delete(doc.Ref)
		count++
		if count >= 500 {
			if _, err := batch.Commit(ctx); err != nil {
				return fmt.Errorf("batch delete: %w", err)
			}
			batch = f.client.Batch()
			count = 0
		}
	}
	if count > 0 {
		if _, err := batch.Commit(ctx); err != nil {
			return fmt.Errorf("batch delete: %w", err)
		}
	}
	return nil
}

// markRunsCrashed marks parent runs as crashed and writes recovery events for a batch of orphaned intervals.
func markRunsCrashed(batch *firestore.WriteBatch, orphans []IntervalState, pipeline string, runsCol *firestore.CollectionRef) {
	now := time.Now().UTC()
	seenRuns := make(map[string]bool)
	for _, iv := range orphans {
		if iv.RunID != "" && !seenRuns[iv.RunID] {
			seenRuns[iv.RunID] = true
			runRef := runsCol.Doc(iv.RunID)
			batch.Update(runRef, []firestore.Update{
				{Path: "status", Value: "crashed"},
				{Path: "error_summary", Value: "engine process terminated unexpectedly"},
				{Path: "completed_at", Value: now},
			})
		}
		// Write recovery event
		eventsCol := runsCol.Doc(iv.RunID).Collection("events")
		batch.Create(eventsCol.NewDoc(), EventDoc{
			RunID:     iv.RunID,
			Pipeline:  pipeline,
			Node:      iv.AssetName,
			EventType: "node_failed",
			Error:     "engine crashed during execution (orphan recovery)",
			Timestamp: now,
		})
	}
}

// dag:boundary
func (f *FirestoreStateBackend) RecoverOrphans(threshold time.Duration) ([]IntervalState, error) {
	if f.pipeline == "" {
		// Cross-pipeline orphan recovery not supported in Firestore yet;
		// each pipeline's state backend handles its own orphans.
		return nil, nil
	}

	if threshold <= 0 {
		threshold = DefaultOrphanTimeout
	}

	ctx := context.Background()
	cutoff := time.Now().UTC().Add(-threshold)

	iter := f.intervalsCol().
		Where("status", "==", "in_progress").
		Where("started_at", "<", cutoff).
		Documents(ctx)
	defer iter.Stop()

	var orphans []IntervalState
	batch := f.client.Batch()

	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("querying orphaned intervals: %w", err)
		}
		data := doc.Data()
		iv := IntervalState{
			AssetName:     strVal(data, "asset_name"),
			IntervalStart: strVal(data, "interval_start"),
			IntervalEnd:   strVal(data, "interval_end"),
			Status:        strVal(data, "status"),
			RunID:         strVal(data, "run_id"),
			StartedAt:     timeStr(data, "started_at"),
		}
		orphans = append(orphans, iv)

		slog.Warn("recovering orphaned interval",
			"asset", iv.AssetName, "interval", iv.IntervalStart,
			"run_id", iv.RunID, "started_at", iv.StartedAt)

		batch.Update(doc.Ref, []firestore.Update{
			{Path: "status", Value: "failed"},
			{Path: "error", Value: "engine crashed during execution (orphan recovery)"},
			{Path: "completed_at", Value: time.Now().UTC()},
		})
	}

	if len(orphans) == 0 {
		return nil, nil
	}

	markRunsCrashed(batch, orphans, f.pipeline, f.runsCol())

	if _, err := batch.Commit(ctx); err != nil {
		return nil, fmt.Errorf("committing orphan recovery: %w", err)
	}

	return orphans, nil
}

// Close shuts down the Firestore client connection.
func (f *FirestoreStateBackend) Close() error {
	return f.client.Close()
}

// --- Run and Event Store ---

// RunDoc represents a pipeline run in Firestore.
type RunDoc struct {
	RunID           string         `firestore:"run_id"`
	Pipeline        string         `firestore:"pipeline"`
	PipelineVersion int            `firestore:"pipeline_version"`
	ContentHash     string         `firestore:"content_hash"`
	Status          string         `firestore:"status"` // running, succeeded, failed, crashed
	TriggeredBy     string         `firestore:"triggered_by"`
	TriggerContext  string         `firestore:"trigger_context"`
	ParentRunID     string         `firestore:"parent_run_id,omitempty"`
	StartedAt       time.Time      `firestore:"started_at"`
	CompletedAt     time.Time      `firestore:"completed_at,omitempty"`
	NodeCount       int            `firestore:"node_count"`
	Succeeded       int            `firestore:"succeeded"`
	Failed          int            `firestore:"failed"`
	Skipped         int            `firestore:"skipped"`
	ErrorSummary    string         `firestore:"error_summary,omitempty"`
	ConfigSnapshot  map[string]any `firestore:"config_snapshot,omitempty"`
}

// EventDoc represents a node-level event in a run's event subcollection.
type EventDoc struct {
	RunID      string         `firestore:"run_id"`
	Pipeline   string         `firestore:"pipeline"`
	Node       string         `firestore:"node"`
	EventType  string         `firestore:"event_type"`
	Status     string         `firestore:"status,omitempty"`
	Error      string         `firestore:"error,omitempty"`
	ExitCode   int            `firestore:"exit_code,omitempty"`
	DurationMs int64          `firestore:"duration_ms,omitempty"`
	Attempt    int            `firestore:"attempt,omitempty"`
	Runner     string         `firestore:"runner,omitempty"`
	Metadata   map[string]any `firestore:"metadata,omitempty"`
	Timestamp  time.Time      `firestore:"timestamp"`
}

// CreateRun creates a new run document in Firestore.
func (f *FirestoreStateBackend) CreateRun(ctx context.Context, run RunDoc) error {
	_, err := f.runsCol().Doc(run.RunID).Set(ctx, run)
	return err
}

// UpdateRun updates fields on a run document.
func (f *FirestoreStateBackend) UpdateRun(ctx context.Context, runID string, updates []firestore.Update) error {
	_, err := f.runsCol().Doc(runID).Update(ctx, updates)
	return err
}

// CompleteRun marks a run as succeeded or failed with final counts.
func (f *FirestoreStateBackend) CompleteRun(ctx context.Context, runID, resultStatus, errorSummary string, succeeded, failed, skipped int) error {
	_, err := f.runsCol().Doc(runID).Update(ctx, []firestore.Update{
		{Path: "status", Value: resultStatus},
		{Path: "completed_at", Value: time.Now().UTC()},
		{Path: "succeeded", Value: succeeded},
		{Path: "failed", Value: failed},
		{Path: "skipped", Value: skipped},
		{Path: "error_summary", Value: errorSummary},
	})
	return err
}

// WriteEvent adds an event to a run's event subcollection.
func (f *FirestoreStateBackend) WriteEvent(ctx context.Context, runID string, event EventDoc) error {
	_, _, err := f.runsCol().Doc(runID).Collection("events").Add(ctx, event)
	return err
}

// WriteFailureBatch atomically records a node failure, updates the interval,
// and records downstream skips in a single Firestore batch.
func (f *FirestoreStateBackend) WriteFailureBatch(ctx context.Context, runID string, failedEvent EventDoc, intervalAsset, intervalStart string, skippedNodes []string) error {
	batch := f.client.Batch()

	// Record the failure event
	eventsCol := f.runsCol().Doc(runID).Collection("events")
	batch.Create(eventsCol.NewDoc(), failedEvent)

	// Update interval state atomically
	if intervalAsset != "" && intervalStart != "" {
		docID := intervalAsset + ":" + intervalStart
		batch.Update(f.intervalsCol().Doc(docID), []firestore.Update{
			{Path: "status", Value: "failed"},
			{Path: "error", Value: failedEvent.Error},
			{Path: "completed_at", Value: time.Now().UTC()},
		})
	}

	// Record downstream skips
	now := time.Now().UTC()
	for _, node := range skippedNodes {
		batch.Create(eventsCol.NewDoc(), EventDoc{
			RunID:     runID,
			Pipeline:  f.pipeline,
			Node:      node,
			EventType: "node_skipped",
			Error:     "upstream " + failedEvent.Node + " failed",
			Timestamp: now,
		})
	}

	_, err := batch.Commit(ctx)
	return err
}

// GetRun retrieves a run document by ID.
func (f *FirestoreStateBackend) GetRun(ctx context.Context, runID string) (*RunDoc, error) {
	doc, err := f.runsCol().Doc(runID).Get(ctx)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, nil
		}
		return nil, err
	}
	var run RunDoc
	if err := doc.DataTo(&run); err != nil {
		return nil, err
	}
	return &run, nil
}

// ListRuns queries runs by pipeline and status, ordered by started_at descending.
func (f *FirestoreStateBackend) ListRuns(ctx context.Context, pipeline string, statuses []string, since time.Time, limit int) ([]RunDoc, error) {
	q := f.runsCol().Where("pipeline", "==", pipeline)
	if len(statuses) > 0 {
		q = q.Where("status", "in", statuses)
	}
	if !since.IsZero() {
		q = q.Where("started_at", ">=", since)
	}
	q = q.OrderBy("started_at", firestore.Desc)
	if limit > 0 {
		q = q.Limit(limit)
	}

	iter := q.Documents(ctx)
	defer iter.Stop()

	var runs []RunDoc
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var run RunDoc
		if err := doc.DataTo(&run); err != nil {
			return nil, err
		}
		runs = append(runs, run)
	}
	return runs, nil
}

// ListEvents retrieves events for a run, optionally filtered by event type.
// dag:boundary
func (f *FirestoreStateBackend) ListEvents(ctx context.Context, runID string, eventTypes []string) ([]EventDoc, error) {
	eventsCol := f.runsCol().Doc(runID).Collection("events")
	var q firestore.Query
	if len(eventTypes) > 0 {
		q = eventsCol.Where("event_type", "in", eventTypes).OrderBy("timestamp", firestore.Asc)
	} else {
		q = eventsCol.OrderBy("timestamp", firestore.Asc)
	}

	iter := q.Documents(ctx)
	defer iter.Stop()

	var events []EventDoc
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var event EventDoc
		if err := doc.DataTo(&event); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, nil
}

// --- Run Locking ---

// dag:boundary
func (f *FirestoreStateBackend) lockRef() *firestore.DocumentRef {
	return f.client.Collection("pipelines").Doc(f.pipeline).Collection("lock").Doc("current")
}

// AcquireLock atomically acquires a pipeline run lock. Returns an error if
// the pipeline is already locked by another run.
// dag:boundary
func (f *FirestoreStateBackend) AcquireLock(ctx context.Context, runID string) error {
	return f.client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		lockDoc, err := tx.Get(f.lockRef())
		if err != nil && status.Code(err) != codes.NotFound {
			return fmt.Errorf("reading lock: %w", err)
		}
		if err == nil && lockDoc.Exists() {
			data := lockDoc.Data()
			existingRunID := strVal(data, "run_id")
			return fmt.Errorf("pipeline %s already running: %s", f.pipeline, existingRunID)
		}
		return tx.Set(f.lockRef(), map[string]interface{}{
			"run_id":    runID,
			"locked_at": time.Now().UTC(),
		})
	})
}

// ReleaseLock releases the pipeline run lock.
// dag:boundary
func (f *FirestoreStateBackend) ReleaseLock(ctx context.Context) error {
	_, err := f.lockRef().Delete(ctx)
	return err
}

// --- Helpers ---

func strVal(data map[string]interface{}, key string) string {
	if v, ok := data[key]; ok && v != nil {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func timeStr(data map[string]interface{}, key string) string {
	if v, ok := data[key]; ok && v != nil {
		if t, ok := v.(time.Time); ok {
			return t.Format(time.RFC3339)
		}
	}
	return ""
}

// Verify FirestoreStateBackend implements StateBackend at compile time.
var _ StateBackend = (*FirestoreStateBackend)(nil)
