package archive

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"cloud.google.com/go/firestore"
	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

// Pruner deletes old run data from Firestore after verifying GCS archives exist.
type Pruner struct {
	fs             *firestore.Client
	gcs            *storage.Client
	archiveBucket  string
	retentionDays  int
}

// NewPruner creates a Firestore pruner.
func NewPruner(ctx context.Context, firestoreProject, archiveBucket string, retentionDays int) (*Pruner, error) {
	if retentionDays <= 0 {
		retentionDays = 30
		if env := os.Getenv("GRANICUS_RETENTION_DAYS"); env != "" {
			if d, err := strconv.Atoi(env); err == nil && d > 0 {
				retentionDays = d
			}
		}
	}
	if archiveBucket == "" {
		archiveBucket = os.Getenv("GRANICUS_OPS_BUCKET")
		if archiveBucket == "" {
			archiveBucket = "granicus-ops"
		}
	}

	fsClient, err := firestore.NewClient(ctx, firestoreProject)
	if err != nil {
		return nil, fmt.Errorf("creating Firestore client: %w", err)
	}

	gcsClient, err := storage.NewClient(ctx)
	if err != nil {
		fsClient.Close()
		return nil, fmt.Errorf("creating GCS client: %w", err)
	}

	return &Pruner{
		fs:            fsClient,
		gcs:           gcsClient,
		archiveBucket: archiveBucket,
		retentionDays: retentionDays,
	}, nil
}

// PruneRuns finds completed runs older than retention and deletes them from
// Firestore, but only after verifying the GCS archive exists.
func (p *Pruner) PruneRuns(ctx context.Context) (int, error) {
	cutoff := time.Now().AddDate(0, 0, -p.retentionDays)

	iter := p.fs.Collection("runs").
		Where("status", "in", []string{"succeeded", "failed", "crashed"}).
		Where("completed_at", "<", cutoff).
		Documents(ctx)
	defer iter.Stop()

	deleted := 0
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return deleted, fmt.Errorf("querying old runs: %w", err)
		}

		data := doc.Data()
		runID, _ := data["run_id"].(string)
		pipeline, _ := data["pipeline"].(string)
		startedAt, _ := data["started_at"].(time.Time)

		// Verify GCS archive exists before deleting
		archivePath := fmt.Sprintf("runs/%s/%s/%s/run_%s.jsonl",
			pipeline, startedAt.Format("2006"), startedAt.Format("01"), runID)

		_, err = p.gcs.Bucket(p.archiveBucket).Object(archivePath).Attrs(ctx)
		if err != nil {
			slog.Warn("skipping prune: archive not found",
				"run_id", runID, "path", archivePath, "error", err)
			continue
		}

		// Delete events subcollection first
		eventsIter := doc.Ref.Collection("events").Documents(ctx)
		batch := p.fs.Batch()
		eventCount := 0
		for {
			eventDoc, err := eventsIter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				eventsIter.Stop()
				break
			}
			batch.Delete(eventDoc.Ref)
			eventCount++
			if eventCount >= 500 {
				batch.Commit(ctx)
				batch = p.fs.Batch()
				eventCount = 0
			}
		}
		eventsIter.Stop()

		// Delete run document
		batch.Delete(doc.Ref)
		if _, err := batch.Commit(ctx); err != nil {
			slog.Error("prune commit failed", "run_id", runID, "error", err)
			continue
		}

		deleted++
		slog.Info("pruned run", "run_id", runID, "pipeline", pipeline,
			"events_deleted", eventCount)
	}

	return deleted, nil
}

// PruneIntervals deletes completed interval documents older than retention.
func (p *Pruner) PruneIntervals(ctx context.Context) (int, error) {
	cutoff := time.Now().AddDate(0, 0, -p.retentionDays)

	// Iterate over all pipeline documents
	pipelinesIter := p.fs.Collection("pipelines").Documents(ctx)
	defer pipelinesIter.Stop()

	deleted := 0
	for {
		pipelineDoc, err := pipelinesIter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return deleted, err
		}

		intervalsIter := pipelineDoc.Ref.Collection("intervals").
			Where("status", "==", "complete").
			Where("completed_at", "<", cutoff).
			Documents(ctx)

		batch := p.fs.Batch()
		count := 0
		for {
			doc, err := intervalsIter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				break
			}
			batch.Delete(doc.Ref)
			count++
			if count >= 500 {
				batch.Commit(ctx)
				batch = p.fs.Batch()
				count = 0
			}
		}
		intervalsIter.Stop()

		if count > 0 {
			batch.Commit(ctx)
		}
		deleted += count
	}

	return deleted, nil
}

// Close releases resources.
func (p *Pruner) Close() {
	p.fs.Close()
	p.gcs.Close()
}
