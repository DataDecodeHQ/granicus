package testmode

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/option"

	"github.com/DataDecodeHQ/granicus/internal/events"
)

// TestConfig bundles the shared dependencies for test dataset operations.
type TestConfig struct {
	Ctx        context.Context
	Project    string
	EventStore *events.Store
	ClientOpts []option.ClientOption
}

// TestDatasetName returns the BigQuery dataset name for a test run.
func TestDatasetName(baseDataset string, runID string) string {
	short := runID
	if len(runID) > 4 {
		short = runID[len(runID)-4:]
	}
	return fmt.Sprintf("%s__test_%s", baseDataset, short)
}

// CreateTestDataset creates an isolated BigQuery dataset for a test run.
func CreateTestDataset(cfg TestConfig, baseDataset, runID string) (string, error) {
	client, err := bigquery.NewClient(cfg.Ctx, cfg.Project, cfg.ClientOpts...)
	if err != nil {
		return "", fmt.Errorf("creating BQ client: %w", err)
	}
	defer client.Close()

	name := TestDatasetName(baseDataset, runID)

	meta := &bigquery.DatasetMetadata{
		Labels: map[string]string{
			"granicus_test_run": sanitizeLabel(runID),
		},
	}

	// Contract: Go owns this boundary. Dataset lifecycle management; isolated per run ID.
	if err := client.Dataset(name).Create(cfg.Ctx, meta); err != nil {
		return "", fmt.Errorf("creating test dataset %q: %w", name, err)
	}

	if cfg.EventStore != nil {
		if err := cfg.EventStore.Emit(events.Event{
			RunID: runID, EventType: "test_dataset_created", Severity: "info",
			Summary: fmt.Sprintf("Test dataset %s created", name),
			Details: map[string]any{"dataset_name": name, "base_dataset": baseDataset, "project": cfg.Project},
		}); err != nil {
			slog.Warn("failed to emit event", "event_type", "test_dataset_created", "error", err)
		}
	}

	return name, nil
}

// dag:boundary
func DropTestDataset(cfg TestConfig, datasetName, runID string) error {
	client, err := bigquery.NewClient(cfg.Ctx, cfg.Project, cfg.ClientOpts...)
	if err != nil {
		return fmt.Errorf("creating BQ client: %w", err)
	}
	defer client.Close()

	if err := client.Dataset(datasetName).DeleteWithContents(cfg.Ctx); err != nil {
		return fmt.Errorf("dropping test dataset %q: %w", datasetName, err)
	}

	if cfg.EventStore != nil {
		if err := cfg.EventStore.Emit(events.Event{
			RunID: runID, EventType: "test_dataset_dropped", Severity: "info",
			Summary: fmt.Sprintf("Test dataset %s dropped", datasetName),
			Details: map[string]any{"dataset_name": datasetName, "project": cfg.Project},
		}); err != nil {
			slog.Warn("failed to emit event", "event_type", "test_dataset_dropped", "error", err)
		}
	}

	return nil
}

type TestDatasetInfo struct {
	Name      string
	RunID     string
	CreatedAt time.Time
}

// dag:boundary
func ListTestDatasets(cfg TestConfig, baseDataset string) ([]TestDatasetInfo, error) {
	client, err := bigquery.NewClient(cfg.Ctx, cfg.Project, cfg.ClientOpts...)
	if err != nil {
		return nil, fmt.Errorf("creating BQ client: %w", err)
	}
	defer client.Close()

	prefix := baseDataset + "__test_"
	var results []TestDatasetInfo

	it := client.Datasets(cfg.Ctx)
	for {
		ds, err := it.Next()
		if err != nil {
			break
		}
		if !strings.HasPrefix(ds.DatasetID, prefix) {
			continue
		}
		meta, err := ds.Metadata(cfg.Ctx)
		if err != nil {
			continue
		}
		info := TestDatasetInfo{
			Name:      ds.DatasetID,
			CreatedAt: meta.CreationTime,
		}
		if runID, ok := meta.Labels["granicus_test_run"]; ok {
			info.RunID = runID
		}
		results = append(results, info)
	}

	return results, nil
}

// CleanupOldTestDatasets drops test datasets older than maxAge.
func CleanupOldTestDatasets(cfg TestConfig, baseDataset string, maxAge time.Duration) ([]string, error) {
	datasets, err := ListTestDatasets(cfg, baseDataset)
	if err != nil {
		return nil, err
	}

	cutoff := time.Now().Add(-maxAge)
	var dropped []string

	for _, ds := range datasets {
		if ds.CreatedAt.Before(cutoff) {
			if err := DropTestDataset(cfg, ds.Name, ds.RunID); err != nil {
				return dropped, fmt.Errorf("cleaning up %s: %w", ds.Name, err)
			}
			dropped = append(dropped, ds.Name)
		}
	}

	return dropped, nil
}

// EmitTestMetadata records test metadata as an event.
func EmitTestMetadata(eventStore *events.Store, runID, pipeline string, metadata map[string]any) {
	if eventStore == nil {
		return
	}
	if err := eventStore.Emit(events.Event{
		RunID: runID, Pipeline: pipeline,
		EventType: "test_metadata_captured", Severity: "info",
		Summary: fmt.Sprintf("Test metadata captured for %s", pipeline),
		Details: metadata,
	}); err != nil {
		slog.Warn("failed to emit event", "event_type", "test_metadata_captured", "error", err)
	}
}

func sanitizeLabel(s string) string {
	s = strings.ToLower(s)
	s = strings.ReplaceAll(s, "-", "_")
	if len(s) > 63 {
		s = s[:63]
	}
	return s
}
