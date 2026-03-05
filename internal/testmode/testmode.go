package testmode

import (
	"context"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/option"

	"github.com/Andrew-DataDecode/Granicus/internal/events"
)

func TestDatasetName(baseDataset string, runID string) string {
	short := runID
	if len(runID) > 4 {
		short = runID[len(runID)-4:]
	}
	return fmt.Sprintf("%s__test_%s", baseDataset, short)
}

func CreateTestDataset(ctx context.Context, project, baseDataset, runID string, eventStore *events.Store, opts ...option.ClientOption) (string, error) {
	client, err := bigquery.NewClient(ctx, project, opts...)
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

	if err := client.Dataset(name).Create(ctx, meta); err != nil {
		return "", fmt.Errorf("creating test dataset %q: %w", name, err)
	}

	if eventStore != nil {
		_ = eventStore.Emit(events.Event{
			RunID: runID, EventType: "test_dataset_created", Severity: "info",
			Summary: fmt.Sprintf("Test dataset %s created", name),
			Details: map[string]any{"dataset_name": name, "base_dataset": baseDataset, "project": project},
		})
	}

	return name, nil
}

func DropTestDataset(ctx context.Context, project, datasetName, runID string, eventStore *events.Store, opts ...option.ClientOption) error {
	client, err := bigquery.NewClient(ctx, project, opts...)
	if err != nil {
		return fmt.Errorf("creating BQ client: %w", err)
	}
	defer client.Close()

	if err := client.Dataset(datasetName).DeleteWithContents(ctx); err != nil {
		return fmt.Errorf("dropping test dataset %q: %w", datasetName, err)
	}

	if eventStore != nil {
		_ = eventStore.Emit(events.Event{
			RunID: runID, EventType: "test_dataset_dropped", Severity: "info",
			Summary: fmt.Sprintf("Test dataset %s dropped", datasetName),
			Details: map[string]any{"dataset_name": datasetName, "project": project},
		})
	}

	return nil
}

type TestDatasetInfo struct {
	Name      string
	RunID     string
	CreatedAt time.Time
}

func ListTestDatasets(ctx context.Context, project, baseDataset string, opts ...option.ClientOption) ([]TestDatasetInfo, error) {
	client, err := bigquery.NewClient(ctx, project, opts...)
	if err != nil {
		return nil, fmt.Errorf("creating BQ client: %w", err)
	}
	defer client.Close()

	prefix := baseDataset + "__test_"
	var results []TestDatasetInfo

	it := client.Datasets(ctx)
	for {
		ds, err := it.Next()
		if err != nil {
			break
		}
		if !strings.HasPrefix(ds.DatasetID, prefix) {
			continue
		}
		meta, err := ds.Metadata(ctx)
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

func CleanupOldTestDatasets(ctx context.Context, project, baseDataset string, maxAge time.Duration, eventStore *events.Store, opts ...option.ClientOption) ([]string, error) {
	datasets, err := ListTestDatasets(ctx, project, baseDataset, opts...)
	if err != nil {
		return nil, err
	}

	cutoff := time.Now().Add(-maxAge)
	var dropped []string

	for _, ds := range datasets {
		if ds.CreatedAt.Before(cutoff) {
			if err := DropTestDataset(ctx, project, ds.Name, ds.RunID, eventStore, opts...); err != nil {
				return dropped, fmt.Errorf("cleaning up %s: %w", ds.Name, err)
			}
			dropped = append(dropped, ds.Name)
		}
	}

	return dropped, nil
}

func EmitTestMetadata(eventStore *events.Store, runID, pipeline string, metadata map[string]any) {
	if eventStore == nil {
		return
	}
	_ = eventStore.Emit(events.Event{
		RunID: runID, Pipeline: pipeline,
		EventType: "test_metadata_captured", Severity: "info",
		Summary: fmt.Sprintf("Test metadata captured for %s", pipeline),
		Details: metadata,
	})
}

func sanitizeLabel(s string) string {
	s = strings.ToLower(s)
	s = strings.ReplaceAll(s, "-", "_")
	if len(s) > 63 {
		s = s[:63]
	}
	return s
}
