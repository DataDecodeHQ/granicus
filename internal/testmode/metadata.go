package testmode

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type ColumnInfo struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type TableMetadata struct {
	Name      string       `json:"name"`
	RowCount  int64        `json:"row_count"`
	Columns   []ColumnInfo `json:"columns"`
	NullCounts map[string]int64 `json:"null_counts,omitempty"`
}

type TestRunMetadata struct {
	RunID      string          `json:"run_id"`
	Dataset    string          `json:"dataset"`
	Tables     []TableMetadata `json:"tables"`
}

func CaptureMetadata(ctx context.Context, project, dataset string, opts ...option.ClientOption) (*TestRunMetadata, error) {
	client, err := bigquery.NewClient(ctx, project, opts...)
	if err != nil {
		return nil, fmt.Errorf("creating BQ client: %w", err)
	}
	defer client.Close()

	meta := &TestRunMetadata{
		Dataset: dataset,
	}

	// List tables in dataset
	it := client.Dataset(dataset).Tables(ctx)
	for {
		table, err := it.Next()
		if err != nil {
			break
		}

		tm, err := captureTableMetadata(ctx, client, project, dataset, table.TableID)
		if err != nil {
			continue
		}
		meta.Tables = append(meta.Tables, *tm)
	}

	return meta, nil
}

func captureTableMetadata(ctx context.Context, client *bigquery.Client, project, dataset, tableName string) (*TableMetadata, error) {
	tm := &TableMetadata{
		Name:       tableName,
		NullCounts: make(map[string]int64),
	}

	// Get schema via table metadata
	tableRef := client.Dataset(dataset).Table(tableName)
	tableMeta, err := tableRef.Metadata(ctx)
	if err != nil {
		return nil, err
	}

	for _, field := range tableMeta.Schema {
		tm.Columns = append(tm.Columns, ColumnInfo{
			Name: field.Name,
			Type: string(field.Type),
		})
	}

	// Get row count
	countSQL := fmt.Sprintf("SELECT COUNT(*) AS cnt FROM `%s.%s.%s`", project, dataset, tableName)
	q := client.Query(countSQL)
	qit, err := q.Read(ctx)
	if err == nil {
		var row []bigquery.Value
		if qit.Next(&row) == nil && len(row) > 0 {
			if cnt, ok := row[0].(int64); ok {
				tm.RowCount = cnt
			}
		}
	}

	// Get null counts per column
	for _, col := range tm.Columns {
		nullSQL := fmt.Sprintf(
			"SELECT COUNT(*) AS cnt FROM `%s.%s.%s` WHERE `%s` IS NULL",
			project, dataset, tableName, col.Name,
		)
		q := client.Query(nullSQL)
		qit, err := q.Read(ctx)
		if err != nil {
			continue
		}
		var row []bigquery.Value
		if qit.Next(&row) == nil && len(row) > 0 {
			if cnt, ok := row[0].(int64); ok {
				tm.NullCounts[col.Name] = cnt
			}
		}
	}

	return tm, nil
}

func WriteMetadata(meta *TestRunMetadata, path string) error {
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

// Ensure iterator import is used
var _ = iterator.Done
