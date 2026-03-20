package context

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
)

const schemaQuery = "SELECT table_name, column_name, data_type, ordinal_position, '' AS description FROM `%s`.INFORMATION_SCHEMA.COLUMNS ORDER BY table_name, ordinal_position"

const schemaQueryTimeout = 60 * time.Second

// SyncSchemas queries INFORMATION_SCHEMA.COLUMNS for each dataset and returns
// the combined schema rows. Failures on individual datasets are logged as
// warnings and skipped so partial results are still returned.
func SyncSchemas(client *bigquery.Client, datasets []string) []Schema {
	if client == nil {
		return nil
	}
	var all []Schema
	for _, ds := range datasets {
		rows, err := queryDatasetSchema(client, ds)
		if err != nil {
			slog.Warn("schema sync failed", "dataset", ds, "error", err)
			continue
		}
		all = append(all, rows...)
	}
	return all
}

type schemaRow struct {
	TableName       string `bigquery:"table_name"`
	ColumnName      string `bigquery:"column_name"`
	DataType        string `bigquery:"data_type"`
	OrdinalPosition int64  `bigquery:"ordinal_position"`
	Description     string `bigquery:"description"`
}

func queryDatasetSchema(client *bigquery.Client, dataset string) ([]Schema, error) {
	ctx, cancel := context.WithTimeout(context.Background(), schemaQueryTimeout)
	defer cancel()

	// Contract: Go owns this boundary. Read-only INFORMATION_SCHEMA query; no template variables.
	sql := fmt.Sprintf(schemaQuery, dataset)
	q := client.Query(sql)
	it, err := q.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("running query: %w", err)
	}

	var schemas []Schema
	for {
		var row schemaRow
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return schemas, fmt.Errorf("reading row: %w", err)
		}
		schemas = append(schemas, Schema{
			Dataset:     dataset,
			TableName:   row.TableName,
			ColumnName:  row.ColumnName,
			DataType:    row.DataType,
			Ordinal:     int(row.OrdinalPosition),
			Description: row.Description,
		})
	}
	return schemas, nil
}
