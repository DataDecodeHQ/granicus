package runner

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"text/template"

	"cloud.google.com/go/bigquery"
	"github.com/DataDecodeHQ/granicus/internal/config"
	"github.com/DataDecodeHQ/granicus/internal/result"
	"google.golang.org/api/option"
)

// renderSQL parses rawSQL as a Go template, executes it with project/dataset/prefix
// derived from conn and asset, then applies interval and test-var substitutions.
func renderSQL(rawSQL []byte, conn *config.ConnectionConfig, asset *Asset, funcMap template.FuncMap) ([]byte, error) {
	tmpl := template.New("sql")
	if funcMap != nil {
		tmpl = tmpl.Funcs(funcMap)
	}
	var err error
	tmpl, err = tmpl.Parse(string(rawSQL))
	if err != nil {
		return nil, fmt.Errorf("parsing SQL template: %w", err)
	}

	dataset := conn.Properties["dataset"]
	if asset.Dataset != "" {
		dataset = asset.Dataset
	}
	data := templateData{
		Project: conn.Properties["project"],
		Dataset: dataset,
		Prefix:  asset.Prefix,
	}

	buf := new(bytes.Buffer)
	if err := tmpl.Execute(buf, data); err != nil {
		return nil, fmt.Errorf("executing SQL template: %w", err)
	}

	rendered := substituteIntervalVars(buf.Bytes(), asset)
	rendered = SubstituteTestVars(rendered, asset.TestStart, asset.TestEnd)
	return rendered, nil
}

// newBQClient creates a BigQuery client for the project in conn.
func newBQClient(ctx context.Context, conn *config.ConnectionConfig) (*bigquery.Client, error) {
	project := conn.Properties["project"]
	var opts []option.ClientOption
	if creds := conn.Properties["credentials"]; creds != "" {
		opts = append(opts, option.WithCredentialsFile(creds))
	}
	return bigquery.NewClient(ctx, project, opts...)
}

// dag:boundary
// collectBQMetadata extracts telemetry and cost metadata from a completed BQ job.
func collectBQMetadata(status *bigquery.JobStatus, job *bigquery.Job) map[string]string {
	metadata := make(map[string]string)
	if stats := status.Statistics; stats != nil {
		bytesProcessed := stats.TotalBytesProcessed
		metadata["total_bytes_processed"] = strconv.FormatInt(bytesProcessed, 10)
		metadata["estimated_cost_usd"] = fmt.Sprintf("%.6f", estimateBQCostUSD(bytesProcessed))
		metadata[result.TelBQBytesScanned] = strconv.FormatInt(bytesProcessed, 10)
		metadata[result.TelBQJobID] = job.ID()
		if qStats, ok := stats.Details.(*bigquery.QueryStatistics); ok {
			metadata["total_slot_ms"] = strconv.FormatInt(qStats.SlotMillis, 10)
			metadata[result.TelBQSlotMs] = strconv.FormatInt(qStats.SlotMillis, 10)
			metadata[result.TelBQCacheHit] = strconv.FormatBool(qStats.CacheHit)
			metadata["cache_hit"] = strconv.FormatBool(qStats.CacheHit)
			metadata["rows_affected"] = strconv.FormatInt(qStats.NumDMLAffectedRows, 10)
			metadata[result.TelBQRowCount] = strconv.FormatInt(qStats.NumDMLAffectedRows, 10)
			if qStats.ReferencedTables != nil {
				for _, t := range qStats.ReferencedTables {
					metadata["destination_table"] = t.DatasetID + "." + t.TableID
				}
			}
			if qStats.TotalBytesProcessedAccuracy != "" {
				metadata[result.TelBQBytesWritten] = strconv.FormatInt(stats.TotalBytesProcessed, 10)
			}
		}
	}
	return metadata
}
