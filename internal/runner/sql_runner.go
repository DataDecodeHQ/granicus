package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"text/template"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/DataDecodeHQ/granicus/internal/config"
	"google.golang.org/api/iterator"
)

type SQLRunner struct {
	Timeout     time.Duration
	Connection  *config.ConnectionConfig
	FuncMap     template.FuncMap
	ValidateSQL bool
}

// NewSQLRunner creates a SQLRunner for the given BigQuery connection.
func NewSQLRunner(conn *config.ConnectionConfig) *SQLRunner {
	return &SQLRunner{
		Timeout:    DefaultTimeout,
		Connection: conn,
	}
}

type templateData struct {
	Project string
	Dataset string
	Prefix  string
}

// Run reads, templates, and executes a SQL file against BigQuery.
func (r *SQLRunner) Run(asset *Asset, projectRoot string, runID string) NodeResult {
	start := time.Now()

	sourcePath := filepath.Join(projectRoot, asset.Source)
	rawSQL, err := os.ReadFile(sourcePath)
	if err != nil {
		return NodeResult{
			AssetName: asset.Name,
			Status:    "failed",
			StartTime: start,
			EndTime:   time.Now(),
			Duration:  time.Since(start),
			Error:     fmt.Sprintf("reading SQL file: %v", err),
			ExitCode:  -1,
		}
	}

	rendered, err := renderSQL(rawSQL, r.Connection, asset, r.FuncMap)
	if err != nil {
		return NodeResult{
			AssetName: asset.Name,
			Status:    "failed",
			StartTime: start,
			EndTime:   time.Now(),
			Duration:  time.Since(start),
			Error:     err.Error(),
			ExitCode:  -1,
		}
	}

	// Prepend DROP TABLE to avoid partition spec conflicts on CREATE OR REPLACE
	rendered = prependDropForReplace(rendered)

	// Multi-statement scripts (DROP TABLE + CREATE OR REPLACE) don't support BQ
	// named parameters, so fall back to string substitution for @start/@end.
	// Single-statement queries use BQ named parameters instead.
	if isMultiStatementScript(rendered) {
		rendered = substituteIntervalVarsForDDL(rendered, asset)
	}

	timeout := effectiveTimeout(asset.Timeout, r.Timeout)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	client, err := newBQClient(ctx, r.Connection)
	if err != nil {
		return NodeResult{
			AssetName: asset.Name,
			Status:    "failed",
			StartTime: start,
			EndTime:   time.Now(),
			Duration:  time.Since(start),
			Error:     fmt.Sprintf("creating BQ client: %v", err),
			ExitCode:  -1,
		}
	}
	defer client.Close()

	q := client.Query(string(rendered))
	if !isMultiStatementScript(rendered) && asset.IntervalStart != "" {
		q.Parameters = append(q.Parameters, bigquery.QueryParameter{
			Name:  "start",
			Value: asset.IntervalStart,
		})
		q.Parameters = append(q.Parameters, bigquery.QueryParameter{
			Name:  "end",
			Value: asset.IntervalEnd,
		})
	}
	var estimatedBytes int64
	if r.ValidateSQL {
		dryQ := client.Query(string(rendered))
		dryQ.DryRun = true
		if !isMultiStatementScript(rendered) && asset.IntervalStart != "" {
			dryQ.Parameters = append(dryQ.Parameters, bigquery.QueryParameter{
				Name:  "start",
				Value: asset.IntervalStart,
			})
			dryQ.Parameters = append(dryQ.Parameters, bigquery.QueryParameter{
				Name:  "end",
				Value: asset.IntervalEnd,
			})
		}
		dryJob, dryErr := dryQ.Run(ctx)
		if dryErr != nil {
			return NodeResult{
				AssetName: asset.Name,
				Status:    "failed",
				StartTime: start,
				EndTime:   time.Now(),
				Duration:  time.Since(start),
				Error:     fmt.Sprintf("dry-run validation failed: %v", dryErr),
				ExitCode:  1,
			}
		}
		dryStatus, dryErr := dryJob.Wait(ctx)
		if dryErr != nil {
			return NodeResult{
				AssetName: asset.Name,
				Status:    "failed",
				StartTime: start,
				EndTime:   time.Now(),
				Duration:  time.Since(start),
				Error:     fmt.Sprintf("dry-run validation failed: %v", dryErr),
				ExitCode:  1,
			}
		}
		if dryStatus.Err() != nil {
			return NodeResult{
				AssetName: asset.Name,
				Status:    "failed",
				StartTime: start,
				EndTime:   time.Now(),
				Duration:  time.Since(start),
				Error:     fmt.Sprintf("dry-run validation failed: %v", dryStatus.Err()),
				ExitCode:  1,
			}
		}
		if dryStatus.Statistics != nil {
			estimatedBytes = dryStatus.Statistics.TotalBytesProcessed
		}
	}
	LogSQLExecution(asset.Name, r.Connection.Properties["dataset"], r.ValidateSQL, estimatedBytes)
	job, err := q.Run(ctx)
	if err != nil {
		return NodeResult{
			AssetName: asset.Name,
			Status:    "failed",
			StartTime: start,
			EndTime:   time.Now(),
			Duration:  time.Since(start),
			Error:     fmt.Sprintf("running query: %v", err),
			ExitCode:  1,
		}
	}

	status, err := job.Wait(ctx)
	end := time.Now()
	if err != nil {
		return NodeResult{
			AssetName: asset.Name,
			Status:    "failed",
			StartTime: start,
			EndTime:   end,
			Duration:  end.Sub(start),
			Error:     fmt.Sprintf("waiting for query: %v", err),
			ExitCode:  1,
		}
	}

	if status.Err() != nil {
		return NodeResult{
			AssetName: asset.Name,
			Status:    "failed",
			StartTime: start,
			EndTime:   end,
			Duration:  end.Sub(start),
			Error:     fmt.Sprintf("query failed: %v", status.Err()),
			ExitCode:  1,
		}
	}

	metadata := collectBQMetadata(status, job)

	return NodeResult{
		AssetName: asset.Name,
		Status:    "success",
		StartTime: start,
		EndTime:   end,
		Duration:  end.Sub(start),
		ExitCode:  0,
		Metadata:  metadata,
		Stdout:    string(rendered),
	}
}

// SQLCheckRunner runs a SQL query and checks if it returns any rows.
// 0 rows = pass (success), 1+ rows = fail.
type SQLCheckRunner struct {
	Connection *config.ConnectionConfig
	Timeout    time.Duration
	FuncMap    template.FuncMap
}

// NewSQLCheckRunner creates a SQLCheckRunner for the given BigQuery connection.
func NewSQLCheckRunner(conn *config.ConnectionConfig) *SQLCheckRunner {
	return &SQLCheckRunner{Connection: conn, Timeout: DefaultTimeout}
}

// Run executes a SQL check query and fails if any rows are returned.
func (r *SQLCheckRunner) Run(asset *Asset, projectRoot string, runID string) NodeResult {
	start := time.Now()

	var rawSQL []byte
	if asset.InlineSQL != "" {
		rawSQL = []byte(asset.InlineSQL)
	} else {
		sourcePath := filepath.Join(projectRoot, asset.Source)
		var err error
		rawSQL, err = os.ReadFile(sourcePath)
		if err != nil {
			return NodeResult{
				AssetName: asset.Name, Status: "failed", StartTime: start,
				EndTime: time.Now(), Duration: time.Since(start),
				Error: fmt.Sprintf("reading check SQL: %v", err), ExitCode: -1,
			}
		}
	}

	checkSQL, err := renderSQL(rawSQL, r.Connection, asset, r.FuncMap)
	if err != nil {
		return NodeResult{
			AssetName: asset.Name, Status: "failed", StartTime: start,
			EndTime: time.Now(), Duration: time.Since(start),
			Error: err.Error(), ExitCode: -1,
		}
	}

	timeout := effectiveTimeout(asset.Timeout, r.Timeout)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	client, err := newBQClient(ctx, r.Connection)
	if err != nil {
		return NodeResult{
			AssetName: asset.Name, Status: "failed", StartTime: start,
			EndTime: time.Now(), Duration: time.Since(start),
			Error: fmt.Sprintf("creating BQ client: %v", err), ExitCode: -1,
		}
	}
	defer client.Close()

	// Check queries are always single-statement SELECTs; pass @start/@end as
	// BQ named parameters rather than substituting into the SQL string.
	sqlStr := string(checkSQL)
	q := client.Query(sqlStr)
	if asset.IntervalStart != "" {
		q.Parameters = append(q.Parameters, bigquery.QueryParameter{
			Name:  "start",
			Value: asset.IntervalStart,
		})
		q.Parameters = append(q.Parameters, bigquery.QueryParameter{
			Name:  "end",
			Value: asset.IntervalEnd,
		})
	}
	it, err := q.Read(ctx)
	if err != nil {
		return NodeResult{
			AssetName: asset.Name, Status: "failed", StartTime: start,
			EndTime: time.Now(), Duration: time.Since(start),
			Error: fmt.Sprintf("running check query: %v", err), ExitCode: 1,
		}
	}

	// Read rows into structured maps (up to 100 sample rows)
	const maxSampleRows = 100
	var sampleRows []map[string]any
	rowCount := 0
	var iterErr error

	for {
		var row map[string]bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			iterErr = err
			break
		}
		rowCount++
		if rowCount <= maxSampleRows {
			m := make(map[string]any, len(row))
			for k, v := range row {
				m[k] = v
			}
			sampleRows = append(sampleRows, m)
		}
	}

	end := time.Now()

	if iterErr != nil {
		return NodeResult{
			AssetName: asset.Name, Status: "failed", StartTime: start,
			EndTime: end, Duration: end.Sub(start),
			Error: fmt.Sprintf("iterating check results: %v", iterErr), ExitCode: 1,
		}
	}
	metadata := map[string]string{
		"check_total_rows": strconv.Itoa(rowCount),
		"check_sql":        sqlStr,
	}

	if rowCount == 0 {
		return NodeResult{
			AssetName: asset.Name, Status: "success", StartTime: start,
			EndTime: end, Duration: end.Sub(start), ExitCode: 0,
			Metadata: metadata,
		}
	}

	if encoded, jerr := json.Marshal(sampleRows); jerr == nil {
		metadata["check_sample_rows"] = string(encoded)
	}

	var stdout string
	for i, row := range sampleRows {
		if i >= 10 {
			stdout += fmt.Sprintf("... and %d more rows\n", rowCount-10)
			break
		}
		stdout += fmt.Sprintf("%v\n", row)
	}

	return NodeResult{
		AssetName: asset.Name, Status: "failed", StartTime: start,
		EndTime: end, Duration: end.Sub(start), ExitCode: 1,
		Error:    fmt.Sprintf("check failed: %d rows returned", rowCount),
		Stdout:   stdout,
		Metadata: metadata,
	}
}

// createOrReplaceRe matches CREATE OR REPLACE TABLE `project.dataset.table`
var createOrReplaceRe = regexp.MustCompile("(?i)CREATE\\s+OR\\s+REPLACE\\s+TABLE\\s+`([^`]+)`")

// prependDropForReplace detects CREATE OR REPLACE TABLE statements and prepends
// a DROP TABLE IF EXISTS to avoid BigQuery errors when the existing table has a
// different partitioning/clustering spec than the replacement.
func prependDropForReplace(sql []byte) []byte {
	m := createOrReplaceRe.Find(sql)
	if m == nil {
		return sql
	}
	sub := createOrReplaceRe.FindSubmatch(sql)
	if sub == nil || len(sub) < 2 {
		return sql
	}
	tableRef := string(sub[1])
	drop := fmt.Sprintf("DROP TABLE IF EXISTS `%s`;\n", tableRef)
	return append([]byte(drop), sql...)
}

// estimateBQCostUSD computes the on-demand query cost at $5 per TB.
func estimateBQCostUSD(bytesProcessed int64) float64 {
	return float64(bytesProcessed) * 5.0 / 1e12
}

// substituteIntervalVarsForDDL replaces @start/@end with quoted string literals.
// Use this only for multi-statement scripts (DDL contexts) where BigQuery named
// parameters are not supported.
func substituteIntervalVarsForDDL(sql []byte, asset *Asset) []byte {
	s := string(sql)
	if asset.IntervalStart != "" {
		s = strings.ReplaceAll(s, "@start", "'"+asset.IntervalStart+"'")
		s = strings.ReplaceAll(s, "@end", "'"+asset.IntervalEnd+"'")
	}
	return []byte(s)
}

// isMultiStatementScript reports whether sql is a multi-statement script.
// We detect this via the DROP prefix that prependDropForReplace injects, since
// that is the only source of multi-statement SQL in this runner.
func isMultiStatementScript(sql []byte) bool {
	return bytes.HasPrefix(bytes.TrimSpace(sql), []byte("DROP TABLE IF EXISTS"))
}

// SubstituteTestVars replaces @test_start and @test_end placeholders with the given date boundaries.
func SubstituteTestVars(sql []byte, testStart, testEnd string) []byte {
	if testStart == "" {
		testStart = "1900-01-01"
	}
	if testEnd == "" {
		testEnd = "2099-12-31"
	}
	s := string(sql)
	s = strings.ReplaceAll(s, "@test_start", "'"+testStart+"'")
	s = strings.ReplaceAll(s, "@test_end", "'"+testEnd+"'")
	return []byte(s)
}

// ParseTestWindow converts a duration string like "7d", "2w", or "3m" into start and end date strings.
func ParseTestWindow(window string) (startDate, endDate string, err error) {
	if window == "" {
		return "1900-01-01", "2099-12-31", nil
	}

	if len(window) < 2 {
		return "", "", fmt.Errorf("invalid test window format: %q (expected Nd, Nw, or Nm)", window)
	}

	numStr := window[:len(window)-1]
	unit := window[len(window)-1]

	num := 0
	for _, c := range numStr {
		if c < '0' || c > '9' {
			return "", "", fmt.Errorf("invalid test window format: %q (expected Nd, Nw, or Nm)", window)
		}
		num = num*10 + int(c-'0')
	}
	if num == 0 {
		return "", "", fmt.Errorf("invalid test window: duration must be > 0")
	}

	now := time.Now().UTC()
	endDate = now.Format("2006-01-02")

	switch unit {
	case 'd':
		startDate = now.AddDate(0, 0, -num).Format("2006-01-02")
	case 'w':
		startDate = now.AddDate(0, 0, -num*7).Format("2006-01-02")
	case 'm':
		startDate = now.AddDate(0, -num, 0).Format("2006-01-02")
	default:
		return "", "", fmt.Errorf("invalid test window unit %q (expected d, w, or m)", string(unit))
	}

	return startDate, endDate, nil
}
