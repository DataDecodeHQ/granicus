package runner

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"text/template"
	"time"

	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/analytehealth/granicus/internal/config"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type SQLRunner struct {
	Timeout    time.Duration
	Connection *config.ConnectionConfig
}

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

	tmpl, err := template.New("sql").Parse(string(rawSQL))
	if err != nil {
		return NodeResult{
			AssetName: asset.Name,
			Status:    "failed",
			StartTime: start,
			EndTime:   time.Now(),
			Duration:  time.Since(start),
			Error:     fmt.Sprintf("parsing SQL template: %v", err),
			ExitCode:  -1,
		}
	}

	data := templateData{
		Project: r.Connection.Properties["project"],
		Dataset: r.Connection.Properties["dataset"],
		Prefix:  asset.Prefix,
	}

	var rendered []byte
	buf := new(bytes.Buffer)
	if err := tmpl.Execute(buf, data); err != nil {
		return NodeResult{
			AssetName: asset.Name,
			Status:    "failed",
			StartTime: start,
			EndTime:   time.Now(),
			Duration:  time.Since(start),
			Error:     fmt.Sprintf("executing SQL template: %v", err),
			ExitCode:  -1,
		}
	}
	rendered = buf.Bytes()

	// Second pass: replace @start/@end with interval boundaries
	rendered = substituteIntervalVars(rendered, asset)
	rendered = SubstituteTestVars(rendered, asset.TestStart, asset.TestEnd)

	timeout := r.Timeout
	if timeout == 0 {
		timeout = DefaultTimeout
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	project := r.Connection.Properties["project"]
	var opts []option.ClientOption
	if creds := r.Connection.Properties["credentials"]; creds != "" {
		opts = append(opts, option.WithCredentialsFile(creds))
	}

	client, err := bigquery.NewClient(ctx, project, opts...)
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

	metadata := make(map[string]string)
	if stats := status.Statistics; stats != nil {
		metadata["bytes_processed"] = strconv.FormatInt(stats.TotalBytesProcessed, 10)
		if dml := stats.Details; dml != nil {
			if qStats, ok := dml.(*bigquery.QueryStatistics); ok {
				metadata["rows_affected"] = strconv.FormatInt(qStats.NumDMLAffectedRows, 10)
				if qStats.ReferencedTables != nil {
					for _, t := range qStats.ReferencedTables {
						metadata["destination_table"] = t.DatasetID + "." + t.TableID
					}
				}
			}
		}
	}

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
}

func NewSQLCheckRunner(conn *config.ConnectionConfig) *SQLCheckRunner {
	return &SQLCheckRunner{Connection: conn, Timeout: DefaultTimeout}
}

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

	tmpl, err := template.New("check").Parse(string(rawSQL))
	if err != nil {
		return NodeResult{
			AssetName: asset.Name, Status: "failed", StartTime: start,
			EndTime: time.Now(), Duration: time.Since(start),
			Error: fmt.Sprintf("parsing check template: %v", err), ExitCode: -1,
		}
	}

	data := templateData{
		Project: r.Connection.Properties["project"],
		Dataset: r.Connection.Properties["dataset"],
		Prefix:  asset.Prefix,
	}
	buf := new(bytes.Buffer)
	if err := tmpl.Execute(buf, data); err != nil {
		return NodeResult{
			AssetName: asset.Name, Status: "failed", StartTime: start,
			EndTime: time.Now(), Duration: time.Since(start),
			Error: fmt.Sprintf("executing check template: %v", err), ExitCode: -1,
		}
	}

	checkSQL := substituteIntervalVars(buf.Bytes(), asset)
	checkSQL = SubstituteTestVars(checkSQL, asset.TestStart, asset.TestEnd)

	timeout := r.Timeout
	if timeout == 0 {
		timeout = DefaultTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	project := r.Connection.Properties["project"]
	var opts []option.ClientOption
	if creds := r.Connection.Properties["credentials"]; creds != "" {
		opts = append(opts, option.WithCredentialsFile(creds))
	}

	client, err := bigquery.NewClient(ctx, project, opts...)
	if err != nil {
		return NodeResult{
			AssetName: asset.Name, Status: "failed", StartTime: start,
			EndTime: time.Now(), Duration: time.Since(start),
			Error: fmt.Sprintf("creating BQ client: %v", err), ExitCode: -1,
		}
	}
	defer client.Close()

	q := client.Query(string(checkSQL))
	it, err := q.Read(ctx)
	if err != nil {
		return NodeResult{
			AssetName: asset.Name, Status: "failed", StartTime: start,
			EndTime: time.Now(), Duration: time.Since(start),
			Error: fmt.Sprintf("running check query: %v", err), ExitCode: 1,
		}
	}

	rowCount := 0
	var stdout string
	for {
		var row []bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			break
		}
		rowCount++
		if rowCount <= 10 {
			stdout += fmt.Sprintf("%v\n", row)
		}
	}

	end := time.Now()
	if rowCount == 0 {
		return NodeResult{
			AssetName: asset.Name, Status: "success", StartTime: start,
			EndTime: end, Duration: end.Sub(start), ExitCode: 0,
			Metadata: map[string]string{"check_rows": "0"},
		}
	}

	return NodeResult{
		AssetName: asset.Name, Status: "failed", StartTime: start,
		EndTime: end, Duration: end.Sub(start), ExitCode: 1,
		Error:  fmt.Sprintf("check failed: %d rows returned", rowCount),
		Stdout: stdout,
		Metadata: map[string]string{"check_rows": strconv.Itoa(rowCount)},
	}
}

func substituteIntervalVars(sql []byte, asset *Asset) []byte {
	s := string(sql)
	if asset.IntervalStart != "" {
		s = strings.ReplaceAll(s, "@start", "'"+asset.IntervalStart+"'")
		s = strings.ReplaceAll(s, "@end", "'"+asset.IntervalEnd+"'")
	}
	return []byte(s)
}

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
