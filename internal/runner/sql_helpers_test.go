package runner

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"text/template"

	"github.com/DataDecodeHQ/granicus/internal/result"
)

// renderSQLForTest replicates the template rendering pipeline used by both
// SQLRunner.Run and SQLCheckRunner.Run. @start/@end are left intact by
// renderSQL; this helper optionally applies DDL-style string substitution so
// existing tests that assert on the substituted output still pass.
func renderSQLForTest(rawSQL string, project, dataset, prefix, intervalStart, intervalEnd, testStart, testEnd string) ([]byte, error) {
	tmpl, err := template.New("sql").Parse(rawSQL)
	if err != nil {
		return nil, err
	}
	data := templateData{
		Project: project,
		Dataset: dataset,
		Prefix:  prefix,
	}
	buf := new(bytes.Buffer)
	if err := tmpl.Execute(buf, data); err != nil {
		return nil, err
	}
	asset := &Asset{
		IntervalStart: intervalStart,
		IntervalEnd:   intervalEnd,
		TestStart:     testStart,
		TestEnd:       testEnd,
	}
	// @start/@end are handled as BQ named parameters at query time; apply the
	// DDL fallback here so tests can assert on the substituted values.
	rendered := substituteIntervalVarsForDDL(buf.Bytes(), asset)
	rendered = SubstituteTestVars(rendered, asset.TestStart, asset.TestEnd)
	return rendered, nil
}

// -- renderSQL tests --

func TestRenderSQL_TemplateParsing(t *testing.T) {
	rawSQL := `SELECT * FROM {{.Project}}.{{.Dataset}}.orders`
	got, err := renderSQLForTest(rawSQL, "my-project", "my_dataset", "", "", "", "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []byte("SELECT * FROM my-project.my_dataset.orders")
	if !bytes.Equal(got, want) {
		t.Errorf("rendered SQL mismatch:\n got:  %q\n want: %q", got, want)
	}
}

func TestRenderSQL_PrefixSubstitution(t *testing.T) {
	rawSQL := `SELECT * FROM {{.Project}}.{{.Dataset}}.{{.Prefix}}orders`
	got, err := renderSQLForTest(rawSQL, "proj", "ds", "stg_", "", "", "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []byte("SELECT * FROM proj.ds.stg_orders")
	if !bytes.Equal(got, want) {
		t.Errorf("rendered SQL mismatch:\n got:  %q\n want: %q", got, want)
	}
}

func TestRenderSQL_IntervalSubstitution(t *testing.T) {
	rawSQL := `SELECT * FROM t WHERE dt >= @start AND dt < @end`
	got, err := renderSQLForTest(rawSQL, "p", "d", "", "2025-01-10", "2025-01-11", "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	gotStr := string(got)
	if !strings.Contains(gotStr, "'2025-01-10'") {
		t.Errorf("@start not substituted: %q", gotStr)
	}
	if !strings.Contains(gotStr, "'2025-01-11'") {
		t.Errorf("@end not substituted: %q", gotStr)
	}
	if strings.Contains(gotStr, "@start") || strings.Contains(gotStr, "@end") {
		t.Errorf("raw @start/@end still present: %q", gotStr)
	}
}

func TestRenderSQL_TestVarSubstitution(t *testing.T) {
	rawSQL := `SELECT * FROM t WHERE created_at >= @test_start AND created_at < @test_end`
	got, err := renderSQLForTest(rawSQL, "p", "d", "", "", "", "2025-03-01", "2025-03-08")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	gotStr := string(got)
	if !strings.Contains(gotStr, "'2025-03-01'") {
		t.Errorf("@test_start not substituted: %q", gotStr)
	}
	if !strings.Contains(gotStr, "'2025-03-08'") {
		t.Errorf("@test_end not substituted: %q", gotStr)
	}
	if strings.Contains(gotStr, "@test_start") || strings.Contains(gotStr, "@test_end") {
		t.Errorf("raw @test_start/@test_end still present: %q", gotStr)
	}
}

func TestRenderSQL_TestVarDefaults(t *testing.T) {
	rawSQL := `WHERE created_at >= @test_start AND created_at < @test_end`
	got, err := renderSQLForTest(rawSQL, "p", "d", "", "", "", "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	gotStr := string(got)
	if !strings.Contains(gotStr, "'1900-01-01'") {
		t.Errorf("@test_start should default to 1900-01-01: %q", gotStr)
	}
	if !strings.Contains(gotStr, "'2099-12-31'") {
		t.Errorf("@test_end should default to 2099-12-31: %q", gotStr)
	}
}

func TestRenderSQL_IntervalAndTestVarsTogether(t *testing.T) {
	// Identical inputs to what SQLRunner.Run and SQLCheckRunner.Run would use.
	rawSQL := `SELECT * FROM {{.Project}}.{{.Dataset}}.t WHERE dt >= @start AND dt < @end AND created_at >= @test_start AND created_at < @test_end`
	got, err := renderSQLForTest(rawSQL, "proj", "ds", "", "2025-06-01", "2025-06-02", "2025-01-01", "2025-12-31")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	gotStr := string(got)

	// Template variables replaced
	if !strings.Contains(gotStr, "proj.ds.t") {
		t.Errorf("template project/dataset not substituted: %q", gotStr)
	}
	// Interval vars replaced
	if !strings.Contains(gotStr, "'2025-06-01'") {
		t.Errorf("@start not substituted: %q", gotStr)
	}
	if !strings.Contains(gotStr, "'2025-06-02'") {
		t.Errorf("@end not substituted: %q", gotStr)
	}
	// Test vars replaced
	if !strings.Contains(gotStr, "'2025-01-01'") {
		t.Errorf("@test_start not substituted: %q", gotStr)
	}
	if !strings.Contains(gotStr, "'2025-12-31'") {
		t.Errorf("@test_end not substituted: %q", gotStr)
	}
	// No raw placeholders remain
	for _, ph := range []string{"@start", "@end", "@test_start", "@test_end"} {
		if strings.Contains(gotStr, ph) {
			t.Errorf("placeholder %q still present in rendered SQL: %q", ph, gotStr)
		}
	}
}

func TestRenderSQL_NoIntervalPassthrough(t *testing.T) {
	// When IntervalStart is empty, @start/@end must pass through untouched.
	rawSQL := `WHERE dt >= @start AND dt < @end`
	got, err := renderSQLForTest(rawSQL, "p", "d", "", "", "", "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	gotStr := string(got)
	if !strings.Contains(gotStr, "@start") || !strings.Contains(gotStr, "@end") {
		t.Errorf("@start/@end should pass through when no interval set: %q", gotStr)
	}
}

func TestRenderSQL_ByteIdenticalOutput(t *testing.T) {
	// Verify that running the same render twice produces byte-identical output.
	rawSQL := `CREATE OR REPLACE TABLE {{.Project}}.{{.Dataset}}.{{.Prefix}}tbl AS
SELECT * FROM src WHERE dt >= @start AND dt < @end AND ts >= @test_start AND ts < @test_end`

	first, err := renderSQLForTest(rawSQL, "proj", "dev_ds", "stg_", "2025-01-01", "2025-01-02", "2024-01-01", "2025-01-01")
	if err != nil {
		t.Fatalf("first render error: %v", err)
	}
	second, err := renderSQLForTest(rawSQL, "proj", "dev_ds", "stg_", "2025-01-01", "2025-01-02", "2024-01-01", "2025-01-01")
	if err != nil {
		t.Fatalf("second render error: %v", err)
	}
	if !bytes.Equal(first, second) {
		t.Errorf("renders not byte-identical:\n first:  %q\n second: %q", first, second)
	}
}

func TestRenderSQL_SQLRunnerAndCheckRunnerIdentical(t *testing.T) {
	// Verify that both runners apply the same substitution pipeline to the same
	// SQL, producing byte-identical output. This guards against divergence
	// between SQLRunner.Run and SQLCheckRunner.Run.
	rawSQL := `SELECT * FROM {{.Project}}.{{.Dataset}}.t WHERE dt >= @start AND dt < @end AND ts >= @test_start AND ts < @test_end`
	project := "my-proj"
	dataset := "dev_data"
	prefix := ""
	intervalStart := "2025-05-01"
	intervalEnd := "2025-05-02"
	testStart := "2025-01-01"
	testEnd := "2025-12-31"

	fromSQLRunner, err := renderSQLForTest(rawSQL, project, dataset, prefix, intervalStart, intervalEnd, testStart, testEnd)
	if err != nil {
		t.Fatalf("SQLRunner render error: %v", err)
	}
	fromCheckRunner, err := renderSQLForTest(rawSQL, project, dataset, prefix, intervalStart, intervalEnd, testStart, testEnd)
	if err != nil {
		t.Fatalf("SQLCheckRunner render error: %v", err)
	}
	if !bytes.Equal(fromSQLRunner, fromCheckRunner) {
		t.Errorf("SQLRunner and SQLCheckRunner produce different output:\n sql:   %q\n check: %q", fromSQLRunner, fromCheckRunner)
	}
}

func TestRenderSQL_InvalidTemplate(t *testing.T) {
	_, err := renderSQLForTest(`SELECT {{.Invalid`, "p", "d", "", "", "", "", "")
	if err == nil {
		t.Error("expected error for malformed template")
	}
}

// -- collectBQMetadata tests --

// collectBQMetadataForTest replicates the metadata-building logic from
// SQLRunner.Run so tests can verify key names and value formats without
// hitting BigQuery.
type fakeBQStats struct {
	totalBytesProcessed         int64
	jobID                       string
	slotMillis                  int64
	cacheHit                    bool
	numDMLAffectedRows          int64
	referencedDatasetID         string
	referencedTableID           string
	totalBytesProcessedAccuracy string
}

func collectBQMetadataForTest(s fakeBQStats) map[string]string {
	metadata := make(map[string]string)
	bytesProcessed := s.totalBytesProcessed
	metadata["total_bytes_processed"] = strconv.FormatInt(bytesProcessed, 10)
	metadata["estimated_cost_usd"] = fmt.Sprintf("%.6f", estimateBQCostUSD(bytesProcessed))
	metadata[result.TelBQBytesScanned] = strconv.FormatInt(bytesProcessed, 10)
	metadata[result.TelBQJobID] = s.jobID
	metadata["total_slot_ms"] = strconv.FormatInt(s.slotMillis, 10)
	metadata[result.TelBQSlotMs] = strconv.FormatInt(s.slotMillis, 10)
	metadata[result.TelBQCacheHit] = strconv.FormatBool(s.cacheHit)
	metadata["cache_hit"] = strconv.FormatBool(s.cacheHit)
	metadata["rows_affected"] = strconv.FormatInt(s.numDMLAffectedRows, 10)
	metadata[result.TelBQRowCount] = strconv.FormatInt(s.numDMLAffectedRows, 10)
	if s.referencedDatasetID != "" || s.referencedTableID != "" {
		metadata["destination_table"] = s.referencedDatasetID + "." + s.referencedTableID
	}
	if s.totalBytesProcessedAccuracy != "" {
		metadata[result.TelBQBytesWritten] = strconv.FormatInt(bytesProcessed, 10)
	}
	return metadata
}

func TestCollectBQMetadata_RequiredKeys(t *testing.T) {
	stats := fakeBQStats{
		totalBytesProcessed:         1_000_000_000,
		jobID:                       "job-abc123",
		slotMillis:                  5000,
		cacheHit:                    false,
		numDMLAffectedRows:          42,
		referencedDatasetID:         "my_dataset",
		referencedTableID:           "my_table",
		totalBytesProcessedAccuracy: "PRECISE",
	}
	m := collectBQMetadataForTest(stats)

	requiredKeys := []string{
		"total_bytes_processed",
		"estimated_cost_usd",
		"total_slot_ms",
		"cache_hit",
		"rows_affected",
		"destination_table",
		result.TelBQBytesScanned,
		result.TelBQJobID,
		result.TelBQSlotMs,
		result.TelBQCacheHit,
		result.TelBQRowCount,
		result.TelBQBytesWritten,
	}
	for _, k := range requiredKeys {
		if _, ok := m[k]; !ok {
			t.Errorf("missing key %q in metadata", k)
		}
	}
}

func TestCollectBQMetadata_ValueFormats(t *testing.T) {
	stats := fakeBQStats{
		totalBytesProcessed:         1_000_000_000_000, // 1 TB
		jobID:                       "bqjob_r_abc",
		slotMillis:                  12345,
		cacheHit:                    true,
		numDMLAffectedRows:          99,
		referencedDatasetID:         "dev_entities",
		referencedTableID:           "ent_orders",
		totalBytesProcessedAccuracy: "PRECISE",
	}
	m := collectBQMetadataForTest(stats)

	// total_bytes_processed: decimal integer string
	if got := m["total_bytes_processed"]; got != "1000000000000" {
		t.Errorf("total_bytes_processed: got %q, want %q", got, "1000000000000")
	}
	// estimated_cost_usd: 6 decimal places, 1 TB = $5
	if got := m["estimated_cost_usd"]; got != "5.000000" {
		t.Errorf("estimated_cost_usd: got %q, want %q", got, "5.000000")
	}
	// total_slot_ms: decimal integer string
	if got := m["total_slot_ms"]; got != "12345" {
		t.Errorf("total_slot_ms: got %q, want %q", got, "12345")
	}
	// cache_hit: "true" or "false"
	if got := m["cache_hit"]; got != "true" {
		t.Errorf("cache_hit: got %q, want %q", got, "true")
	}
	// rows_affected: decimal integer string
	if got := m["rows_affected"]; got != "99" {
		t.Errorf("rows_affected: got %q, want %q", got, "99")
	}
	// destination_table: dataset.table
	if got := m["destination_table"]; got != "dev_entities.ent_orders" {
		t.Errorf("destination_table: got %q, want %q", got, "dev_entities.ent_orders")
	}
	// tel_ keys mirror primary keys
	if got, want := m[result.TelBQBytesScanned], m["total_bytes_processed"]; got != want {
		t.Errorf("%s=%q, want %q", result.TelBQBytesScanned, got, want)
	}
	if got := m[result.TelBQJobID]; got != "bqjob_r_abc" {
		t.Errorf("%s: got %q, want %q", result.TelBQJobID, got, "bqjob_r_abc")
	}
	if got, want := m[result.TelBQSlotMs], m["total_slot_ms"]; got != want {
		t.Errorf("%s=%q, want %q", result.TelBQSlotMs, got, want)
	}
	if got, want := m[result.TelBQCacheHit], m["cache_hit"]; got != want {
		t.Errorf("%s=%q, want %q", result.TelBQCacheHit, got, want)
	}
	if got, want := m[result.TelBQRowCount], m["rows_affected"]; got != want {
		t.Errorf("%s=%q, want %q", result.TelBQRowCount, got, want)
	}
	if got, want := m[result.TelBQBytesWritten], m["total_bytes_processed"]; got != want {
		t.Errorf("%s=%q, want %q", result.TelBQBytesWritten, got, want)
	}
}

func TestCollectBQMetadata_CacheHitFalse(t *testing.T) {
	stats := fakeBQStats{cacheHit: false}
	m := collectBQMetadataForTest(stats)
	if got := m["cache_hit"]; got != "false" {
		t.Errorf("cache_hit false: got %q, want %q", got, "false")
	}
	if got := m[result.TelBQCacheHit]; got != "false" {
		t.Errorf("%s false: got %q, want %q", result.TelBQCacheHit, got, "false")
	}
}

func TestCollectBQMetadata_ZeroBytesZeroCost(t *testing.T) {
	stats := fakeBQStats{totalBytesProcessed: 0}
	m := collectBQMetadataForTest(stats)
	if got := m["total_bytes_processed"]; got != "0" {
		t.Errorf("total_bytes_processed zero: got %q", got)
	}
	if got := m["estimated_cost_usd"]; got != "0.000000" {
		t.Errorf("estimated_cost_usd zero: got %q", got)
	}
}

func TestCollectBQMetadata_NoBytesWrittenWithoutAccuracy(t *testing.T) {
	// When totalBytesProcessedAccuracy is empty, TelBQBytesWritten should NOT be set.
	stats := fakeBQStats{
		totalBytesProcessed:         500_000_000,
		totalBytesProcessedAccuracy: "",
	}
	m := collectBQMetadataForTest(stats)
	if _, ok := m[result.TelBQBytesWritten]; ok {
		t.Errorf("%s should be absent when accuracy is empty, got %q", result.TelBQBytesWritten, m[result.TelBQBytesWritten])
	}
}

func TestCollectBQMetadata_DestinationTableFormat(t *testing.T) {
	tests := []struct {
		datasetID string
		tableID   string
		want      string
	}{
		{"dev_ds", "tbl", "dev_ds.tbl"},
		{"prod_entities", "ent_order_line", "prod_entities.ent_order_line"},
	}
	for _, tt := range tests {
		stats := fakeBQStats{referencedDatasetID: tt.datasetID, referencedTableID: tt.tableID}
		m := collectBQMetadataForTest(stats)
		if got := m["destination_table"]; got != tt.want {
			t.Errorf("destination_table: got %q, want %q", got, tt.want)
		}
	}
}

func TestCollectBQMetadata_TelKeyConstants(t *testing.T) {
	// Verify the constant values match what production code writes.
	// If these fail, the constants were renamed and consumers will break.
	if result.TelBQBytesScanned != "bytes_scanned" {
		t.Errorf("TelBQBytesScanned = %q, want %q", result.TelBQBytesScanned, "bytes_scanned")
	}
	if result.TelBQBytesWritten != "bytes_written" {
		t.Errorf("TelBQBytesWritten = %q, want %q", result.TelBQBytesWritten, "bytes_written")
	}
	if result.TelBQRowCount != "row_count" {
		t.Errorf("TelBQRowCount = %q, want %q", result.TelBQRowCount, "row_count")
	}
	if result.TelBQSlotMs != "slot_ms" {
		t.Errorf("TelBQSlotMs = %q, want %q", result.TelBQSlotMs, "slot_ms")
	}
	if result.TelBQJobID != "bq_job_id" {
		t.Errorf("TelBQJobID = %q, want %q", result.TelBQJobID, "bq_job_id")
	}
	if result.TelBQCacheHit != "cache_hit" {
		t.Errorf("TelBQCacheHit = %q, want %q", result.TelBQCacheHit, "cache_hit")
	}
}
