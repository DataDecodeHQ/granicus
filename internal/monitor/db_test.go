package monitor

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

func tempDBPath(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	return filepath.Join(dir, ".granicus", "monitor.db")
}

func queryCount(t *testing.T, dbPath, table string) int {
	t.Helper()
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db for count: %v", err)
	}
	defer db.Close()
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return count
}

func TestWriteCurrentErrors_CreatesDB(t *testing.T) {
	dbPath := tempDBPath(t)

	errors := []CurrentError{
		{Pipeline: "test_pipeline", Asset: "stg_orders", CheckName: "not_null", Severity: "error", Message: "null found", DetailsJSON: "{}", RunAt: "2026-02-26T00:00:00Z"},
	}

	if err := WriteCurrentErrors(dbPath, errors); err != nil {
		t.Fatalf("WriteCurrentErrors: %v", err)
	}

	if _, err := os.Stat(dbPath); err != nil {
		t.Fatalf("db file not created: %v", err)
	}

	if got := queryCount(t, dbPath, "current_errors"); got != 1 {
		t.Errorf("expected 1 row, got %d", got)
	}
}

func TestWriteCurrentErrors_ReplacesOnSecondCall(t *testing.T) {
	dbPath := tempDBPath(t)

	batch1 := []CurrentError{
		{Pipeline: "p", Asset: "a1", CheckName: "c1", Severity: "error", Message: "m1", DetailsJSON: "{}", RunAt: "2026-02-26T00:00:00Z"},
		{Pipeline: "p", Asset: "a2", CheckName: "c2", Severity: "warning", Message: "m2", DetailsJSON: "{}", RunAt: "2026-02-26T00:00:00Z"},
	}
	if err := WriteCurrentErrors(dbPath, batch1); err != nil {
		t.Fatalf("first write: %v", err)
	}
	if got := queryCount(t, dbPath, "current_errors"); got != 2 {
		t.Fatalf("after first write: expected 2, got %d", got)
	}

	batch2 := []CurrentError{
		{Pipeline: "p", Asset: "a3", CheckName: "c3", Severity: "error", Message: "m3", DetailsJSON: "{}", RunAt: "2026-02-26T01:00:00Z"},
	}
	if err := WriteCurrentErrors(dbPath, batch2); err != nil {
		t.Fatalf("second write: %v", err)
	}
	if got := queryCount(t, dbPath, "current_errors"); got != 1 {
		t.Errorf("after replace: expected 1, got %d", got)
	}
}

func TestWriteCurrentErrors_EmptySlice(t *testing.T) {
	dbPath := tempDBPath(t)

	if err := WriteCurrentErrors(dbPath, nil); err != nil {
		t.Fatalf("WriteCurrentErrors(nil): %v", err)
	}
	if got := queryCount(t, dbPath, "current_errors"); got != 0 {
		t.Errorf("expected 0, got %d", got)
	}
}

func TestAppendSnapshots(t *testing.T) {
	dbPath := tempDBPath(t)

	batch1 := []MetricSnapshot{
		{Pipeline: "p", TableName: "orders", ColumnName: "total", MetricName: "sum", MetricValue: 100.0, CapturedAt: "2026-02-26T00:00:00Z"},
	}
	if err := AppendSnapshots(dbPath, batch1); err != nil {
		t.Fatalf("first append: %v", err)
	}

	batch2 := []MetricSnapshot{
		{Pipeline: "p", TableName: "orders", ColumnName: "total", MetricName: "sum", MetricValue: 110.0, CapturedAt: "2026-02-27T00:00:00Z"},
	}
	if err := AppendSnapshots(dbPath, batch2); err != nil {
		t.Fatalf("second append: %v", err)
	}

	if got := queryCount(t, dbPath, "metric_snapshots"); got != 2 {
		t.Errorf("expected 2 accumulated rows, got %d", got)
	}
}

func TestAppendSnapshots_WithSegment(t *testing.T) {
	dbPath := tempDBPath(t)

	snapshots := []MetricSnapshot{
		{Pipeline: "p", TableName: "orders", ColumnName: "total", MetricName: "sum", MetricValue: 50.0, SegmentValue: "state=CA", CapturedAt: "2026-02-26T00:00:00Z"},
	}
	if err := AppendSnapshots(dbPath, snapshots); err != nil {
		t.Fatalf("AppendSnapshots: %v", err)
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	var seg string
	if err := db.QueryRow("SELECT segment_value FROM metric_snapshots").Scan(&seg); err != nil {
		t.Fatalf("query segment: %v", err)
	}
	if seg != "state=CA" {
		t.Errorf("expected segment_value=state=CA, got %q", seg)
	}
}

func TestAppendFlags_AppendsAndCopiesToErrors(t *testing.T) {
	dbPath := tempDBPath(t)

	flags := []DistributionFlag{
		{Pipeline: "p", TableName: "orders", ColumnName: "total", MetricName: "sum", Window: "week", CurrentValue: 110, PriorValue: 100, PctChange: 0.10, Severity: "warning", CapturedAt: "2026-02-26T00:00:00Z"},
	}
	if err := AppendFlags(dbPath, flags); err != nil {
		t.Fatalf("AppendFlags: %v", err)
	}

	if got := queryCount(t, dbPath, "distribution_flags"); got != 1 {
		t.Errorf("distribution_flags: expected 1, got %d", got)
	}
	if got := queryCount(t, dbPath, "current_errors"); got != 1 {
		t.Errorf("current_errors: expected 1 (from flag copy), got %d", got)
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	var checkName, severity string
	if err := db.QueryRow("SELECT check_name, severity FROM current_errors").Scan(&checkName, &severity); err != nil {
		t.Fatalf("query error row: %v", err)
	}
	if checkName != "distribution_orders_total_sum" {
		t.Errorf("check_name: got %q", checkName)
	}
	if severity != "warning" {
		t.Errorf("severity: got %q", severity)
	}
}

func TestAppendFlags_AccumulatesOverCalls(t *testing.T) {
	dbPath := tempDBPath(t)

	f1 := []DistributionFlag{
		{Pipeline: "p", TableName: "t", ColumnName: "c", MetricName: "m", Window: "day", CurrentValue: 1, PriorValue: 1, PctChange: 0, Severity: "warning", CapturedAt: "2026-02-25T00:00:00Z"},
	}
	f2 := []DistributionFlag{
		{Pipeline: "p", TableName: "t", ColumnName: "c", MetricName: "m", Window: "day", CurrentValue: 2, PriorValue: 1, PctChange: 1.0, Severity: "error", CapturedAt: "2026-02-26T00:00:00Z"},
	}

	if err := AppendFlags(dbPath, f1); err != nil {
		t.Fatalf("first: %v", err)
	}
	if err := AppendFlags(dbPath, f2); err != nil {
		t.Fatalf("second: %v", err)
	}

	if got := queryCount(t, dbPath, "distribution_flags"); got != 2 {
		t.Errorf("expected 2 accumulated flags, got %d", got)
	}
	if got := queryCount(t, dbPath, "current_errors"); got != 2 {
		t.Errorf("expected 2 error copies, got %d", got)
	}
}

func TestSchemaHasIndex(t *testing.T) {
	dbPath := tempDBPath(t)

	if err := AppendSnapshots(dbPath, nil); err != nil {
		t.Fatalf("create db: %v", err)
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_snapshots_lookup'").Scan(&count)
	if err != nil {
		t.Fatalf("query index: %v", err)
	}
	if count != 1 {
		t.Errorf("expected idx_snapshots_lookup index, found %d", count)
	}
}
