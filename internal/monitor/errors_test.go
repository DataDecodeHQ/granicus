package monitor

import (
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/Andrew-DataDecode/Granicus/internal/executor"
	_ "modernc.org/sqlite"
)

func TestCollectCheckErrors_FailedChecks(t *testing.T) {
	dbPath := tempDBPath(t)
	now := time.Now().UTC()

	run := &executor.RunResult{
		StartTime: now.Add(-time.Minute),
		EndTime:   now,
		Results: []executor.NodeResult{
			{AssetName: "stg_orders", Status: "success"},
			{AssetName: "check:stg_orders:not_null_id", Status: "failed", Error: "3 rows with null id", Stderr: "query returned 3 rows"},
			{AssetName: "check:stg_orders:unique_id", Status: "success"},
			{AssetName: "check:int_orders:row_count", Status: "failed", Error: "row count dropped 50%"},
		},
	}

	if err := CollectCheckErrors(dbPath, "legacy_dbt", run); err != nil {
		t.Fatalf("CollectCheckErrors: %v", err)
	}

	if got := queryCount(t, dbPath, "current_errors"); got != 2 {
		t.Fatalf("expected 2 errors, got %d", got)
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT pipeline, asset, check_name, severity, message, details_json FROM current_errors ORDER BY asset, check_name")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	type row struct {
		pipeline, asset, checkName, severity, message, details string
	}
	var got []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.pipeline, &r.asset, &r.checkName, &r.severity, &r.message, &r.details); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, r)
	}

	if got[0].pipeline != "legacy_dbt" {
		t.Errorf("pipeline: got %q", got[0].pipeline)
	}
	if got[0].asset != "int_orders" {
		t.Errorf("asset[0]: got %q, want int_orders", got[0].asset)
	}
	if got[0].checkName != "row_count" {
		t.Errorf("check_name[0]: got %q, want row_count", got[0].checkName)
	}
	if got[0].severity != "error" {
		t.Errorf("severity[0]: got %q, want error", got[0].severity)
	}

	if got[1].asset != "stg_orders" {
		t.Errorf("asset[1]: got %q, want stg_orders", got[1].asset)
	}
	if got[1].checkName != "not_null_id" {
		t.Errorf("check_name[1]: got %q, want not_null_id", got[1].checkName)
	}

	var details map[string]string
	if err := json.Unmarshal([]byte(got[1].details), &details); err != nil {
		t.Fatalf("unmarshal details: %v", err)
	}
	if details["stderr"] != "query returned 3 rows" {
		t.Errorf("details stderr: got %q", details["stderr"])
	}
}

func TestCollectCheckErrors_SkippedChecksAreWarnings(t *testing.T) {
	dbPath := tempDBPath(t)
	now := time.Now().UTC()

	run := &executor.RunResult{
		StartTime: now.Add(-time.Minute),
		EndTime:   now,
		Results: []executor.NodeResult{
			{AssetName: "stg_orders", Status: "failed", Error: "query error"},
			{AssetName: "check:stg_orders:not_null_id", Status: "skipped", Error: "skipped: dependency stg_orders failed"},
		},
	}

	if err := CollectCheckErrors(dbPath, "test_pipeline", run); err != nil {
		t.Fatalf("CollectCheckErrors: %v", err)
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	var severity string
	if err := db.QueryRow("SELECT severity FROM current_errors").Scan(&severity); err != nil {
		t.Fatalf("query: %v", err)
	}
	if severity != "warning" {
		t.Errorf("severity: got %q, want warning", severity)
	}
}

func TestCollectCheckErrors_NoFailures(t *testing.T) {
	dbPath := tempDBPath(t)
	now := time.Now().UTC()

	run := &executor.RunResult{
		StartTime: now.Add(-time.Minute),
		EndTime:   now,
		Results: []executor.NodeResult{
			{AssetName: "stg_orders", Status: "success"},
			{AssetName: "check:stg_orders:not_null_id", Status: "success"},
		},
	}

	if err := CollectCheckErrors(dbPath, "test_pipeline", run); err != nil {
		t.Fatalf("CollectCheckErrors: %v", err)
	}

	if got := queryCount(t, dbPath, "current_errors"); got != 0 {
		t.Errorf("expected 0 errors for clean run, got %d", got)
	}
}

func TestCollectCheckErrors_EmptyRun(t *testing.T) {
	dbPath := tempDBPath(t)
	now := time.Now().UTC()

	run := &executor.RunResult{
		StartTime: now,
		EndTime:   now,
	}

	if err := CollectCheckErrors(dbPath, "test_pipeline", run); err != nil {
		t.Fatalf("CollectCheckErrors: %v", err)
	}

	if got := queryCount(t, dbPath, "current_errors"); got != 0 {
		t.Errorf("expected 0 errors for empty run, got %d", got)
	}
}

func TestCollectCheckErrors_NonCheckNodesIgnored(t *testing.T) {
	dbPath := tempDBPath(t)
	now := time.Now().UTC()

	run := &executor.RunResult{
		StartTime: now.Add(-time.Minute),
		EndTime:   now,
		Results: []executor.NodeResult{
			{AssetName: "stg_orders", Status: "failed", Error: "query error"},
			{AssetName: "int_orders", Status: "skipped", Error: "skipped: dependency stg_orders failed"},
		},
	}

	if err := CollectCheckErrors(dbPath, "test_pipeline", run); err != nil {
		t.Fatalf("CollectCheckErrors: %v", err)
	}

	if got := queryCount(t, dbPath, "current_errors"); got != 0 {
		t.Errorf("expected 0 errors (non-check nodes), got %d", got)
	}
}

func TestParseCheckNode(t *testing.T) {
	tests := []struct {
		input     string
		asset     string
		checkName string
		ok        bool
	}{
		{"check:stg_orders:not_null_id", "stg_orders", "not_null_id", true},
		{"check:int_orders:row_count", "int_orders", "row_count", true},
		{"check:stg_orders:colon:in:name", "stg_orders", "colon:in:name", true},
		{"stg_orders", "", "", false},
		{"check:", "", "", false},
		{"check:asset_only", "", "", false},
	}

	for _, tt := range tests {
		asset, checkName, ok := parseCheckNode(tt.input)
		if ok != tt.ok {
			t.Errorf("parseCheckNode(%q): ok=%v, want %v", tt.input, ok, tt.ok)
			continue
		}
		if !ok {
			continue
		}
		if asset != tt.asset {
			t.Errorf("parseCheckNode(%q): asset=%q, want %q", tt.input, asset, tt.asset)
		}
		if checkName != tt.checkName {
			t.Errorf("parseCheckNode(%q): checkName=%q, want %q", tt.input, checkName, tt.checkName)
		}
	}
}

func TestCollectCheckErrors_ReplacesOnSecondRun(t *testing.T) {
	dbPath := tempDBPath(t)
	now := time.Now().UTC()

	run1 := &executor.RunResult{
		StartTime: now.Add(-2 * time.Minute),
		EndTime:   now.Add(-time.Minute),
		Results: []executor.NodeResult{
			{AssetName: "check:stg_orders:not_null_id", Status: "failed", Error: "3 nulls"},
			{AssetName: "check:stg_orders:unique_id", Status: "failed", Error: "2 dupes"},
		},
	}
	if err := CollectCheckErrors(dbPath, "p", run1); err != nil {
		t.Fatalf("first run: %v", err)
	}
	if got := queryCount(t, dbPath, "current_errors"); got != 2 {
		t.Fatalf("after run 1: expected 2, got %d", got)
	}

	run2 := &executor.RunResult{
		StartTime: now.Add(-time.Minute),
		EndTime:   now,
		Results: []executor.NodeResult{
			{AssetName: "check:stg_orders:not_null_id", Status: "success"},
			{AssetName: "check:stg_orders:unique_id", Status: "success"},
		},
	}
	if err := CollectCheckErrors(dbPath, "p", run2); err != nil {
		t.Fatalf("second run: %v", err)
	}
	if got := queryCount(t, dbPath, "current_errors"); got != 0 {
		t.Errorf("after clean run: expected 0, got %d", got)
	}
}

func TestCollectCheckErrors_WithMetadata(t *testing.T) {
	dbPath := tempDBPath(t)
	now := time.Now().UTC()

	run := &executor.RunResult{
		StartTime: now.Add(-time.Minute),
		EndTime:   now,
		Results: []executor.NodeResult{
			{
				AssetName: "check:stg_orders:completeness",
				Status:    "failed",
				Error:     "missing 5 rows",
				Metadata:  map[string]string{"failing_rows": "5", "check_sql": "SELECT count(*)..."},
			},
		},
	}

	if err := CollectCheckErrors(dbPath, "p", run); err != nil {
		t.Fatalf("CollectCheckErrors: %v", err)
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	var detailsStr string
	if err := db.QueryRow("SELECT details_json FROM current_errors").Scan(&detailsStr); err != nil {
		t.Fatalf("query: %v", err)
	}

	var details map[string]string
	if err := json.Unmarshal([]byte(detailsStr), &details); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if details["failing_rows"] != "5" {
		t.Errorf("failing_rows: got %q", details["failing_rows"])
	}
	if details["check_sql"] != "SELECT count(*)..." {
		t.Errorf("check_sql: got %q", details["check_sql"])
	}
}
