package runner

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/DataDecodeHQ/granicus/internal/config"
)

func TestSQLRunner_TemplateSubstitution(t *testing.T) {
	dir := t.TempDir()
	sqlContent := `CREATE OR REPLACE TABLE {{.Project}}.{{.Dataset}}.test AS SELECT 1`
	os.WriteFile(filepath.Join(dir, "test.sql"), []byte(sqlContent), 0644)

	conn := &config.ConnectionConfig{
		Name: "bq",
		Type: "bigquery",
		Properties: map[string]string{
			"project": "my-project",
			"dataset": "my_dataset",
		},
	}
	runner := NewSQLRunner(conn)

	// We can't test actual BQ execution in unit tests, but we can test
	// that it reads the file and attempts to render the template.
	// The BQ client creation will fail without credentials, which is expected.
	result := runner.Run(&Asset{Name: "test", Type: "sql", Source: "test.sql"}, dir, "run1")

	// It should fail at BQ client creation (no credentials), but the template should have been rendered
	if result.Status != "failed" {
		t.Logf("result: %+v", result)
	}
	// The error should be about BQ, not about template parsing
	if strings.Contains(result.Error, "template") {
		t.Errorf("should not have template error: %s", result.Error)
	}
}

func TestSQLRunner_MissingFile(t *testing.T) {
	conn := &config.ConnectionConfig{
		Name:       "bq",
		Type:       "bigquery",
		Properties: map[string]string{"project": "p", "dataset": "d"},
	}
	runner := NewSQLRunner(conn)
	result := runner.Run(&Asset{Name: "missing", Type: "sql", Source: "nonexistent.sql"}, "/tmp", "run1")
	if result.Status != "failed" {
		t.Errorf("expected failed, got %s", result.Status)
	}
	if !strings.Contains(result.Error, "reading SQL") {
		t.Errorf("expected file read error: %s", result.Error)
	}
}

func TestSubstituteIntervalVarsForDDL_DayInterval(t *testing.T) {
	sql := []byte(`SELECT * FROM tbl WHERE dt >= @start AND dt < @end`)
	asset := &Asset{IntervalStart: "2025-01-10", IntervalEnd: "2025-01-11"}

	result := string(substituteIntervalVarsForDDL(sql, asset))
	if !strings.Contains(result, "'2025-01-10'") {
		t.Errorf("@start not replaced: %s", result)
	}
	if !strings.Contains(result, "'2025-01-11'") {
		t.Errorf("@end not replaced: %s", result)
	}
	if strings.Contains(result, "@start") || strings.Contains(result, "@end") {
		t.Errorf("@ placeholders still present: %s", result)
	}
}

func TestSubstituteIntervalVarsForDDL_HourInterval(t *testing.T) {
	sql := []byte(`WHERE ts >= @start AND ts < @end`)
	asset := &Asset{IntervalStart: "2025-01-10T00:00:00", IntervalEnd: "2025-01-10T01:00:00"}

	result := string(substituteIntervalVarsForDDL(sql, asset))
	if !strings.Contains(result, "'2025-01-10T00:00:00'") {
		t.Errorf("@start not replaced: %s", result)
	}
}

func TestSubstituteIntervalVarsForDDL_NoInterval(t *testing.T) {
	sql := []byte(`SELECT * FROM tbl WHERE dt >= @start AND dt < @end`)
	asset := &Asset{}

	result := string(substituteIntervalVarsForDDL(sql, asset))
	// With empty interval, @start/@end should pass through unchanged
	if !strings.Contains(result, "@start") || !strings.Contains(result, "@end") {
		t.Errorf("should pass through without interval: %s", result)
	}
}

func TestSubstituteIntervalVarsForDDL_MultipleOccurrences(t *testing.T) {
	sql := []byte(`SELECT @start as s1, @start as s2, @end as e1, @end as e2`)
	asset := &Asset{IntervalStart: "2025-01-10", IntervalEnd: "2025-01-11"}

	result := string(substituteIntervalVarsForDDL(sql, asset))
	if strings.Count(result, "'2025-01-10'") != 2 {
		t.Errorf("@start should be replaced twice: %s", result)
	}
	if strings.Count(result, "'2025-01-11'") != 2 {
		t.Errorf("@end should be replaced twice: %s", result)
	}
}

func TestIsMultiStatementScript(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want bool
	}{
		{
			name: "drop prefix",
			sql:  "DROP TABLE IF EXISTS `p.d.t`;\nCREATE OR REPLACE TABLE `p.d.t` AS SELECT 1",
			want: true,
		},
		{
			name: "create only",
			sql:  "CREATE OR REPLACE TABLE `p.d.t` AS SELECT 1",
			want: false,
		},
		{
			name: "plain select",
			sql:  "SELECT * FROM t WHERE dt >= @start",
			want: false,
		},
		{
			name: "leading whitespace before drop",
			sql:  "  DROP TABLE IF EXISTS `p.d.t`;\nSELECT 1",
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isMultiStatementScript([]byte(tt.sql))
			if got != tt.want {
				t.Errorf("isMultiStatementScript(%q) = %v, want %v", tt.sql, got, tt.want)
			}
		})
	}
}

func TestSubstituteTestVars(t *testing.T) {
	sql := []byte(`WHERE created_at >= @test_start AND created_at < @test_end`)

	// With values
	result := string(SubstituteTestVars(sql, "2025-12-01", "2025-12-08"))
	if !strings.Contains(result, "'2025-12-01'") {
		t.Errorf("@test_start not replaced: %s", result)
	}
	if !strings.Contains(result, "'2025-12-08'") {
		t.Errorf("@test_end not replaced: %s", result)
	}

	// With empty values (defaults)
	result = string(SubstituteTestVars(sql, "", ""))
	if !strings.Contains(result, "'1900-01-01'") {
		t.Errorf("@test_start should default to 1900-01-01: %s", result)
	}
	if !strings.Contains(result, "'2099-12-31'") {
		t.Errorf("@test_end should default to 2099-12-31: %s", result)
	}
}

func TestParseTestWindow(t *testing.T) {
	// Empty = no-op defaults
	start, end, err := ParseTestWindow("")
	if err != nil {
		t.Fatal(err)
	}
	if start != "1900-01-01" || end != "2099-12-31" {
		t.Errorf("empty window: %s to %s", start, end)
	}

	// Days
	start, end, err = ParseTestWindow("7d")
	if err != nil {
		t.Fatal(err)
	}
	if start == "" || end == "" {
		t.Error("7d returned empty dates")
	}
	if start >= end {
		t.Errorf("start %s should be before end %s", start, end)
	}

	// Weeks
	start, _, err = ParseTestWindow("4w")
	if err != nil {
		t.Fatal(err)
	}
	if start == "" {
		t.Error("4w returned empty start")
	}

	// Months
	start, _, err = ParseTestWindow("3m")
	if err != nil {
		t.Fatal(err)
	}
	if start == "" {
		t.Error("3m returned empty start")
	}

	// Invalid
	_, _, err = ParseTestWindow("x")
	if err == nil {
		t.Error("expected error for 'x'")
	}

	_, _, err = ParseTestWindow("7x")
	if err == nil {
		t.Error("expected error for '7x'")
	}

	_, _, err = ParseTestWindow("0d")
	if err == nil {
		t.Error("expected error for '0d'")
	}
}

func TestPrependDropForReplace(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantDrop bool
		table    string
	}{
		{
			name:     "standard CREATE OR REPLACE",
			input:    "CREATE OR REPLACE TABLE `my-project.my_dataset.my_table` AS SELECT 1",
			wantDrop: true,
			table:    "my-project.my_dataset.my_table",
		},
		{
			name:     "lowercase",
			input:    "create or replace table `proj.ds.tbl` AS SELECT 1",
			wantDrop: true,
			table:    "proj.ds.tbl",
		},
		{
			name:     "no CREATE OR REPLACE",
			input:    "SELECT * FROM `my-project.ds.tbl`",
			wantDrop: false,
		},
		{
			name:     "plain CREATE TABLE (no REPLACE)",
			input:    "CREATE TABLE `proj.ds.tbl` AS SELECT 1",
			wantDrop: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := string(prependDropForReplace([]byte(tt.input)))
			hasDrop := strings.Contains(result, "DROP TABLE IF EXISTS")
			if hasDrop != tt.wantDrop {
				t.Errorf("wantDrop=%v, got=%v\nresult: %s", tt.wantDrop, hasDrop, result)
			}
			if tt.wantDrop && !strings.Contains(result, tt.table) {
				t.Errorf("expected table ref %q in drop statement: %s", tt.table, result)
			}
			if tt.wantDrop {
				// Original SQL should still be present
				if !strings.Contains(result, tt.input) {
					t.Errorf("original SQL missing from result: %s", result)
				}
			}
		})
	}
}

func TestEstimateBQCostUSD(t *testing.T) {
	tests := []struct {
		bytes   int64
		wantUSD float64
	}{
		{0, 0.0},
		{1_000_000_000_000, 5.0},    // 1 TB = $5
		{100_000_000_000, 0.5},      // 100 GB = $0.50
		{5_000_000_000_000, 25.0},   // 5 TB = $25
	}
	for _, tt := range tests {
		got := estimateBQCostUSD(tt.bytes)
		if got != tt.wantUSD {
			t.Errorf("estimateBQCostUSD(%d) = %v, want %v", tt.bytes, got, tt.wantUSD)
		}
	}
}

func TestSQLRunner_BadTemplate(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "bad.sql"), []byte(`SELECT {{.Invalid`), 0644)

	conn := &config.ConnectionConfig{
		Name:       "bq",
		Type:       "bigquery",
		Properties: map[string]string{"project": "p", "dataset": "d"},
	}
	runner := NewSQLRunner(conn)
	result := runner.Run(&Asset{Name: "bad", Type: "sql", Source: "bad.sql"}, dir, "run1")
	if result.Status != "failed" {
		t.Errorf("expected failed, got %s", result.Status)
	}
	if !strings.Contains(result.Error, "template") {
		t.Errorf("expected template error: %s", result.Error)
	}
}
