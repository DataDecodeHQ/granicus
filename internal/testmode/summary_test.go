package testmode

import (
	"strings"
	"testing"
)

func TestFormatSummary_Passed(t *testing.T) {
	r := &TestResult{
		AssetCount:       3,
		CheckCount:       5,
		RowCounts:        map[string]int64{"stg_orders": 100, "stg_payments": 50},
		MetadataFilePath: "/tmp/metadata.json",
		DatasetName:      "dev__test_abcd",
		DatasetDropped:   true,
	}

	s := FormatSummary(r)
	if !strings.Contains(s, "PASSED") {
		t.Error("should show PASSED")
	}
	if !strings.Contains(s, "Assets: 3") {
		t.Error("should show asset count")
	}
	if !strings.Contains(s, "(dropped)") {
		t.Error("should show dropped")
	}
}

func TestFormatSummary_Failed(t *testing.T) {
	r := &TestResult{
		AssetCount: 3,
		CheckCount: 5,
		FailedChecks: []FailedCheck{
			{Name: "check:stg_orders:unique_grain", Error: "2 rows returned", RowCount: 2},
		},
		DatasetName:    "dev__test_abcd",
		DatasetDropped: false,
	}

	s := FormatSummary(r)
	if !strings.Contains(s, "FAILED") {
		t.Error("should show FAILED")
	}
	if !strings.Contains(s, "(1 failed)") {
		t.Error("should show failed count")
	}
	if !strings.Contains(s, "(preserved)") {
		t.Error("should show preserved")
	}
	if !strings.Contains(s, "unique_grain") {
		t.Error("should show failed check name")
	}
}

func TestTestResult_Passed(t *testing.T) {
	r := &TestResult{}
	if !r.Passed() {
		t.Error("no failed checks = passed")
	}

	r.FailedChecks = []FailedCheck{{Name: "x"}}
	if r.Passed() {
		t.Error("has failed checks = not passed")
	}
}
