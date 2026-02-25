package testmode

import (
	"strings"
	"testing"
)

func TestTestDatasetName_Deterministic(t *testing.T) {
	name1 := TestDatasetName("dev_analytics", "run-20260225-abcd")
	name2 := TestDatasetName("dev_analytics", "run-20260225-abcd")
	if name1 != name2 {
		t.Errorf("not deterministic: %q vs %q", name1, name2)
	}
	if name1 != "dev_analytics__test_abcd" {
		t.Errorf("unexpected name: %q", name1)
	}
}

func TestTestDatasetName_ValidBQIdentifier(t *testing.T) {
	name := TestDatasetName("my_ds", "run-20260225-wxyz")
	for _, c := range name {
		if !((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_') {
			t.Errorf("invalid BQ identifier char %q in %q", string(c), name)
		}
	}
	if len(name) > 1024 {
		t.Errorf("name too long: %d", len(name))
	}
}

func TestFormatSummary_FullWorkflow_Pass(t *testing.T) {
	result := &TestResult{
		AssetCount:       2,
		CheckCount:       4,
		RowCounts:        map[string]int64{"stg_orders": 150, "stg_payments": 75},
		MetadataFilePath: ".granicus/test-metadata.json",
		DatasetName:      "dev__test_abcd",
		DatasetDropped:   true,
	}

	s := FormatSummary(result)
	if !strings.Contains(s, "PASSED") {
		t.Error("should show PASSED")
	}
	if !strings.Contains(s, "(dropped)") {
		t.Error("should show dropped on pass")
	}
	if !strings.Contains(s, "test-metadata.json") {
		t.Error("should show metadata path")
	}
}

func TestFormatSummary_FullWorkflow_Fail(t *testing.T) {
	result := &TestResult{
		AssetCount: 2,
		CheckCount: 4,
		FailedChecks: []FailedCheck{
			{Name: "check:stg_orders:default:unique_grain", Error: "3 rows returned", RowCount: 3},
		},
		RowCounts:        map[string]int64{"stg_orders": 150},
		MetadataFilePath: ".granicus/test-metadata.json",
		DatasetName:      "dev__test_abcd",
		DatasetDropped:   false,
	}

	s := FormatSummary(result)
	if !strings.Contains(s, "FAILED") {
		t.Error("should show FAILED")
	}
	if !strings.Contains(s, "(preserved)") {
		t.Error("should preserve dataset on fail")
	}
	if !strings.Contains(s, "unique_grain") {
		t.Error("should list failed check")
	}
}

func TestFormatSummary_KeepTestData(t *testing.T) {
	result := &TestResult{
		AssetCount:     1,
		CheckCount:     2,
		DatasetName:    "dev__test_abcd",
		DatasetDropped: false,
	}

	s := FormatSummary(result)
	if !strings.Contains(s, "(preserved)") {
		t.Error("should show preserved for keep-test-data")
	}
}

func TestSanitizeLabel_RunIDFormat(t *testing.T) {
	// Verify the format used for dataset labels
	label := sanitizeLabel("run-20260225-154530-abcd")
	if strings.Contains(label, "-") {
		t.Errorf("label should not contain hyphens: %q", label)
	}
	if len(label) > 63 {
		t.Errorf("label too long: %d", len(label))
	}
}
