package testmode

import (
	"fmt"
	"strings"
)

type TestResult struct {
	AssetCount       int
	CheckCount       int
	FailedChecks     []FailedCheck
	RowCounts        map[string]int64
	MetadataFilePath string
	DatasetName      string
	DatasetDropped   bool
}

type FailedCheck struct {
	Name     string
	Error    string
	RowCount int
}

// Passed returns true if no checks failed during the test run.
func (r *TestResult) Passed() bool {
	return len(r.FailedChecks) == 0
}

// FormatSummary returns a human-readable summary of the test run including status, counts, and failures.
func FormatSummary(r *TestResult) string {
	var b strings.Builder

	b.WriteString("\n--- Test Run Summary ---\n")

	if r.Passed() {
		b.WriteString("Status: PASSED\n")
	} else {
		b.WriteString("Status: FAILED\n")
	}

	b.WriteString(fmt.Sprintf("Assets: %d\n", r.AssetCount))
	b.WriteString(fmt.Sprintf("Checks: %d", r.CheckCount))
	if !r.Passed() {
		b.WriteString(fmt.Sprintf(" (%d failed)", len(r.FailedChecks)))
	}
	b.WriteString("\n")

	if len(r.RowCounts) > 0 {
		b.WriteString("\nRow counts:\n")
		for name, count := range r.RowCounts {
			b.WriteString(fmt.Sprintf("  %s: %d\n", name, count))
		}
	}

	if !r.Passed() {
		b.WriteString("\nFailed checks:\n")
		for _, c := range r.FailedChecks {
			b.WriteString(fmt.Sprintf("  %s: %s", c.Name, c.Error))
			if c.RowCount > 0 {
				b.WriteString(fmt.Sprintf(" (%d rows)", c.RowCount))
			}
			b.WriteString("\n")
		}
	}

	if r.MetadataFilePath != "" {
		b.WriteString(fmt.Sprintf("\nMetadata: %s\n", r.MetadataFilePath))
	}

	b.WriteString(fmt.Sprintf("Dataset: %s", r.DatasetName))
	if r.DatasetDropped {
		b.WriteString(" (dropped)")
	} else {
		b.WriteString(" (preserved)")
	}
	b.WriteString("\n")

	return b.String()
}
