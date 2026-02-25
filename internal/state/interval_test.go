package state

import (
	"testing"
)

func TestGenerateIntervals_Day(t *testing.T) {
	ivs, err := GenerateIntervals("2025-01-10", "2025-01-13", "day")
	if err != nil {
		t.Fatal(err)
	}
	if len(ivs) != 3 {
		t.Fatalf("expected 3 intervals, got %d", len(ivs))
	}
	// Half-open: [Jan 10, Jan 11), [Jan 11, Jan 12), [Jan 12, Jan 13)
	if ivs[0].Start != "2025-01-10" || ivs[0].End != "2025-01-11" {
		t.Errorf("iv[0]: %v", ivs[0])
	}
	if ivs[2].Start != "2025-01-12" || ivs[2].End != "2025-01-13" {
		t.Errorf("iv[2]: %v", ivs[2])
	}
}

func TestGenerateIntervals_Hour(t *testing.T) {
	ivs, err := GenerateIntervals("2025-01-10", "2025-01-11", "hour")
	if err != nil {
		t.Fatal(err)
	}
	if len(ivs) != 24 {
		t.Fatalf("expected 24 intervals, got %d", len(ivs))
	}
	if ivs[0].Start != "2025-01-10T00:00:00" || ivs[0].End != "2025-01-10T01:00:00" {
		t.Errorf("iv[0]: %v", ivs[0])
	}
	if ivs[23].Start != "2025-01-10T23:00:00" || ivs[23].End != "2025-01-11T00:00:00" {
		t.Errorf("iv[23]: %v", ivs[23])
	}
}

func TestGenerateIntervals_Week(t *testing.T) {
	// 2025-01-06 is a Monday
	ivs, err := GenerateIntervals("2025-01-06", "2025-01-27", "week")
	if err != nil {
		t.Fatal(err)
	}
	if len(ivs) != 3 {
		t.Fatalf("expected 3 intervals, got %d", len(ivs))
	}
	if ivs[0].Start != "2025-01-06" || ivs[0].End != "2025-01-13" {
		t.Errorf("iv[0]: %v", ivs[0])
	}
}

func TestGenerateIntervals_Month(t *testing.T) {
	ivs, err := GenerateIntervals("2025-01-01", "2025-04-01", "month")
	if err != nil {
		t.Fatal(err)
	}
	if len(ivs) != 3 {
		t.Fatalf("expected 3 intervals, got %d", len(ivs))
	}
	if ivs[0].Start != "2025-01-01" || ivs[0].End != "2025-02-01" {
		t.Errorf("iv[0]: %v", ivs[0])
	}
	if ivs[2].Start != "2025-03-01" || ivs[2].End != "2025-04-01" {
		t.Errorf("iv[2]: %v", ivs[2])
	}
}

func TestGenerateIntervals_MonthBoundary(t *testing.T) {
	// Feb has 28 days in non-leap year
	ivs, err := GenerateIntervals("2025-01-31", "2025-04-01", "month")
	if err != nil {
		t.Fatal(err)
	}
	// Jan 31 + 1 month = Feb 28 (Go truncates), Feb 28 + 1 month = Mar 28
	if len(ivs) < 2 {
		t.Fatalf("expected at least 2, got %d", len(ivs))
	}
}

func TestGenerateIntervals_EmptyRange(t *testing.T) {
	ivs, err := GenerateIntervals("2025-01-10", "2025-01-10", "day")
	if err != nil {
		t.Fatal(err)
	}
	if len(ivs) != 0 {
		t.Errorf("expected 0, got %d", len(ivs))
	}
}

func TestGenerateIntervals_InvalidDate(t *testing.T) {
	_, err := GenerateIntervals("not-a-date", "2025-01-10", "day")
	if err == nil {
		t.Error("expected error")
	}
}

func TestComputeMissing_GapDetection(t *testing.T) {
	all := []Interval{
		{"2025-01-01", "2025-01-02"},
		{"2025-01-02", "2025-01-03"},
		{"2025-01-03", "2025-01-04"},
		{"2025-01-04", "2025-01-05"},
		{"2025-01-05", "2025-01-06"},
	}

	completed := []IntervalState{
		{IntervalStart: "2025-01-01", Status: "complete"},
		{IntervalStart: "2025-01-02", Status: "complete"},
		// gap: 2025-01-03 missing
		{IntervalStart: "2025-01-04", Status: "complete"},
		// 2025-01-05 missing
	}

	missing := ComputeMissing(all, completed, 0)
	if len(missing) != 2 {
		t.Fatalf("expected 2 missing, got %d: %v", len(missing), missing)
	}
	if missing[0].Start != "2025-01-03" || missing[1].Start != "2025-01-05" {
		t.Errorf("missing: %v", missing)
	}
}

func TestComputeMissing_FailedReprocessed(t *testing.T) {
	all := []Interval{
		{"2025-01-01", "2025-01-02"},
		{"2025-01-02", "2025-01-03"},
	}

	completed := []IntervalState{
		{IntervalStart: "2025-01-01", Status: "complete"},
		{IntervalStart: "2025-01-02", Status: "failed"},
	}

	missing := ComputeMissing(all, completed, 0)
	if len(missing) != 1 || missing[0].Start != "2025-01-02" {
		t.Errorf("expected failed interval in missing: %v", missing)
	}
}

func TestComputeMissing_InProgressReprocessed(t *testing.T) {
	all := []Interval{
		{"2025-01-01", "2025-01-02"},
	}

	completed := []IntervalState{
		{IntervalStart: "2025-01-01", Status: "in_progress"},
	}

	missing := ComputeMissing(all, completed, 0)
	if len(missing) != 1 {
		t.Errorf("in_progress should be reprocessed: %v", missing)
	}
}

func TestComputeMissing_Lookback(t *testing.T) {
	all := []Interval{
		{"2025-01-01", "2025-01-02"},
		{"2025-01-02", "2025-01-03"},
		{"2025-01-03", "2025-01-04"},
		{"2025-01-04", "2025-01-05"},
		{"2025-01-05", "2025-01-06"},
	}

	completed := []IntervalState{
		{IntervalStart: "2025-01-01", Status: "complete"},
		{IntervalStart: "2025-01-02", Status: "complete"},
		{IntervalStart: "2025-01-03", Status: "complete"},
	}

	// lookback=2: last 2 intervals should be reprocessed even if complete
	// Missing: 2025-01-04, 2025-01-05 (not complete)
	// Lookback from end: 2025-01-04, 2025-01-05 already in missing
	missing := ComputeMissing(all, completed, 2)

	// Should include 01-04, 01-05 (missing) + lookback reaches 01-04,01-05 (already there)
	if len(missing) != 2 {
		t.Fatalf("expected 2, got %d: %v", len(missing), missing)
	}

	// Now test with all complete
	allComplete := []IntervalState{
		{IntervalStart: "2025-01-01", Status: "complete"},
		{IntervalStart: "2025-01-02", Status: "complete"},
		{IntervalStart: "2025-01-03", Status: "complete"},
		{IntervalStart: "2025-01-04", Status: "complete"},
		{IntervalStart: "2025-01-05", Status: "complete"},
	}

	missing2 := ComputeMissing(all, allComplete, 2)
	// Lookback=2: last 2 intervals reprocessed
	if len(missing2) != 2 {
		t.Fatalf("expected 2 lookback intervals, got %d: %v", len(missing2), missing2)
	}
	if missing2[0].Start != "2025-01-04" || missing2[1].Start != "2025-01-05" {
		t.Errorf("lookback: %v", missing2)
	}
}

func TestApplyBatchSize(t *testing.T) {
	intervals := []Interval{
		{"2025-01-01", "2025-01-02"},
		{"2025-01-02", "2025-01-03"},
		{"2025-01-03", "2025-01-04"},
		{"2025-01-04", "2025-01-05"},
		{"2025-01-05", "2025-01-06"},
	}

	result := ApplyBatchSize(intervals, 3)
	if len(result) != 3 {
		t.Errorf("expected 3, got %d", len(result))
	}

	// batch_size=0 means unlimited
	result = ApplyBatchSize(intervals, 0)
	if len(result) != 5 {
		t.Errorf("expected 5 (unlimited), got %d", len(result))
	}

	// batch_size > len
	result = ApplyBatchSize(intervals, 10)
	if len(result) != 5 {
		t.Errorf("expected 5 (no truncation), got %d", len(result))
	}
}
