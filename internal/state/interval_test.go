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
	if ivs[0].Start != "2025-01-10T00:00:00Z" || ivs[0].End != "2025-01-11T00:00:00Z" {
		t.Errorf("iv[0]: %v", ivs[0])
	}
	if ivs[2].Start != "2025-01-12T00:00:00Z" || ivs[2].End != "2025-01-13T00:00:00Z" {
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
	if ivs[0].Start != "2025-01-10T00:00:00Z" || ivs[0].End != "2025-01-10T01:00:00Z" {
		t.Errorf("iv[0]: %v", ivs[0])
	}
	if ivs[23].Start != "2025-01-10T23:00:00Z" || ivs[23].End != "2025-01-11T00:00:00Z" {
		t.Errorf("iv[23]: %v", ivs[23])
	}
}

func TestGenerateIntervals_Week(t *testing.T) {
	ivs, err := GenerateIntervals("2025-01-06", "2025-01-27", "week")
	if err != nil {
		t.Fatal(err)
	}
	if len(ivs) != 3 {
		t.Fatalf("expected 3 intervals, got %d", len(ivs))
	}
	if ivs[0].Start != "2025-01-06T00:00:00Z" || ivs[0].End != "2025-01-13T00:00:00Z" {
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
	if ivs[0].Start != "2025-01-01T00:00:00Z" || ivs[0].End != "2025-02-01T00:00:00Z" {
		t.Errorf("iv[0]: %v", ivs[0])
	}
	if ivs[2].Start != "2025-03-01T00:00:00Z" || ivs[2].End != "2025-04-01T00:00:00Z" {
		t.Errorf("iv[2]: %v", ivs[2])
	}
}

func TestGenerateIntervals_MonthBoundary(t *testing.T) {
	ivs, err := GenerateIntervals("2025-01-31", "2025-04-01", "month")
	if err != nil {
		t.Fatal(err)
	}
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

func TestGenerateIntervals_DatetimeInput(t *testing.T) {
	ivs, err := GenerateIntervals("2025-01-10T00:00:00Z", "2025-01-13T00:00:00Z", "day")
	if err != nil {
		t.Fatal(err)
	}
	if len(ivs) != 3 {
		t.Fatalf("expected 3 intervals, got %d", len(ivs))
	}
	if ivs[0].Start != "2025-01-10T00:00:00Z" || ivs[0].End != "2025-01-11T00:00:00Z" {
		t.Errorf("iv[0]: %v", ivs[0])
	}
}

func TestComputeMissing_GapDetection(t *testing.T) {
	all := []Interval{
		{"2025-01-01T00:00:00Z", "2025-01-02T00:00:00Z"},
		{"2025-01-02T00:00:00Z", "2025-01-03T00:00:00Z"},
		{"2025-01-03T00:00:00Z", "2025-01-04T00:00:00Z"},
		{"2025-01-04T00:00:00Z", "2025-01-05T00:00:00Z"},
		{"2025-01-05T00:00:00Z", "2025-01-06T00:00:00Z"},
	}

	completed := []IntervalState{
		{IntervalStart: "2025-01-01T00:00:00Z", Status: "complete"},
		{IntervalStart: "2025-01-02T00:00:00Z", Status: "complete"},
		{IntervalStart: "2025-01-04T00:00:00Z", Status: "complete"},
	}

	missing := ComputeMissing(all, completed, 0)
	if len(missing) != 2 {
		t.Fatalf("expected 2 missing, got %d: %v", len(missing), missing)
	}
	if missing[0].Start != "2025-01-03T00:00:00Z" || missing[1].Start != "2025-01-05T00:00:00Z" {
		t.Errorf("missing: %v", missing)
	}
}

func TestComputeMissing_FailedReprocessed(t *testing.T) {
	all := []Interval{
		{"2025-01-01T00:00:00Z", "2025-01-02T00:00:00Z"},
		{"2025-01-02T00:00:00Z", "2025-01-03T00:00:00Z"},
	}

	completed := []IntervalState{
		{IntervalStart: "2025-01-01T00:00:00Z", Status: "complete"},
		{IntervalStart: "2025-01-02T00:00:00Z", Status: "failed"},
	}

	missing := ComputeMissing(all, completed, 0)
	if len(missing) != 1 || missing[0].Start != "2025-01-02T00:00:00Z" {
		t.Errorf("expected failed interval in missing: %v", missing)
	}
}

func TestComputeMissing_InProgressReprocessed(t *testing.T) {
	all := []Interval{
		{"2025-01-01T00:00:00Z", "2025-01-02T00:00:00Z"},
	}

	completed := []IntervalState{
		{IntervalStart: "2025-01-01T00:00:00Z", Status: "in_progress"},
	}

	missing := ComputeMissing(all, completed, 0)
	if len(missing) != 1 {
		t.Errorf("in_progress should be reprocessed: %v", missing)
	}
}

func TestComputeMissing_Lookback(t *testing.T) {
	all := []Interval{
		{"2025-01-01T00:00:00Z", "2025-01-02T00:00:00Z"},
		{"2025-01-02T00:00:00Z", "2025-01-03T00:00:00Z"},
		{"2025-01-03T00:00:00Z", "2025-01-04T00:00:00Z"},
		{"2025-01-04T00:00:00Z", "2025-01-05T00:00:00Z"},
		{"2025-01-05T00:00:00Z", "2025-01-06T00:00:00Z"},
	}

	completed := []IntervalState{
		{IntervalStart: "2025-01-01T00:00:00Z", Status: "complete"},
		{IntervalStart: "2025-01-02T00:00:00Z", Status: "complete"},
		{IntervalStart: "2025-01-03T00:00:00Z", Status: "complete"},
	}

	missing := ComputeMissing(all, completed, 2)
	if len(missing) != 2 {
		t.Fatalf("expected 2, got %d: %v", len(missing), missing)
	}

	allComplete := []IntervalState{
		{IntervalStart: "2025-01-01T00:00:00Z", Status: "complete"},
		{IntervalStart: "2025-01-02T00:00:00Z", Status: "complete"},
		{IntervalStart: "2025-01-03T00:00:00Z", Status: "complete"},
		{IntervalStart: "2025-01-04T00:00:00Z", Status: "complete"},
		{IntervalStart: "2025-01-05T00:00:00Z", Status: "complete"},
	}

	missing2 := ComputeMissing(all, allComplete, 2)
	if len(missing2) != 2 {
		t.Fatalf("expected 2 lookback intervals, got %d: %v", len(missing2), missing2)
	}
	if missing2[0].Start != "2025-01-04T00:00:00Z" || missing2[1].Start != "2025-01-05T00:00:00Z" {
		t.Errorf("lookback: %v", missing2)
	}
}

func TestApplyBatchSize(t *testing.T) {
	intervals := []Interval{
		{"2025-01-01T00:00:00Z", "2025-01-02T00:00:00Z"},
		{"2025-01-02T00:00:00Z", "2025-01-03T00:00:00Z"},
		{"2025-01-03T00:00:00Z", "2025-01-04T00:00:00Z"},
		{"2025-01-04T00:00:00Z", "2025-01-05T00:00:00Z"},
		{"2025-01-05T00:00:00Z", "2025-01-06T00:00:00Z"},
	}

	result := ApplyBatchSize(intervals, 3)
	if len(result) != 3 {
		t.Errorf("expected 3, got %d", len(result))
	}

	result = ApplyBatchSize(intervals, 0)
	if len(result) != 5 {
		t.Errorf("expected 5 (unlimited), got %d", len(result))
	}

	result = ApplyBatchSize(intervals, 10)
	if len(result) != 5 {
		t.Errorf("expected 5 (no truncation), got %d", len(result))
	}
}
