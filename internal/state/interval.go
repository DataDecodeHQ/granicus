package state

import (
	"fmt"
	"time"
)

type Interval struct {
	Start string
	End   string
}

func parseFlexibleDate(s string) (time.Time, error) {
	if t, err := time.Parse("2006-01-02T15:04:05Z", s); err == nil {
		return t, nil
	}
	if t, err := time.Parse("2006-01-02T15:04:05", s); err == nil {
		return t, nil
	}
	return time.Parse("2006-01-02", s)
}

// GenerateIntervals produces a sequence of time intervals between startDate and endDate using the given unit (hour, day, week, month).
func GenerateIntervals(startDate, endDate, unit string) ([]Interval, error) {
	start, err := parseFlexibleDate(startDate)
	if err != nil {
		return nil, fmt.Errorf("parsing start_date %q: %w", startDate, err)
	}
	end, err := parseFlexibleDate(endDate)
	if err != nil {
		return nil, fmt.Errorf("parsing end_date %q: %w", endDate, err)
	}

	if !end.After(start) {
		return nil, nil
	}

	var intervals []Interval
	cur := start

	for cur.Before(end) {
		next := advance(cur, unit)
		if next.After(end) {
			next = end
		}
		intervals = append(intervals, Interval{
			Start: formatTime(cur, unit),
			End:   formatTime(next, unit),
		})
		cur = next
	}

	return intervals, nil
}

func advance(t time.Time, unit string) time.Time {
	switch unit {
	case "hour":
		return t.Add(time.Hour)
	case "day":
		return t.AddDate(0, 0, 1)
	case "week":
		return t.AddDate(0, 0, 7)
	case "month":
		return t.AddDate(0, 1, 0)
	default:
		return t.AddDate(0, 0, 1)
	}
}

func formatTime(t time.Time, unit string) string {
	return t.Format("2006-01-02T15:04:05Z")
}

// ComputeMissing returns intervals that are not yet complete, including the last N completed intervals if lookback is set.
func ComputeMissing(allIntervals []Interval, completed []IntervalState, lookback int) []Interval {
	completeSet := make(map[string]bool)
	for _, c := range completed {
		if c.Status == "complete" {
			completeSet[c.IntervalStart] = true
		}
	}

	// Find all incomplete intervals (missing, failed, or in_progress)
	var missing []Interval
	for _, iv := range allIntervals {
		if !completeSet[iv.Start] {
			missing = append(missing, iv)
		}
	}

	// Apply lookback: ensure last N completed intervals are reprocessed
	if lookback > 0 && len(allIntervals) > 0 {
		lookbackStart := len(allIntervals) - lookback
		if lookbackStart < 0 {
			lookbackStart = 0
		}

		lookbackSet := make(map[string]bool)
		for _, iv := range missing {
			lookbackSet[iv.Start] = true
		}

		for i := lookbackStart; i < len(allIntervals); i++ {
			iv := allIntervals[i]
			if !lookbackSet[iv.Start] {
				missing = append(missing, iv)
				lookbackSet[iv.Start] = true
			}
		}

		// Re-sort by start
		sortIntervals(missing)
	}

	return missing
}

// ApplyBatchSize truncates the interval list to at most batchSize entries.
func ApplyBatchSize(intervals []Interval, batchSize int) []Interval {
	if batchSize <= 0 || len(intervals) <= batchSize {
		return intervals
	}
	return intervals[:batchSize]
}

func sortIntervals(intervals []Interval) {
	for i := 1; i < len(intervals); i++ {
		for j := i; j > 0 && intervals[j].Start < intervals[j-1].Start; j-- {
			intervals[j], intervals[j-1] = intervals[j-1], intervals[j]
		}
	}
}
