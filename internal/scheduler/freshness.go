package scheduler

import (
	"fmt"
	"time"

	"github.com/Andrew-DataDecode/Granicus/internal/state"
)

type FreshnessCheck struct {
	UpstreamAsset string
	FreshnessWindow time.Duration
}

type FreshnessResult struct {
	Asset   string
	Fresh   bool
	LastRun time.Time
	MaxAge  time.Duration
	Message string
}

func CheckFreshness(stateStore *state.Store, checks []FreshnessCheck) []FreshnessResult {
	var results []FreshnessResult

	for _, check := range checks {
		intervals, err := stateStore.GetIntervals(check.UpstreamAsset)
		if err != nil || len(intervals) == 0 {
			results = append(results, FreshnessResult{
				Asset:   check.UpstreamAsset,
				Fresh:   false,
				MaxAge:  check.FreshnessWindow,
				Message: fmt.Sprintf("no completed intervals found for %s", check.UpstreamAsset),
			})
			continue
		}

		// Find the most recent completed interval
		var lastComplete time.Time
		for _, iv := range intervals {
			if iv.Status != "complete" || iv.CompletedAt == "" {
				continue
			}
			t, err := time.Parse(time.RFC3339, iv.CompletedAt)
			if err != nil {
				continue
			}
			if t.After(lastComplete) {
				lastComplete = t
			}
		}

		if lastComplete.IsZero() {
			results = append(results, FreshnessResult{
				Asset:   check.UpstreamAsset,
				Fresh:   false,
				MaxAge:  check.FreshnessWindow,
				Message: fmt.Sprintf("no completed intervals for %s", check.UpstreamAsset),
			})
			continue
		}

		age := time.Since(lastComplete)
		fresh := age <= check.FreshnessWindow

		result := FreshnessResult{
			Asset:   check.UpstreamAsset,
			Fresh:   fresh,
			LastRun: lastComplete,
			MaxAge:  check.FreshnessWindow,
		}

		if !fresh {
			result.Message = fmt.Sprintf("%s is stale: last run %s ago (max %s)",
				check.UpstreamAsset, age.Round(time.Minute), check.FreshnessWindow)
		}

		results = append(results, result)
	}

	return results
}

func ParseFreshnessWindow(s string) (time.Duration, error) {
	if len(s) < 2 {
		return 0, fmt.Errorf("invalid freshness window: %q", s)
	}

	numStr := s[:len(s)-1]
	unit := s[len(s)-1]

	num := 0
	for _, c := range numStr {
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("invalid freshness window: %q", s)
		}
		num = num*10 + int(c-'0')
	}

	switch unit {
	case 'h':
		return time.Duration(num) * time.Hour, nil
	case 'd':
		return time.Duration(num) * 24 * time.Hour, nil
	default:
		return 0, fmt.Errorf("invalid freshness unit %q (use h or d)", string(unit))
	}
}
