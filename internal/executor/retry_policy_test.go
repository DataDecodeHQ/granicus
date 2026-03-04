package executor

import (
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/analytehealth/granicus/internal/graph"
	"github.com/analytehealth/granicus/internal/state"
)

// retryableErrMsg matches CategoryRateLimit so it is retryable by default.
const retryableErrMsg = "googleapi: Error 429: Too Many Requests, rateLimitExceeded"

// nonRetryableErrMsg does not match any category.
const nonRetryableErrMsg = "Error 400: Syntax error in query"

// TestRetry_NonRetryableFailsImmediately verifies that a non-retryable error
// is not retried regardless of max_attempts.
func TestRetry_NonRetryableFailsImmediately(t *testing.T) {
	var callCount int32

	g := buildTestGraph(t,
		[]graph.AssetInput{
			{
				Name:        "asset",
				Type:        "shell",
				Source:      "a.sh",
				MaxAttempts: 3,
				BackoffBase: time.Millisecond,
			},
		},
		nil,
	)

	runner := func(asset *graph.Asset, projectRoot string, runID string) NodeResult {
		atomic.AddInt32(&callCount, 1)
		return NodeResult{
			AssetName: asset.Name,
			Status:    "failed",
			Error:     nonRetryableErrMsg,
			ExitCode:  1,
		}
	}

	rr := Execute(g, RunConfig{MaxParallel: 1}, runner)

	if got := int(atomic.LoadInt32(&callCount)); got != 1 {
		t.Errorf("non-retryable error: expected 1 call, got %d", got)
	}
	if rm := resultMap(rr); rm["asset"] != "failed" {
		t.Errorf("asset status: %s", rm["asset"])
	}
}

// TestRetry_RetryableExhaustsMaxAttempts verifies that a retryable error is
// retried up to max_attempts times and then reports failure.
func TestRetry_RetryableExhaustsMaxAttempts(t *testing.T) {
	var callCount int32

	g := buildTestGraph(t,
		[]graph.AssetInput{
			{
				Name:        "asset",
				Type:        "shell",
				Source:      "a.sh",
				MaxAttempts: 3,
				BackoffBase: time.Millisecond,
			},
		},
		nil,
	)

	runner := func(asset *graph.Asset, projectRoot string, runID string) NodeResult {
		atomic.AddInt32(&callCount, 1)
		return NodeResult{
			AssetName: asset.Name,
			Status:    "failed",
			Error:     retryableErrMsg,
			ExitCode:  1,
		}
	}

	rr := Execute(g, RunConfig{MaxParallel: 1}, runner)

	if got := int(atomic.LoadInt32(&callCount)); got != 3 {
		t.Errorf("retryable error max_attempts=3: expected 3 calls, got %d", got)
	}
	if rm := resultMap(rr); rm["asset"] != "failed" {
		t.Errorf("asset status: %s", rm["asset"])
	}
}

// TestRetry_SucceedsOnSecondAttempt verifies retry succeeds when the runner
// recovers after the first failure.
func TestRetry_SucceedsOnSecondAttempt(t *testing.T) {
	var callCount int32

	g := buildTestGraph(t,
		[]graph.AssetInput{
			{
				Name:        "asset",
				Type:        "shell",
				Source:      "a.sh",
				MaxAttempts: 3,
				BackoffBase: time.Millisecond,
			},
		},
		nil,
	)

	runner := func(asset *graph.Asset, projectRoot string, runID string) NodeResult {
		n := atomic.AddInt32(&callCount, 1)
		if n == 1 {
			return NodeResult{
				AssetName: asset.Name,
				Status:    "failed",
				Error:     retryableErrMsg,
				ExitCode:  1,
			}
		}
		return NodeResult{
			AssetName: asset.Name,
			Status:    "success",
			ExitCode:  0,
		}
	}

	rr := Execute(g, RunConfig{MaxParallel: 1}, runner)

	if got := int(atomic.LoadInt32(&callCount)); got != 2 {
		t.Errorf("succeed on 2nd attempt: expected 2 calls, got %d", got)
	}
	if rm := resultMap(rr); rm["asset"] != "success" {
		t.Errorf("asset status: %s", rm["asset"])
	}
}

// TestRetry_CustomRetryableErrorsRespected verifies that per-asset
// retryable_errors controls which categories are retried.
// Only "network" is in the retryable list — rate_limit should NOT be retried.
func TestRetry_CustomRetryableErrorsRespected(t *testing.T) {
	var callCount int32

	g := buildTestGraph(t,
		[]graph.AssetInput{
			{
				Name:            "asset",
				Type:            "shell",
				Source:          "a.sh",
				MaxAttempts:     3,
				BackoffBase:     time.Millisecond,
				RetryableErrors: []string{"network"},
			},
		},
		nil,
	)

	runner := func(asset *graph.Asset, projectRoot string, runID string) NodeResult {
		atomic.AddInt32(&callCount, 1)
		return NodeResult{
			AssetName: asset.Name,
			Status:    "failed",
			Error:     retryableErrMsg, // rate_limit — not in policy
			ExitCode:  1,
		}
	}

	Execute(g, RunConfig{MaxParallel: 1}, runner)

	if got := int(atomic.LoadInt32(&callCount)); got != 1 {
		t.Errorf("rate_limit excluded from policy: expected 1 call, got %d", got)
	}
}

// TestRetry_IncrementalRetryableInterval verifies retry for incremental assets.
func TestRetry_IncrementalRetryableInterval(t *testing.T) {
	store := newRetryTestState(t)
	var callCount int32

	g := buildTestGraph(t,
		[]graph.AssetInput{
			{
				Name:         "inc",
				Type:         "shell",
				Source:       "a.sh",
				TimeColumn:   "dt",
				IntervalUnit: "day",
				StartDate:    "2025-01-01",
				MaxAttempts:  3,
				BackoffBase:  time.Millisecond,
			},
		},
		nil,
	)

	// First call for each interval fails with retryable error; subsequent succeed.
	failedIntervals := make(map[string]bool)
	runner := func(asset *graph.Asset, projectRoot string, runID string) NodeResult {
		atomic.AddInt32(&callCount, 1)
		key := asset.IntervalStart
		if !failedIntervals[key] {
			failedIntervals[key] = true
			return NodeResult{
				AssetName: asset.Name,
				Status:    "failed",
				Error:     retryableErrMsg,
				ExitCode:  1,
			}
		}
		return NodeResult{
			AssetName: asset.Name,
			Status:    "success",
			ExitCode:  0,
		}
	}

	rr := Execute(g, RunConfig{
		MaxParallel: 1,
		ProjectRoot: t.TempDir(),
		RunID:       "r1",
		ToDate:      "2025-01-03",
		StateStore:  store,
	}, runner)

	// 2 intervals; each fails once then succeeds: 4 total calls
	if got := int(atomic.LoadInt32(&callCount)); got != 4 {
		t.Errorf("incremental retry: expected 4 calls (2 retries + 2 success), got %d", got)
	}
	if rm := resultMap(rr); rm["inc"] != "success" {
		t.Errorf("inc status: %s", rm["inc"])
	}
}

func newRetryTestState(t *testing.T) *state.Store {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), ".granicus", "state.db")
	s, err := state.New(dbPath)
	if err != nil {
		t.Fatalf("creating store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}
