package checker

import (
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/Andrew-DataDecode/Granicus/internal/events"
)

const (
	DefaultVolumeWindow    = 10
	DefaultAnomalyThreshold = 0.25

	VolumeCheckWarn   = "warn"
	VolumeCheckError  = "error"
	VolumeCheckIgnore = "ignore"
)

// VolumeCheckConfig holds configurable parameters for volume anomaly detection.
type VolumeCheckConfig struct {
	// WindowSize is the number of past runs to include in the rolling average.
	// Defaults to DefaultVolumeWindow (10) when zero.
	WindowSize int
	// AnomalyThreshold is the fractional deviation from the rolling average
	// that triggers an anomaly event (e.g. 0.25 = 25%). Defaults to
	// DefaultAnomalyThreshold (0.25) when zero.
	AnomalyThreshold float64
	// Response controls what happens when an anomaly is detected:
	// "warn" (default), "error" (returns error), or "ignore" (no-op).
	Response string
}

// VolumeCheckResult contains the outcome of a single volume check.
type VolumeCheckResult struct {
	Asset        string  `json:"asset"`
	RowCount     int64   `json:"row_count"`
	Average      float64 `json:"average"`
	DeviationPct float64 `json:"deviation_pct"`
	IsAnomaly    bool    `json:"is_anomaly"`
}

// CheckVolumeAnomaly records the current row count as a volume_snapshot event,
// then computes a rolling average over the last cfg.WindowSize runs and flags
// deviations beyond cfg.AnomalyThreshold as volume_anomaly events.
//
// On the first run (no prior history) it stores the snapshot and returns a
// result with no anomaly. When cfg.Response is "error" and an anomaly is
// detected, an error is returned alongside the result.
func CheckVolumeAnomaly(
	store *events.Store,
	pipeline, asset, runID string,
	rowCount int64,
	cfg VolumeCheckConfig,
) (*VolumeCheckResult, error) {
	if cfg.Response == VolumeCheckIgnore {
		return nil, nil
	}
	if cfg.Response == "" {
		cfg.Response = VolumeCheckWarn
	}
	if cfg.WindowSize <= 0 {
		cfg.WindowSize = DefaultVolumeWindow
	}
	if cfg.AnomalyThreshold <= 0 {
		cfg.AnomalyThreshold = DefaultAnomalyThreshold
	}

	// Load historical snapshots before storing the new one.
	history, err := loadVolumeHistory(store, asset, cfg.WindowSize)
	if err != nil {
		return nil, fmt.Errorf("loading volume history for %s: %w", asset, err)
	}

	// Store current snapshot.
	if err := storeVolumeSnapshot(store, pipeline, asset, runID, rowCount); err != nil {
		return nil, fmt.Errorf("storing volume snapshot for %s: %w", asset, err)
	}

	result := &VolumeCheckResult{
		Asset:    asset,
		RowCount: rowCount,
	}

	// No prior history — nothing to compare against.
	if len(history) == 0 {
		return result, nil
	}

	avg := rollingAverage(history)
	result.Average = avg

	if avg == 0 {
		// Avoid division by zero; treat as no anomaly.
		return result, nil
	}

	deviationPct := math.Abs(float64(rowCount)-avg) / avg
	result.DeviationPct = deviationPct

	if deviationPct <= cfg.AnomalyThreshold {
		return result, nil
	}

	result.IsAnomaly = true

	severity := "warning"
	if cfg.Response == VolumeCheckError {
		severity = "error"
	}

	if err := store.Emit(events.Event{
		RunID:     runID,
		Pipeline:  pipeline,
		Asset:     asset,
		EventType: "volume_anomaly",
		Severity:  severity,
		Timestamp: time.Now().UTC(),
		Summary: fmt.Sprintf(
			"Volume anomaly for %s: %d rows (avg %.0f, deviation %.1f%%)",
			asset, rowCount, avg, deviationPct*100,
		),
		Details: map[string]any{
			"row_count":     rowCount,
			"average":       avg,
			"deviation_pct": deviationPct * 100,
			"threshold_pct": cfg.AnomalyThreshold * 100,
			"window_size":   cfg.WindowSize,
		},
	}); err != nil {
		return result, fmt.Errorf("emitting volume_anomaly event: %w", err)
	}

	if cfg.Response == VolumeCheckError {
		return result, fmt.Errorf(
			"volume anomaly for asset %s: %.1f%% deviation exceeds threshold %.1f%%",
			asset, deviationPct*100, cfg.AnomalyThreshold*100,
		)
	}
	return result, nil
}

func loadVolumeHistory(store *events.Store, asset string, limit int) ([]int64, error) {
	// Query the most recent N snapshots (DESC order) using the underlying DB
	// so we don't pull unbounded history from assets with many runs.
	rows, err := store.DB().Query(`
		SELECT details FROM events
		WHERE asset = ? AND event_type = 'volume_snapshot'
		ORDER BY timestamp DESC LIMIT ?
	`, asset, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var counts []int64
	for rows.Next() {
		var detailsJSON string
		if err := rows.Scan(&detailsJSON); err != nil {
			return nil, err
		}
		var d struct {
			RowCount int64 `json:"row_count"`
		}
		if err := json.Unmarshal([]byte(detailsJSON), &d); err != nil {
			continue
		}
		counts = append(counts, d.RowCount)
	}
	return counts, rows.Err()
}

func storeVolumeSnapshot(store *events.Store, pipeline, asset, runID string, rowCount int64) error {
	return store.Emit(events.Event{
		RunID:     runID,
		Pipeline:  pipeline,
		Asset:     asset,
		EventType: "volume_snapshot",
		Severity:  "info",
		Timestamp: time.Now().UTC(),
		Summary:   fmt.Sprintf("Volume snapshot for %s: %d rows", asset, rowCount),
		Details: map[string]any{
			"row_count": rowCount,
		},
	})
}

func rollingAverage(counts []int64) float64 {
	if len(counts) == 0 {
		return 0
	}
	var sum float64
	for _, c := range counts {
		sum += float64(c)
	}
	return sum / float64(len(counts))
}

