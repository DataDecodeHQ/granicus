package state

import (
	"fmt"
	"log/slog"
	"time"
)

const DefaultOrphanTimeout = 2 * time.Hour

// RecoverOrphans scans for intervals stuck in in_progress longer than threshold
// and resets them to pending. Returns the recovered intervals, which callers
// should use to emit interval_recovered events.
//
// If threshold is 0 or negative, DefaultOrphanTimeout is used.
// The query is efficient via idx_interval_state_status_started (status, started_at).
func (s *Store) RecoverOrphans(threshold time.Duration) ([]IntervalState, error) {
	if threshold <= 0 {
		threshold = DefaultOrphanTimeout
	}

	cutoff := time.Now().UTC().Add(-threshold).Format(time.RFC3339)

	rows, err := s.db.Query(`
		SELECT asset_name, interval_start, interval_end, status, run_id, started_at, completed_at
		FROM interval_state
		WHERE status = 'in_progress' AND started_at < ?
		ORDER BY asset_name, interval_start
	`, cutoff)
	if err != nil {
		return nil, fmt.Errorf("querying orphaned intervals: %w", err)
	}
	defer rows.Close()

	var orphans []IntervalState
	for rows.Next() {
		var iv IntervalState
		if err := rows.Scan(&iv.AssetName, &iv.IntervalStart, &iv.IntervalEnd, &iv.Status, &iv.RunID, &iv.StartedAt, &iv.CompletedAt); err != nil {
			return nil, fmt.Errorf("scanning orphan: %w", err)
		}
		orphans = append(orphans, iv)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(orphans) == 0 {
		return nil, nil
	}

	for _, iv := range orphans {
		slog.Warn("recovering orphaned interval", "asset", iv.AssetName, "interval", iv.IntervalStart, "run_id", iv.RunID, "started_at", iv.StartedAt)
		if _, err := s.db.Exec(`
			UPDATE interval_state SET status = 'pending'
			WHERE asset_name = ? AND interval_start = ? AND status = 'in_progress'
		`, iv.AssetName, iv.IntervalStart); err != nil {
			return nil, fmt.Errorf("resetting orphan %s/%s: %w", iv.AssetName, iv.IntervalStart, err)
		}
	}

	return orphans, nil
}
