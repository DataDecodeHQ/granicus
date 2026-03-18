package state

import "time"

// StateBackend abstracts interval state storage. The existing SQLite-backed
// Store is the first implementation; Firestore follows in Phase 3.
type StateBackend interface {
	// MarkInProgress records that an asset's interval is being processed.
	MarkInProgress(asset, start, end, runID string) error

	// MarkComplete records successful completion of an asset's interval.
	MarkComplete(asset, start, end string) error

	// MarkFailed records that an asset's interval processing failed.
	MarkFailed(asset, start, end string) error

	// GetIntervals returns all tracked intervals for an asset, ordered by start.
	GetIntervals(asset string) ([]IntervalState, error)

	// InvalidateAll removes all interval state for an asset (full refresh).
	InvalidateAll(asset string) error

	// RecoverOrphans finds intervals stuck in_progress beyond threshold and
	// resets them. Returns the recovered intervals for event emission.
	// A zero or negative threshold uses DefaultOrphanTimeout.
	RecoverOrphans(threshold time.Duration) ([]IntervalState, error)

	// Close releases resources held by the backend.
	Close() error
}

// Verify Store implements StateBackend at compile time.
var _ StateBackend = (*Store)(nil)
