package state

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

type IntervalState struct {
	AssetName     string
	IntervalStart string
	IntervalEnd   string
	Status        string // "in_progress", "complete", "failed"
	RunID         string
	StartedAt     string
	CompletedAt   string
}

type Store struct {
	db *sql.DB
}

const schema = `
CREATE TABLE IF NOT EXISTS interval_state (
	asset_name     TEXT NOT NULL,
	interval_start TEXT NOT NULL,
	interval_end   TEXT NOT NULL,
	status         TEXT NOT NULL,
	run_id         TEXT NOT NULL,
	started_at     TEXT NOT NULL,
	completed_at   TEXT NOT NULL DEFAULT '',
	PRIMARY KEY (asset_name, interval_start)
);

CREATE INDEX IF NOT EXISTS idx_interval_state_status_started ON interval_state(status, started_at);
`

// New opens or creates a SQLite state store at the given path, initializing the schema if needed.
func New(dbPath string) (*Store, error) {
	dir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("creating state dir: %w", err)
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("opening state db: %w", err)
	}

	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("enabling WAL: %w", err)
	}

	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("creating schema: %w", err)
	}

	// Migrate date-only intervals to datetime format
	if err := migrateIntervalDatetime(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrating intervals to datetime: %w", err)
	}

	return &Store{db: db}, nil
}

func migrateIntervalDatetime(db *sql.DB) error {
	var version int
	_ = db.QueryRow("PRAGMA user_version").Scan(&version)
	if version >= 1 {
		return nil
	}
	_, err := db.Exec(`
		UPDATE interval_state SET interval_start = interval_start || 'T00:00:00Z' WHERE interval_start NOT LIKE '%T%';
		UPDATE interval_state SET interval_end = interval_end || 'T00:00:00Z' WHERE interval_end NOT LIKE '%T%';
		PRAGMA user_version = 1;
	`)
	return err
}

// Close closes the underlying SQLite database connection.
func (s *Store) Close() error {
	return s.db.Close()
}

// dag:boundary
func (s *Store) MarkInProgress(asset, start, end, runID string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.Exec(`
		INSERT INTO interval_state (asset_name, interval_start, interval_end, status, run_id, started_at)
		VALUES (?, ?, ?, 'in_progress', ?, ?)
		ON CONFLICT (asset_name, interval_start) DO UPDATE SET
			interval_end = excluded.interval_end,
			status = 'in_progress',
			run_id = excluded.run_id,
			started_at = excluded.started_at,
			completed_at = ''
	`, asset, start, end, runID, now)
	return err
}

// dag:boundary
func (s *Store) MarkComplete(asset, start, end string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.Exec(`
		UPDATE interval_state SET status = 'complete', completed_at = ?
		WHERE asset_name = ? AND interval_start = ?
	`, now, asset, start)
	return err
}

// dag:boundary
func (s *Store) MarkFailed(asset, start, end string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.Exec(`
		UPDATE interval_state SET status = 'failed', completed_at = ?
		WHERE asset_name = ? AND interval_start = ?
	`, now, asset, start)
	return err
}

// GetIntervals returns all interval states for the given asset, ordered by start time.
func (s *Store) GetIntervals(asset string) ([]IntervalState, error) {
	rows, err := s.db.Query(`
		SELECT asset_name, interval_start, interval_end, status, run_id, started_at, completed_at
		FROM interval_state
		WHERE asset_name = ?
		ORDER BY interval_start
	`, asset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []IntervalState
	for rows.Next() {
		var is IntervalState
		if err := rows.Scan(&is.AssetName, &is.IntervalStart, &is.IntervalEnd, &is.Status, &is.RunID, &is.StartedAt, &is.CompletedAt); err != nil {
			return nil, err
		}
		result = append(result, is)
	}
	return result, rows.Err()
}

// dag:boundary
func (s *Store) InvalidateAll(asset string) error {
	_, err := s.db.Exec(`DELETE FROM interval_state WHERE asset_name = ?`, asset)
	return err
}
