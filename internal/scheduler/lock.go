package scheduler

import (
	"database/sql"
	"time"

	_ "modernc.org/sqlite"
)

type LockStore struct {
	db *sql.DB
}

const lockSchema = `
CREATE TABLE IF NOT EXISTS pipeline_locks (
	pipeline_name TEXT PRIMARY KEY,
	run_id        TEXT NOT NULL,
	started_at    TEXT NOT NULL,
	status        TEXT NOT NULL DEFAULT 'running'
);
`

func NewLockStore(db *sql.DB) (*LockStore, error) {
	if _, err := db.Exec(lockSchema); err != nil {
		return nil, err
	}
	return &LockStore{db: db}, nil
}

func (s *LockStore) AcquireLock(pipeline, runID string) (bool, error) {
	now := time.Now().UTC().Format(time.RFC3339)

	// Check if already locked
	var status string
	err := s.db.QueryRow(`SELECT status FROM pipeline_locks WHERE pipeline_name = ?`, pipeline).Scan(&status)
	if err == nil && status == "running" {
		return false, nil
	}

	_, err = s.db.Exec(`
		INSERT INTO pipeline_locks (pipeline_name, run_id, started_at, status)
		VALUES (?, ?, ?, 'running')
		ON CONFLICT (pipeline_name) DO UPDATE SET
			run_id = excluded.run_id,
			started_at = excluded.started_at,
			status = 'running'
	`, pipeline, runID, now)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *LockStore) ReleaseLock(pipeline, runID string) error {
	_, err := s.db.Exec(`
		UPDATE pipeline_locks SET status = 'complete'
		WHERE pipeline_name = ? AND run_id = ?
	`, pipeline, runID)
	return err
}

func (s *LockStore) IsLocked(pipeline string) (bool, string, error) {
	var runID, status string
	err := s.db.QueryRow(`SELECT run_id, status FROM pipeline_locks WHERE pipeline_name = ?`, pipeline).Scan(&runID, &status)
	if err == sql.ErrNoRows {
		return false, "", nil
	}
	if err != nil {
		return false, "", err
	}
	return status == "running", runID, nil
}

func (s *LockStore) RecoverStaleLocks(maxAge time.Duration) (int, error) {
	cutoff := time.Now().UTC().Add(-maxAge).Format(time.RFC3339)
	result, err := s.db.Exec(`
		UPDATE pipeline_locks SET status = 'stale_recovered'
		WHERE status = 'running' AND started_at < ?
	`, cutoff)
	if err != nil {
		return 0, err
	}
	n, _ := result.RowsAffected()
	return int(n), nil
}
