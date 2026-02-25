package events

import (
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
	_ "modernc.org/sqlite"
)

type Event struct {
	EventID    string         `json:"event_id"`
	RunID      string         `json:"run_id"`
	Pipeline   string         `json:"pipeline"`
	Asset      string         `json:"asset,omitempty"`
	EventType  string         `json:"event_type"`
	Severity   string         `json:"severity"`
	Timestamp  time.Time      `json:"timestamp"`
	DurationMs int64          `json:"duration_ms,omitempty"`
	Summary    string         `json:"summary"`
	Details    map[string]any `json:"details,omitempty"`
}

type Store struct {
	db *sql.DB
	mu sync.Mutex
}

const schema = `
CREATE TABLE IF NOT EXISTS events (
	event_id   TEXT PRIMARY KEY,
	run_id     TEXT NOT NULL,
	pipeline   TEXT NOT NULL,
	asset      TEXT NOT NULL DEFAULT '',
	event_type TEXT NOT NULL,
	severity   TEXT NOT NULL DEFAULT 'info',
	timestamp  TEXT NOT NULL,
	duration_ms INTEGER NOT NULL DEFAULT 0,
	summary    TEXT NOT NULL DEFAULT '',
	details    TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS model_registry (
	asset_name   TEXT PRIMARY KEY,
	source_hash  TEXT NOT NULL,
	version      INTEGER NOT NULL DEFAULT 1,
	last_run_at  TEXT NOT NULL,
	last_run_id  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS model_history (
	asset_name      TEXT NOT NULL,
	version         INTEGER NOT NULL,
	source_hash     TEXT NOT NULL,
	source_snapshot TEXT NOT NULL DEFAULT '',
	activated_at    TEXT NOT NULL,
	activated_run   TEXT NOT NULL,
	replaced_at     TEXT NOT NULL DEFAULT '',
	PRIMARY KEY (asset_name, version)
);

CREATE INDEX IF NOT EXISTS idx_events_run_id ON events(run_id);
CREATE INDEX IF NOT EXISTS idx_events_asset ON events(asset);
CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type);
CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp);
CREATE INDEX IF NOT EXISTS idx_events_pipeline ON events(pipeline);
`

func New(dbPath string) (*Store, error) {
	dir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("creating events dir: %w", err)
	}

	db, err := sql.Open("sqlite", dbPath+"?_pragma=busy_timeout(30000)")
	if err != nil {
		return nil, fmt.Errorf("opening events db: %w", err)
	}

	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("enabling WAL: %w", err)
	}

	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("creating schema: %w", err)
	}

	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) DB() *sql.DB {
	return s.db
}

func generateULID() string {
	return ulid.MustNew(ulid.Timestamp(time.Now()), rand.Reader).String()
}

func (s *Store) Emit(event Event) error {
	if event.EventID == "" {
		event.EventID = generateULID()
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}
	if event.Severity == "" {
		event.Severity = "info"
	}

	details, err := json.Marshal(event.Details)
	if err != nil {
		details = []byte("{}")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	_, err = s.db.Exec(`
		INSERT INTO events (event_id, run_id, pipeline, asset, event_type, severity, timestamp, duration_ms, summary, details)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, event.EventID, event.RunID, event.Pipeline, event.Asset, event.EventType,
		event.Severity, event.Timestamp.Format(time.RFC3339Nano), event.DurationMs, event.Summary, string(details))
	return err
}

func (s *Store) EmitBatch(events []Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO events (event_id, run_id, pipeline, asset, event_type, severity, timestamp, duration_ms, summary, details)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("prepare: %w", err)
	}
	defer stmt.Close()

	for i := range events {
		if events[i].EventID == "" {
			events[i].EventID = generateULID()
		}
		if events[i].Timestamp.IsZero() {
			events[i].Timestamp = time.Now().UTC()
		}
		if events[i].Severity == "" {
			events[i].Severity = "info"
		}

		details, err := json.Marshal(events[i].Details)
		if err != nil {
			details = []byte("{}")
		}

		_, err = stmt.Exec(
			events[i].EventID, events[i].RunID, events[i].Pipeline, events[i].Asset,
			events[i].EventType, events[i].Severity,
			events[i].Timestamp.Format(time.RFC3339Nano),
			events[i].DurationMs, events[i].Summary, string(details),
		)
		if err != nil {
			return fmt.Errorf("inserting event %d: %w", i, err)
		}
	}

	return tx.Commit()
}

// GenerateRunID creates a run ID in the format run_YYYYMMDD_HHMMSS_XXXX.
// Moved from internal/logging to be the canonical location.
func GenerateRunID() string {
	now := time.Now()
	suffix := fmt.Sprintf("%04x", mathRandIntn(0xFFFF))
	return fmt.Sprintf("run_%s_%s_%s",
		now.Format("20060102"),
		now.Format("150405"),
		suffix[:4],
	)
}

// CountLines counts the number of lines in a string.
// Moved from internal/logging to be the canonical location.
func CountLines(s string) int {
	if s == "" {
		return 0
	}
	count := 0
	for _, c := range s {
		if c == '\n' {
			count++
		}
	}
	return count + 1
}

func mathRandIntn(n int) int {
	// Use crypto/rand for a random int
	var b [2]byte
	rand.Read(b[:])
	return int(b[0])<<8 | int(b[1])%int(math.MaxInt16) % n
}
