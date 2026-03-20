package events

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

type ModelVersion struct {
	AssetName      string `json:"asset_name"`
	Version        int    `json:"version"`
	SourceHash     string `json:"source_hash"`
	SourceSnapshot string `json:"source_snapshot,omitempty"`
	ActivatedAt    string `json:"activated_at"`
	ActivatedRun   string `json:"activated_run"`
	ReplacedAt     string `json:"replaced_at,omitempty"`
}

// HashFile returns the hex-encoded SHA-256 hash of the file at the given path.
func HashFile(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return HashBytes(data), nil
}

// HashBytes returns the hex-encoded SHA-256 hash of the given byte slice.
func HashBytes(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

// dag:boundary
func (s *Store) RecordModelVersion(asset, sourceFile, sourceHash, runID string) (changed bool, version int, err error) {
	now := time.Now().UTC().Format(time.RFC3339)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if asset exists in registry
	var currentHash string
	var currentVersion int
	err = s.db.QueryRow("SELECT source_hash, version FROM model_registry WHERE asset_name = ?", asset).Scan(&currentHash, &currentVersion)

	if err != nil {
		// New asset — insert into registry and history
		_, err = s.db.Exec(`
			INSERT INTO model_registry (asset_name, source_hash, version, last_run_at, last_run_id)
			VALUES (?, ?, 1, ?, ?)
		`, asset, sourceHash, now, runID)
		if err != nil {
			return false, 0, fmt.Errorf("inserting model registry: %w", err)
		}

		// Read source snapshot
		snapshot := ""
		if sourceFile != "" {
			if data, ferr := os.ReadFile(sourceFile); ferr == nil {
				snapshot = string(data)
			}
		}

		_, err = s.db.Exec(`
			INSERT INTO model_history (asset_name, version, source_hash, source_snapshot, activated_at, activated_run)
			VALUES (?, 1, ?, ?, ?, ?)
		`, asset, sourceHash, snapshot, now, runID)
		if err != nil {
			return false, 0, fmt.Errorf("inserting model history: %w", err)
		}

		// Emit model_registered event (without lock since we already hold it — use db directly)
		s.emitUnlocked(Event{
			EventID:   generateULID(),
			RunID:     runID,
			Asset:     asset,
			EventType: "model_registered",
			Severity:  "info",
			Timestamp: time.Now().UTC(),
			Summary:   fmt.Sprintf("Model %s registered (v1)", asset),
			Details:   map[string]any{"source_hash": sourceHash, "version": 1},
		})

		return true, 1, nil
	}

	if currentHash == sourceHash {
		// Hash matches — just update last_run
		_, err = s.db.Exec(`
			UPDATE model_registry SET last_run_at = ?, last_run_id = ? WHERE asset_name = ?
		`, now, runID, asset)
		return false, currentVersion, err
	}

	// Hash differs — new version
	newVersion := currentVersion + 1

	// Read source snapshot
	snapshot := ""
	if sourceFile != "" {
		if data, ferr := os.ReadFile(sourceFile); ferr == nil {
			snapshot = string(data)
		}
	}

	// Update registry
	_, err = s.db.Exec(`
		UPDATE model_registry SET source_hash = ?, version = ?, last_run_at = ?, last_run_id = ? WHERE asset_name = ?
	`, sourceHash, newVersion, now, runID, asset)
	if err != nil {
		return false, 0, err
	}

	// Set replaced_at on previous version
	_, _ = s.db.Exec(`
		UPDATE model_history SET replaced_at = ? WHERE asset_name = ? AND version = ?
	`, now, asset, currentVersion)

	// Insert new history entry
	_, err = s.db.Exec(`
		INSERT INTO model_history (asset_name, version, source_hash, source_snapshot, activated_at, activated_run)
		VALUES (?, ?, ?, ?, ?, ?)
	`, asset, newVersion, sourceHash, snapshot, now, runID)
	if err != nil {
		return false, 0, err
	}

	s.emitUnlocked(Event{
		EventID:   generateULID(),
		RunID:     runID,
		Asset:     asset,
		EventType: "model_changed",
		Severity:  "info",
		Timestamp: time.Now().UTC(),
		Summary:   fmt.Sprintf("Model %s changed (v%d -> v%d)", asset, currentVersion, newVersion),
		Details: map[string]any{
			"source_hash":      sourceHash,
			"previous_hash":    currentHash,
			"version":          newVersion,
			"previous_version": currentVersion,
		},
	})

	return true, newVersion, nil
}

// dag:boundary
func (s *Store) emitUnlocked(event Event) error {
	details, _ := json.Marshal(event.Details)
	_, err := s.db.Exec(`
		INSERT INTO events (event_id, run_id, pipeline, asset, event_type, severity, timestamp, duration_ms, summary, details)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, event.EventID, event.RunID, event.Pipeline, event.Asset, event.EventType,
		event.Severity, event.Timestamp.Format(time.RFC3339Nano), event.DurationMs, event.Summary, string(details))
	return err
}

// GetModelVersion returns the current version number and source hash for the given asset.
func (s *Store) GetModelVersion(asset string) (int, string, error) {
	var version int
	var hash string
	err := s.db.QueryRow("SELECT version, source_hash FROM model_registry WHERE asset_name = ?", asset).Scan(&version, &hash)
	if err != nil {
		return 0, "", err
	}
	return version, hash, nil
}

// GetModelHistory returns all version history entries for the given asset, ordered newest first.
func (s *Store) GetModelHistory(asset string) ([]ModelVersion, error) {
	rows, err := s.db.Query(`
		SELECT asset_name, version, source_hash, source_snapshot, activated_at, activated_run, replaced_at
		FROM model_history WHERE asset_name = ? ORDER BY version DESC
	`, asset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var versions []ModelVersion
	for rows.Next() {
		var mv ModelVersion
		if err := rows.Scan(&mv.AssetName, &mv.Version, &mv.SourceHash, &mv.SourceSnapshot, &mv.ActivatedAt, &mv.ActivatedRun, &mv.ReplacedAt); err != nil {
			return nil, err
		}
		versions = append(versions, mv)
	}
	return versions, rows.Err()
}

// dag:boundary
func (s *Store) ListModels() ([]ModelVersion, error) {
	rows, err := s.db.Query(`
		SELECT asset_name, version, source_hash, '', last_run_at, last_run_id, ''
		FROM model_registry ORDER BY asset_name
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var models []ModelVersion
	for rows.Next() {
		var mv ModelVersion
		if err := rows.Scan(&mv.AssetName, &mv.Version, &mv.SourceHash, &mv.SourceSnapshot, &mv.ActivatedAt, &mv.ActivatedRun, &mv.ReplacedAt); err != nil {
			return nil, err
		}
		models = append(models, mv)
	}
	return models, rows.Err()
}
