package events

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"
)

type QueryFilters struct {
	RunID     string
	Pipeline  string
	Asset     string
	EventType string // comma-separated list
	Severity  string // minimum level: "error", "warning", "info"
	Since     time.Time
	Until     time.Time
	Limit     int
	Offset    int
}

type RunSummary struct {
	RunID           string    `json:"run_id"`
	Pipeline        string    `json:"pipeline"`
	StartTime       time.Time `json:"start_time"`
	EndTime         time.Time `json:"end_time"`
	DurationSeconds float64   `json:"duration_seconds"`
	TotalNodes      int       `json:"total_nodes"`
	Succeeded       int       `json:"succeeded"`
	Failed          int       `json:"failed"`
	Skipped         int       `json:"skipped"`
	Status          string    `json:"status"`
}

type AssetResult struct {
	Asset      string            `json:"asset"`
	Status     string            `json:"status"`
	DurationMs int64             `json:"duration_ms"`
	Error      string            `json:"error,omitempty"`
	Metadata   map[string]string `json:"metadata,omitempty"`
}

// NodeResult is a deprecated alias for AssetResult.
type NodeResult = AssetResult

var severityLevels = map[string]int{
	"error":   3,
	"warning": 2,
	"info":    1,
}

// Query returns events matching the given filters, ordered by timestamp ascending.
func (s *Store) Query(filters QueryFilters) ([]Event, error) {
	query := "SELECT event_id, run_id, pipeline, asset, event_type, severity, timestamp, duration_ms, summary, details FROM events WHERE 1=1"
	var args []any

	if filters.RunID != "" {
		query += " AND run_id = ?"
		args = append(args, filters.RunID)
	}
	if filters.Pipeline != "" {
		query += " AND pipeline = ?"
		args = append(args, filters.Pipeline)
	}
	if filters.Asset != "" {
		query += " AND asset = ?"
		args = append(args, filters.Asset)
	}
	if filters.EventType != "" {
		types := strings.Split(filters.EventType, ",")
		placeholders := make([]string, len(types))
		for i, t := range types {
			placeholders[i] = "?"
			args = append(args, strings.TrimSpace(t))
		}
		query += " AND event_type IN (" + strings.Join(placeholders, ",") + ")"
	}
	if filters.Severity != "" {
		minLevel, ok := severityLevels[filters.Severity]
		if ok {
			var severities []string
			for sev, level := range severityLevels {
				if level >= minLevel {
					severities = append(severities, "?")
					args = append(args, sev)
				}
			}
			if len(severities) > 0 {
				query += " AND severity IN (" + strings.Join(severities, ",") + ")"
			}
		}
	}
	if !filters.Since.IsZero() {
		query += " AND timestamp >= ?"
		args = append(args, filters.Since.Format(time.RFC3339Nano))
	}
	if !filters.Until.IsZero() {
		query += " AND timestamp <= ?"
		args = append(args, filters.Until.Format(time.RFC3339Nano))
	}

	query += " ORDER BY timestamp ASC"

	if filters.Limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", filters.Limit)
	}
	if filters.Offset > 0 {
		query += fmt.Sprintf(" OFFSET %d", filters.Offset)
	}

	return s.queryEvents(query, args...)
}

func (s *Store) queryEvents(query string, args ...any) ([]Event, error) {
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []Event
	for rows.Next() {
		var e Event
		var ts, detailsJSON string
		if err := rows.Scan(&e.EventID, &e.RunID, &e.Pipeline, &e.Asset, &e.EventType, &e.Severity, &ts, &e.DurationMs, &e.Summary, &detailsJSON); err != nil {
			return nil, err
		}
		if t, perr := time.Parse(time.RFC3339Nano, ts); perr != nil {
			slog.Warn("failed to parse event timestamp", "event_id", e.EventID, "raw", ts, "error", perr)
		} else {
			e.Timestamp = t
		}
		if detailsJSON != "" && detailsJSON != "{}" {
			if jerr := json.Unmarshal([]byte(detailsJSON), &e.Details); jerr != nil {
				slog.Warn("failed to unmarshal event details", "event_id", e.EventID, "error", jerr)
			}
		}
		events = append(events, e)
	}
	return events, rows.Err()
}

// GetRunSummary builds a run summary from the start and completion events for the given run ID.
func (s *Store) GetRunSummary(runID string) (*RunSummary, error) {
	events, err := s.Query(QueryFilters{
		RunID:     runID,
		EventType: "run_started,run_completed",
	})
	if err != nil {
		return nil, err
	}

	summary := &RunSummary{RunID: runID}
	for _, e := range events {
		switch e.EventType {
		case "run_started":
			summary.Pipeline = e.Pipeline
			summary.StartTime = e.Timestamp
		case "run_completed":
			summary.EndTime = e.Timestamp
			summary.Status = getDetailString(e.Details, "status")
			summary.Succeeded = getDetailInt(e.Details, "succeeded")
			summary.Failed = getDetailInt(e.Details, "failed")
			summary.Skipped = getDetailInt(e.Details, "skipped")
			summary.TotalNodes = getDetailInt(e.Details, "total_nodes")
			summary.DurationSeconds = getDetailFloat(e.Details, "duration_seconds")
		}
	}

	if summary.Pipeline == "" {
		return nil, fmt.Errorf("run %s not found", runID)
	}

	return summary, nil
}

// ListRuns returns summaries of recent pipeline runs, ordered by start time descending.
func (s *Store) ListRuns(limit int) ([]RunSummary, error) {
	query := `
		SELECT DISTINCT run_id, pipeline, timestamp
		FROM events
		WHERE event_type = 'run_started'
		ORDER BY timestamp DESC
	`
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	rows, err := s.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var summaries []RunSummary
	for rows.Next() {
		var runID, pipeline, ts string
		if err := rows.Scan(&runID, &pipeline, &ts); err != nil {
			return nil, err
		}
		summary, err := s.GetRunSummary(runID)
		if err != nil {
			continue
		}
		summaries = append(summaries, *summary)
	}
	return summaries, rows.Err()
}

// GetFailedNodes returns the asset names of all nodes that failed during the given run.
func (s *Store) GetFailedNodes(runID string) ([]string, error) {
	events, err := s.Query(QueryFilters{
		RunID:     runID,
		EventType: "node_failed",
	})
	if err != nil {
		return nil, err
	}

	var names []string
	for _, e := range events {
		if e.Asset != "" {
			names = append(names, e.Asset)
		}
	}
	return names, nil
}

// GetNodeResults returns the execution result for every asset in the given run.
func (s *Store) GetNodeResults(runID string) ([]AssetResult, error) {
	events, err := s.Query(QueryFilters{
		RunID:     runID,
		EventType: "node_succeeded,node_failed,node_skipped",
	})
	if err != nil {
		return nil, err
	}

	var results []AssetResult
	for _, e := range events {
		status := "success"
		switch e.EventType {
		case "node_failed":
			status = "failed"
		case "node_skipped":
			status = "skipped"
		}
		nr := AssetResult{
			Asset:      e.Asset,
			Status:     status,
			DurationMs: e.DurationMs,
			Error:      getDetailString(e.Details, "error_message"),
		}
		if md, ok := e.Details["metadata"].(map[string]any); ok {
			nr.Metadata = make(map[string]string)
			for k, v := range md {
				nr.Metadata[k] = fmt.Sprintf("%v", v)
			}
		}
		results = append(results, nr)
	}
	return results, nil
}

// GetCheckResults returns check pass/fail events for the given run, optionally filtered by asset.
func (s *Store) GetCheckResults(runID, asset string) ([]Event, error) {
	filters := QueryFilters{
		RunID:     runID,
		EventType: "check_passed,check_failed",
	}
	if asset != "" {
		filters.Asset = asset
	}
	return s.Query(filters)
}

// GetLastSuccess returns the timestamp of the most recent successful execution for the given asset.
func (s *Store) GetLastSuccess(asset string) (*time.Time, error) {
	row := s.db.QueryRow(`
		SELECT timestamp FROM events
		WHERE asset = ? AND event_type = 'node_succeeded'
		ORDER BY timestamp DESC LIMIT 1
	`, asset)

	var ts string
	if err := row.Scan(&ts); err != nil {
		return nil, err
	}
	t, err := time.Parse(time.RFC3339Nano, ts)
	if err != nil {
		return nil, err
	}
	return &t, nil
}

// DeleteBefore removes all events with timestamps before the cutoff and returns the number deleted.
func (s *Store) DeleteBefore(cutoff time.Time) (int, error) {
	result, err := s.db.Exec(`DELETE FROM events WHERE timestamp < ?`, cutoff.Format(time.RFC3339Nano))
	if err != nil {
		return 0, err
	}
	n, _ := result.RowsAffected()
	return int(n), nil
}

func getDetailString(details map[string]any, key string) string {
	if details == nil {
		return ""
	}
	v, ok := details[key]
	if !ok {
		return ""
	}
	s, ok := v.(string)
	if ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}

func getDetailInt(details map[string]any, key string) int {
	if details == nil {
		return 0
	}
	v, ok := details[key]
	if !ok {
		return 0
	}
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	case int64:
		return int(n)
	}
	return 0
}

// RunCostSummary aggregates BQ job cost metadata across all node_succeeded events in a run.
type RunCostSummary struct {
	RunID               string  `json:"run_id"`
	TotalBytesProcessed int64   `json:"total_bytes_processed"`
	TotalCostUSD        float64 `json:"total_cost_usd"`
	TotalBQNodes        int     `json:"total_bq_nodes"`
	CachedNodes         int     `json:"cached_nodes"`
	CacheHitRate        float64 `json:"cache_hit_rate"`
}

// GetRunCostSummary aggregates BQ job metadata from node_succeeded events for a run.
// Returns a summary with all-zero values if no BQ cost data was recorded.
func (s *Store) GetRunCostSummary(runID string) (*RunCostSummary, error) {
	evts, err := s.Query(QueryFilters{
		RunID:     runID,
		EventType: "node_succeeded",
	})
	if err != nil {
		return nil, err
	}

	summary := &RunCostSummary{RunID: runID}
	for _, e := range evts {
		md, ok := e.Details["metadata"].(map[string]any)
		if !ok {
			continue
		}
		bytesStr, ok := md["total_bytes_processed"].(string)
		if !ok {
			continue
		}
		bytes, err := strconv.ParseInt(bytesStr, 10, 64)
		if err != nil {
			continue
		}
		summary.TotalBQNodes++
		summary.TotalBytesProcessed += bytes

		if costStr, ok := md["estimated_cost_usd"].(string); ok {
			if cost, err := strconv.ParseFloat(costStr, 64); err == nil {
				summary.TotalCostUSD += cost
			}
		}

		if cacheStr, ok := md["cache_hit"].(string); ok && cacheStr == "true" {
			summary.CachedNodes++
		}
	}

	if summary.TotalBQNodes > 0 {
		summary.CacheHitRate = float64(summary.CachedNodes) / float64(summary.TotalBQNodes)
	}

	return summary, nil
}

func getDetailFloat(details map[string]any, key string) float64 {
	if details == nil {
		return 0
	}
	v, ok := details[key]
	if !ok {
		return 0
	}
	switch n := v.(type) {
	case float64:
		return n
	case int:
		return float64(n)
	}
	return 0
}
