package server

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/DataDecodeHQ/granicus/internal/state"
)

// PipelineStateBackend is the subset of state operations needed by pipeline state handlers.
type PipelineStateBackend interface {
	ListRuns(ctx context.Context, pipeline string, statuses []string, since time.Time, limit int) ([]state.RunDoc, error)
	ListEvents(ctx context.Context, runID string, eventTypes []string) ([]state.EventDoc, error)
	GetIntervals(asset string) ([]state.IntervalState, error)
	Close() error
}

// StateBackendFactory creates a state backend for a given pipeline.
type StateBackendFactory func(ctx context.Context, pipeline string) (PipelineStateBackend, error)

// SetStateFactory sets the factory used to create state backends for pipeline state queries.
func (s *Server) SetStateFactory(fn StateBackendFactory) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stateFactory = fn
}

// handlePipelineState dispatches /api/v1/state/{pipeline}/{action} routes.
func (s *Server) handlePipelineState(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, ErrorResponse{Error: "method not allowed"})
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/api/v1/state/")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) < 2 || parts[0] == "" {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "pipeline and action required"})
		return
	}
	pipeline := parts[0]
	action := parts[1]

	s.mu.RLock()
	factory := s.stateFactory
	s.mu.RUnlock()

	if factory == nil {
		writeJSON(w, http.StatusNotImplemented, ErrorResponse{Error: "state backend not configured"})
		return
	}

	backend, err := factory(r.Context(), pipeline)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: err.Error()})
		return
	}
	defer backend.Close()

	switch action {
	case "history":
		s.handleStateHistory(w, r, backend, pipeline)
	case "events":
		s.handleStateEvents(w, r, backend)
	case "failures":
		s.handleStateFailures(w, r, backend, pipeline)
	case "stats":
		s.handleStateStats(w, r, backend, pipeline)
	case "status":
		s.handleStateStatus(w, r, backend, pipeline)
	case "intervals":
		s.handleStateIntervals(w, r, backend)
	default:
		writeJSON(w, http.StatusNotFound, ErrorResponse{Error: fmt.Sprintf("unknown state action %q", action)})
	}
}

func (s *Server) handleStateHistory(w http.ResponseWriter, r *http.Request, backend PipelineStateBackend, pipeline string) {
	limit := 20
	if l := r.URL.Query().Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 {
			limit = n
		}
	}

	since := time.Now().AddDate(0, 0, -7)
	if s := r.URL.Query().Get("since"); s != "" {
		if t, err := parseSinceParam(s); err == nil {
			since = t
		}
	}

	runs, err := backend.ListRuns(r.Context(), pipeline, nil, since, limit)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, runs)
}

func (s *Server) handleStateEvents(w http.ResponseWriter, r *http.Request, backend PipelineStateBackend) {
	runID := r.URL.Query().Get("run_id")
	if runID == "" {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "run_id query param required"})
		return
	}

	var types []string
	if t := r.URL.Query().Get("type"); t != "" {
		types = strings.Split(t, ",")
	}

	events, err := backend.ListEvents(r.Context(), runID, types)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, events)
}

func (s *Server) handleStateFailures(w http.ResponseWriter, r *http.Request, backend PipelineStateBackend, pipeline string) {
	since := time.Now().AddDate(0, 0, -7)
	if s := r.URL.Query().Get("since"); s != "" {
		if t, err := parseSinceParam(s); err == nil {
			since = t
		}
	}

	runs, err := backend.ListRuns(r.Context(), pipeline, []string{"failed", "crashed"}, since, 50)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: err.Error()})
		return
	}

	type failureRecord struct {
		RunID          string           `json:"run_id"`
		Pipeline       string           `json:"pipeline"`
		Status         string           `json:"status"`
		StartedAt      time.Time        `json:"started_at"`
		ErrorSummary   string           `json:"error_summary"`
		TriggerContext string           `json:"trigger_context"`
		FailedEvents   []state.EventDoc `json:"failed_events,omitempty"`
	}

	var records []failureRecord
	for _, run := range runs {
		events, _ := backend.ListEvents(r.Context(), run.RunID, []string{"asset_failed", "node_failed"})
		records = append(records, failureRecord{
			RunID:          run.RunID,
			Pipeline:       run.Pipeline,
			Status:         run.Status,
			StartedAt:      run.StartedAt,
			ErrorSummary:   run.ErrorSummary,
			TriggerContext: run.TriggerContext,
			FailedEvents:   events,
		})
	}
	writeJSON(w, http.StatusOK, records)
}

func (s *Server) handleStateStats(w http.ResponseWriter, r *http.Request, backend PipelineStateBackend, pipeline string) {
	node := r.URL.Query().Get("node")
	if node == "" {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "node query param required"})
		return
	}

	since := time.Now().AddDate(0, 0, -30)
	if s := r.URL.Query().Get("since"); s != "" {
		if t, err := parseSinceParam(s); err == nil {
			since = t
		}
	}

	runs, err := backend.ListRuns(r.Context(), pipeline, nil, since, 0)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: err.Error()})
		return
	}

	var totalRuns, successes, failures int
	var totalDuration int64
	errorCounts := make(map[string]int)

	for _, run := range runs {
		events, _ := backend.ListEvents(r.Context(), run.RunID, nil)
		for _, e := range events {
			if e.Node != node {
				continue
			}
			switch e.EventType {
			case "asset_succeeded", "node_succeeded":
				totalRuns++
				successes++
				totalDuration += e.DurationMs
			case "asset_failed", "node_failed":
				totalRuns++
				failures++
				totalDuration += e.DurationMs
				errorCounts[e.Error]++
			}
		}
	}

	stats := map[string]any{
		"node":            node,
		"pipeline":        pipeline,
		"total_runs":      totalRuns,
		"successes":       successes,
		"failures":        failures,
		"success_rate":    0.0,
		"avg_duration_ms": int64(0),
		"top_errors":      errorCounts,
	}
	if totalRuns > 0 {
		stats["success_rate"] = float64(successes) / float64(totalRuns)
		stats["avg_duration_ms"] = totalDuration / int64(totalRuns)
	}

	writeJSON(w, http.StatusOK, stats)
}

func (s *Server) handleStateStatus(w http.ResponseWriter, r *http.Request, backend PipelineStateBackend, pipeline string) {
	runs, err := backend.ListRuns(r.Context(), pipeline, []string{"running"}, time.Time{}, 50)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, runs)
}

func (s *Server) handleStateIntervals(w http.ResponseWriter, r *http.Request, backend PipelineStateBackend) {
	asset := r.URL.Query().Get("asset")
	if asset == "" {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "asset query param required"})
		return
	}

	intervals, err := backend.GetIntervals(asset)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, intervals)
}

func parseSinceParam(s string) (time.Time, error) {
	if len(s) < 2 {
		return time.Time{}, fmt.Errorf("invalid since: %s", s)
	}
	numStr := s[:len(s)-1]
	unit := s[len(s)-1]
	num, err := strconv.Atoi(numStr)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid since: %s", s)
	}
	switch unit {
	case 'h':
		return time.Now().Add(-time.Duration(num) * time.Hour), nil
	case 'd':
		return time.Now().AddDate(0, 0, -num), nil
	default:
		return time.Time{}, fmt.Errorf("invalid since unit: %c", unit)
	}
}
