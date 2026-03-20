package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/DataDecodeHQ/granicus/internal/config"
	"github.com/DataDecodeHQ/granicus/internal/events"
	"github.com/DataDecodeHQ/granicus/internal/scheduler"
	"github.com/DataDecodeHQ/granicus/internal/pipe_registry"
)

type TriggerRequest struct {
	Assets   []string `json:"assets,omitempty"`
	FromDate string   `json:"from_date,omitempty"`
	ToDate   string   `json:"to_date,omitempty"`
}

type TriggerResponse struct {
	RunID    string `json:"run_id"`
	Status   string `json:"status"`
	Pipeline string `json:"pipeline"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}

type RunFunc func(cfg *config.PipelineConfig, projectRoot string, runID string, req TriggerRequest)

type Server struct {
	mu          sync.RWMutex
	port        int
	projectRoot string
	configs     map[string]*config.PipelineConfig
	lockStore   *scheduler.LockStore
	eventStore  *events.Store
	runFunc     RunFunc
	pruneFunc    PruneFunc
	registry     pipe_registry.PipelineRegistry
	scheduleStore ScheduleStore
	stateFactory  StateBackendFactory
	httpServer   *http.Server
	wg          sync.WaitGroup
	shutdownCtx context.Context
}

// NewServer creates an API server for triggering and monitoring pipeline runs.
func NewServer(port int, projectRoot string, lockStore *scheduler.LockStore, eventStore *events.Store, runFunc RunFunc) *Server {
	return &Server{
		port:        port,
		projectRoot: projectRoot,
		configs:     make(map[string]*config.PipelineConfig),
		lockStore:   lockStore,
		eventStore:  eventStore,
		runFunc:     runFunc,
	}
}

// SetConfigs replaces the server's pipeline configuration map.
func (s *Server) SetConfigs(configs map[string]*config.PipelineConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.configs = configs
}

// PruneFunc runs archive+prune for a given retention_days and dry_run flag.
// Returns a result map suitable for JSON response.
type PruneFunc func(ctx context.Context, retentionDays int, dryRun bool) (map[string]any, error)

// SetPruneFunc registers the function used by the admin prune endpoint.
func (s *Server) SetPruneFunc(fn PruneFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneFunc = fn
}

// Handler returns the HTTP handler with all API routes registered.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/health", s.handleHealth)
	mux.HandleFunc("/api/v1/trigger/", s.handleTrigger)
	mux.HandleFunc("/api/v1/status/", s.handleStatus)
	mux.HandleFunc("/api/v1/runs/", s.handleRuns)
	mux.HandleFunc("/api/v1/pipelines/", s.handlePipelines)
	mux.HandleFunc("/api/v1/schedules", s.handleSchedules)
	mux.HandleFunc("/api/v1/schedules/", s.handleScheduleRoutes)
	mux.HandleFunc("/api/v1/admin/prune", s.handleAdminPrune)
	mux.HandleFunc("/api/v1/registry/", s.handleRegistry)
	mux.HandleFunc("/api/v1/state/", s.handlePipelineState)
	return mux
}

// ListenAndServe starts the HTTP server on the configured port.
func (s *Server) ListenAndServe() error {
	s.httpServer = &http.Server{
		Addr:    fmt.Sprintf(":%d", s.port),
		Handler: s.Handler(),
	}
	return s.httpServer.ListenAndServe()
}

// Close immediately shuts down the HTTP server.
func (s *Server) Close() error {
	if s.httpServer != nil {
		return s.httpServer.Close()
	}
	return nil
}

// SetShutdownCtx sets a context that is cancelled when the server should stop
// accepting new trigger requests. Must be called before serving begins.
func (s *Server) SetShutdownCtx(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.shutdownCtx = ctx
}

// WaitForRuns blocks until all in-progress pipeline runs finish or ctx is done.
func (s *Server) WaitForRuns(ctx context.Context) {
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, HealthResponse{Status: "ok"})
}

func (s *Server) handleTrigger(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, ErrorResponse{Error: "method not allowed"})
		return
	}

	// Reject new runs if shutdown has been signalled.
	s.mu.RLock()
	shutdownCtx := s.shutdownCtx
	s.mu.RUnlock()
	if shutdownCtx != nil {
		select {
		case <-shutdownCtx.Done():
			writeJSON(w, http.StatusServiceUnavailable, ErrorResponse{Error: "server is shutting down"})
			return
		default:
		}
	}

	pipeline := strings.TrimPrefix(r.URL.Path, "/api/v1/trigger/")
	if pipeline == "" {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "pipeline name required"})
		return
	}

	s.mu.RLock()
	cfg, ok := s.configs[pipeline]
	s.mu.RUnlock()
	if !ok {
		writeJSON(w, http.StatusNotFound, ErrorResponse{Error: fmt.Sprintf("pipeline %q not found", pipeline)})
		return
	}

	var req TriggerRequest
	if r.Body != nil && r.ContentLength > 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "invalid JSON body"})
			return
		}
	}

	runID := events.GenerateRunID()

	acquired, err := s.lockStore.AcquireLock(pipeline, runID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: "lock error"})
		return
	}
	if !acquired {
		writeJSON(w, http.StatusConflict, ErrorResponse{Error: fmt.Sprintf("pipeline %q is already running", pipeline)})
		return
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer s.lockStore.ReleaseLock(pipeline, runID)
		s.runFunc(cfg, s.projectRoot, runID, req)
	}()

	writeJSON(w, http.StatusAccepted, TriggerResponse{
		RunID:    runID,
		Status:   "accepted",
		Pipeline: pipeline,
	})
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, ErrorResponse{Error: "method not allowed"})
		return
	}

	runID := strings.TrimPrefix(r.URL.Path, "/api/v1/status/")
	if runID == "" {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "run_id required"})
		return
	}

	summary, err := s.eventStore.GetRunSummary(runID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, ErrorResponse{Error: fmt.Sprintf("run %q not found", runID)})
		return
	}

	writeJSON(w, http.StatusOK, summary)
}

// handleRuns dispatches run-related subroutes:
//   GET /api/v1/runs/{id}/logs - SSE log stream
func (s *Server) handleRuns(w http.ResponseWriter, r *http.Request) {
	// Path: /api/v1/runs/{id}/logs or /api/v1/runs/{id}
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/runs/")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) == 0 || parts[0] == "" {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "run_id required"})
		return
	}
	runID := parts[0]

	if len(parts) == 2 && parts[1] == "logs" {
		s.handleRunLogs(w, r, runID)
		return
	}

	// Default: same as status
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, ErrorResponse{Error: "method not allowed"})
		return
	}
	summary, err := s.eventStore.GetRunSummary(runID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, ErrorResponse{Error: fmt.Sprintf("run %q not found", runID)})
		return
	}
	writeJSON(w, http.StatusOK, summary)
}

// handleRunLogs streams run events as SSE.
func (s *Server) handleRunLogs(w http.ResponseWriter, r *http.Request, runID string) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, ErrorResponse{Error: "method not allowed"})
		return
	}

	evts, err := s.eventStore.Query(events.QueryFilters{RunID: runID, Limit: 1000})
	if err != nil {
		writeJSON(w, http.StatusNotFound, ErrorResponse{Error: fmt.Sprintf("run %q not found", runID)})
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeJSON(w, http.StatusOK, evts)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	for _, evt := range evts {
		data, _ := json.Marshal(evt)
		fmt.Fprintf(w, "data: %s\n\n", data)
		flusher.Flush()
	}
}

// handlePipelines dispatches pipeline-related subroutes:
//   GET  /api/v1/pipelines/{p}/runs     - run history
//   POST /api/v1/pipelines/{p}/trigger  - alias for trigger
//   POST /api/v1/pipelines/{p}/validate - validate config
//   POST /api/v1/pipelines/{p}/schedule - update schedule
func (s *Server) handlePipelines(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/pipelines/")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) < 2 || parts[0] == "" {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "pipeline name and action required (e.g. /api/v1/pipelines/my_pipeline/runs)"})
		return
	}
	pipeline := parts[0]
	action := parts[1]

	switch action {
	case "runs":
		s.handlePipelineRuns(w, r, pipeline)
	case "trigger":
		// Rewrite as standard trigger
		r.URL.Path = "/api/v1/trigger/" + pipeline
		s.handleTrigger(w, r)
	case "validate":
		s.handlePipelineValidate(w, r, pipeline)
	case "schedule":
		s.handlePipelineSchedule(w, r, pipeline)
	default:
		writeJSON(w, http.StatusNotFound, ErrorResponse{Error: fmt.Sprintf("unknown action %q", action)})
	}
}

// handlePipelineRuns returns run history for a pipeline.
func (s *Server) handlePipelineRuns(w http.ResponseWriter, r *http.Request, pipeline string) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, ErrorResponse{Error: "method not allowed"})
		return
	}

	runs, err := s.eventStore.ListRuns(50)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: "failed to list runs"})
		return
	}

	// Filter by pipeline
	var filtered []events.RunSummary
	for _, run := range runs {
		if run.Pipeline == pipeline {
			filtered = append(filtered, run)
		}
	}

	writeJSON(w, http.StatusOK, filtered)
}

// handlePipelineValidate validates a pipeline config.
func (s *Server) handlePipelineValidate(w http.ResponseWriter, r *http.Request, pipeline string) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, ErrorResponse{Error: "method not allowed"})
		return
	}

	s.mu.RLock()
	cfg, ok := s.configs[pipeline]
	s.mu.RUnlock()
	if !ok {
		writeJSON(w, http.StatusNotFound, ErrorResponse{Error: fmt.Sprintf("pipeline %q not found", pipeline)})
		return
	}

	var errs []string
	if err := config.ValidateResources(cfg); err != nil {
		errs = append(errs, err.Error())
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"pipeline": pipeline,
		"valid":    len(errs) == 0,
		"errors":   errs,
	})
}

type scheduleRequest struct {
	Schedule string `json:"schedule"`
}

// handlePipelineSchedule updates a pipeline's schedule.
func (s *Server) handlePipelineSchedule(w http.ResponseWriter, r *http.Request, pipeline string) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, ErrorResponse{Error: "method not allowed"})
		return
	}

	s.mu.RLock()
	cfg, ok := s.configs[pipeline]
	s.mu.RUnlock()
	if !ok {
		writeJSON(w, http.StatusNotFound, ErrorResponse{Error: fmt.Sprintf("pipeline %q not found", pipeline)})
		return
	}

	var req scheduleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "invalid JSON body"})
		return
	}

	s.mu.Lock()
	cfg.Schedule = req.Schedule
	s.mu.Unlock()

	writeJSON(w, http.StatusOK, map[string]any{
		"pipeline": pipeline,
		"schedule": req.Schedule,
	})
}

// handleSchedules returns all pipeline schedules.
func (s *Server) handleSchedules(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, ErrorResponse{Error: "method not allowed"})
		return
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	type scheduleEntry struct {
		Pipeline string `json:"pipeline"`
		Schedule string `json:"schedule"`
	}

	var schedules []scheduleEntry
	for name, cfg := range s.configs {
		if cfg.Schedule != "" {
			schedules = append(schedules, scheduleEntry{Pipeline: name, Schedule: cfg.Schedule})
		}
	}
	writeJSON(w, http.StatusOK, schedules)
}

type pruneRequest struct {
	RetentionDays int  `json:"retention_days"`
	DryRun        bool `json:"dry_run"`
}

func (s *Server) handleAdminPrune(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, ErrorResponse{Error: "method not allowed"})
		return
	}

	s.mu.RLock()
	fn := s.pruneFunc
	s.mu.RUnlock()

	if fn == nil {
		writeJSON(w, http.StatusNotImplemented, ErrorResponse{Error: "prune not configured"})
		return
	}

	var req pruneRequest
	if r.Body != nil && r.ContentLength > 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "invalid JSON body"})
			return
		}
	}

	if req.RetentionDays <= 0 {
		req.RetentionDays = 90
	}

	result, err := fn(r.Context(), req.RetentionDays, req.DryRun)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, result)
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
