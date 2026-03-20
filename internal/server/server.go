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
	httpServer  *http.Server
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

// Handler returns the HTTP handler with all API routes registered.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/health", s.handleHealth)
	mux.HandleFunc("/api/v1/trigger/", s.handleTrigger)
	mux.HandleFunc("/api/v1/status/", s.handleStatus)
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

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
