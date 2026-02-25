package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/analytehealth/granicus/internal/config"
	"github.com/analytehealth/granicus/internal/logging"
	"github.com/analytehealth/granicus/internal/scheduler"
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
	logStore    *logging.Store
	runFunc     RunFunc
	httpServer  *http.Server
}

func NewServer(port int, projectRoot string, lockStore *scheduler.LockStore, logStore *logging.Store, runFunc RunFunc) *Server {
	return &Server{
		port:        port,
		projectRoot: projectRoot,
		configs:     make(map[string]*config.PipelineConfig),
		lockStore:   lockStore,
		logStore:    logStore,
		runFunc:     runFunc,
	}
}

func (s *Server) SetConfigs(configs map[string]*config.PipelineConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.configs = configs
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/health", s.handleHealth)
	mux.HandleFunc("/api/v1/trigger/", s.handleTrigger)
	mux.HandleFunc("/api/v1/status/", s.handleStatus)
	return mux
}

func (s *Server) ListenAndServe() error {
	s.httpServer = &http.Server{
		Addr:    fmt.Sprintf(":%d", s.port),
		Handler: s.Handler(),
	}
	return s.httpServer.ListenAndServe()
}

func (s *Server) Close() error {
	if s.httpServer != nil {
		return s.httpServer.Close()
	}
	return nil
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, HealthResponse{Status: "ok"})
}

func (s *Server) handleTrigger(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, ErrorResponse{Error: "method not allowed"})
		return
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

	runID := logging.GenerateRunID()

	acquired, err := s.lockStore.AcquireLock(pipeline, runID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: "lock error"})
		return
	}
	if !acquired {
		writeJSON(w, http.StatusConflict, ErrorResponse{Error: fmt.Sprintf("pipeline %q is already running", pipeline)})
		return
	}

	go func() {
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

	summary, err := s.logStore.ReadRunSummary(runID)
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
