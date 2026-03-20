package server

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/DataDecodeHQ/granicus/internal/schedule"
)

// ScheduleStore provides access to schedule configuration for the API.
type ScheduleStore interface {
	List() map[string]schedule.ScheduleEntry
	Get(pipeline string) (schedule.ScheduleEntry, bool)
	Set(pipeline string, entry schedule.ScheduleEntry) error
	Delete(pipeline string) error
}

// SetScheduleStore registers the schedule store used by schedule management endpoints.
func (s *Server) SetScheduleStore(store ScheduleStore) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scheduleStore = store
}

// handleScheduleRoutes dispatches /api/v1/schedules/* subroutes.
//
//	GET  /api/v1/schedules/list        - list all schedules
//	GET  /api/v1/schedules/{pipeline}  - show detail for one pipeline
//	POST /api/v1/schedules/{pipeline}  - create/update schedule entry
//	DELETE /api/v1/schedules/{pipeline} - remove schedule entry
func (s *Server) handleScheduleRoutes(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	store := s.scheduleStore
	s.mu.RUnlock()

	if store == nil {
		writeJSON(w, http.StatusNotImplemented, ErrorResponse{Error: "schedule store not configured"})
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/api/v1/schedules/")

	if path == "list" || path == "list/" {
		s.handleScheduleList(w, r, store)
		return
	}

	pipeline := strings.Trim(path, "/")
	if pipeline == "" {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "pipeline name required"})
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.handleScheduleGet(w, r, store, pipeline)
	case http.MethodPost:
		s.handleScheduleSet(w, r, store, pipeline)
	case http.MethodDelete:
		s.handleScheduleDelete(w, r, store, pipeline)
	default:
		writeJSON(w, http.StatusMethodNotAllowed, ErrorResponse{Error: "method not allowed"})
	}
}

type scheduleListEntry struct {
	Pipeline string `json:"pipeline"`
	Cron     string `json:"cron"`
	Timezone string `json:"timezone"`
	Mode     string `json:"mode"`
	Enabled  bool   `json:"enabled"`
}

func (s *Server) handleScheduleList(w http.ResponseWriter, r *http.Request, store ScheduleStore) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, ErrorResponse{Error: "method not allowed"})
		return
	}

	entries := store.List()
	var result []scheduleListEntry
	for name, e := range entries {
		mode := e.Mode
		if mode == "" {
			mode = "local"
		}
		result = append(result, scheduleListEntry{
			Pipeline: name,
			Cron:     e.Cron,
			Timezone: e.EffectiveTimezone(),
			Mode:     mode,
			Enabled:  e.IsEnabled(),
		})
	}

	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleScheduleGet(w http.ResponseWriter, r *http.Request, store ScheduleStore, pipeline string) {
	entry, ok := store.Get(pipeline)
	if !ok {
		writeJSON(w, http.StatusNotFound, ErrorResponse{Error: "pipeline " + pipeline + " not found in schedules"})
		return
	}

	mode := entry.Mode
	if mode == "" {
		mode = "local"
	}

	writeJSON(w, http.StatusOK, scheduleListEntry{
		Pipeline: pipeline,
		Cron:     entry.Cron,
		Timezone: entry.EffectiveTimezone(),
		Mode:     mode,
		Enabled:  entry.IsEnabled(),
	})
}

type scheduleSetRequest struct {
	Cron     string `json:"cron"`
	Timezone string `json:"timezone,omitempty"`
	Mode     string `json:"mode,omitempty"`
	Enabled  *bool  `json:"enabled,omitempty"`
}

func (s *Server) handleScheduleSet(w http.ResponseWriter, r *http.Request, store ScheduleStore, pipeline string) {
	var req scheduleSetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "invalid JSON body"})
		return
	}

	existing, exists := store.Get(pipeline)
	if req.Cron == "" && !exists {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "cron expression required for new schedule"})
		return
	}

	entry := existing
	if req.Cron != "" {
		entry.Cron = req.Cron
	}
	if req.Timezone != "" {
		entry.Timezone = req.Timezone
	}
	if req.Mode != "" {
		entry.Mode = req.Mode
	}
	if req.Enabled != nil {
		entry.Enabled = req.Enabled
	}

	if err := store.Set(pipeline, entry); err != nil {
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: err.Error()})
		return
	}

	mode := entry.Mode
	if mode == "" {
		mode = "local"
	}

	writeJSON(w, http.StatusOK, scheduleListEntry{
		Pipeline: pipeline,
		Cron:     entry.Cron,
		Timezone: entry.EffectiveTimezone(),
		Mode:     mode,
		Enabled:  entry.IsEnabled(),
	})
}

func (s *Server) handleScheduleDelete(w http.ResponseWriter, r *http.Request, store ScheduleStore, pipeline string) {
	if err := store.Delete(pipeline); err != nil {
		writeJSON(w, http.StatusNotFound, ErrorResponse{Error: err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"pipeline": pipeline, "deleted": true})
}
