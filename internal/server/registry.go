package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/DataDecodeHQ/granicus/internal/pipe_registry"
)

// RegistryProvider gives the server access to a pipeline registry.
type RegistryProvider interface {
	Registry() pipe_registry.PipelineRegistry
}

// SetRegistry sets the pipeline source used by registry endpoints.
func (s *Server) SetRegistry(src pipe_registry.PipelineRegistry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.registry = src
}

// handleRegistryPush handles POST /api/v1/registry/push.
// Accepts multipart/form-data with an "archive" file field and optional "pipeline" and "activate" fields.
func (s *Server) handleRegistryPush(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, ErrorResponse{Error: "method not allowed"})
		return
	}

	s.mu.RLock()
	reg := s.registry
	s.mu.RUnlock()
	if reg == nil {
		writeJSON(w, http.StatusNotImplemented, ErrorResponse{Error: "registry not configured"})
		return
	}

	if err := r.ParseMultipartForm(256 << 20); err != nil {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "invalid multipart form: " + err.Error()})
		return
	}

	file, _, err := r.FormFile("archive")
	if err != nil {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "archive file required"})
		return
	}
	defer file.Close()

	pipeline := r.FormValue("pipeline")
	if pipeline == "" {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "pipeline field required"})
		return
	}

	// Write to temp file for Register()
	tmp, err := os.CreateTemp("", "granicus-push-*.tar.gz")
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: "creating temp file"})
		return
	}
	defer os.Remove(tmp.Name())
	defer tmp.Close()

	if _, err := io.Copy(tmp, file); err != nil {
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: "saving upload"})
		return
	}
	tmp.Close()

	ver, err := reg.Register(r.Context(), pipeline, tmp.Name())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: fmt.Sprintf("register: %v", err)})
		return
	}

	if r.FormValue("activate") == "true" {
		if err := reg.Activate(r.Context(), pipeline, ver.Number); err != nil {
			writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: fmt.Sprintf("activate: %v", err)})
			return
		}
		ver.Active = true
	}

	writeJSON(w, http.StatusOK, ver)
}

// handleRegistryActivate handles POST /api/v1/registry/{pipeline}/activate.
func (s *Server) handleRegistryActivate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, ErrorResponse{Error: "method not allowed"})
		return
	}

	s.mu.RLock()
	reg := s.registry
	s.mu.RUnlock()
	if reg == nil {
		writeJSON(w, http.StatusNotImplemented, ErrorResponse{Error: "registry not configured"})
		return
	}

	pipeline := extractPathSegment(r.URL.Path, "/api/v1/registry/", "/activate")
	if pipeline == "" {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "pipeline name required"})
		return
	}

	var body struct {
		Version int `json:"version"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Version <= 0 {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "valid version number required"})
		return
	}

	if err := reg.Activate(r.Context(), pipeline, body.Version); err != nil {
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"pipeline": pipeline, "version": body.Version, "activated": true})
}

// handleRegistryVersions handles GET /api/v1/registry/{pipeline}/versions.
func (s *Server) handleRegistryVersions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, ErrorResponse{Error: "method not allowed"})
		return
	}

	s.mu.RLock()
	reg := s.registry
	s.mu.RUnlock()
	if reg == nil {
		writeJSON(w, http.StatusNotImplemented, ErrorResponse{Error: "registry not configured"})
		return
	}

	pipeline := extractPathSegment(r.URL.Path, "/api/v1/registry/", "/versions")
	if pipeline == "" {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "pipeline name required"})
		return
	}

	versions, err := reg.List(r.Context(), pipeline)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, versions)
}

// handleRegistryDiff handles GET /api/v1/registry/{pipeline}/diff?v1=X&v2=Y.
func (s *Server) handleRegistryDiff(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, ErrorResponse{Error: "method not allowed"})
		return
	}

	s.mu.RLock()
	reg := s.registry
	s.mu.RUnlock()
	if reg == nil {
		writeJSON(w, http.StatusNotImplemented, ErrorResponse{Error: "registry not configured"})
		return
	}

	pipeline := extractPathSegment(r.URL.Path, "/api/v1/registry/", "/diff")
	if pipeline == "" {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "pipeline name required"})
		return
	}

	v1Str := r.URL.Query().Get("v1")
	v2Str := r.URL.Query().Get("v2")
	v1, err1 := strconv.Atoi(v1Str)
	v2, err2 := strconv.Atoi(v2Str)
	if err1 != nil || err2 != nil {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "v1 and v2 query params required (integers)"})
		return
	}

	type diffSource interface {
		Diff(ctx interface{}, pipeline string, vA, vB int) ([]string, []string, []string, error)
	}
	ds, ok := reg.(diffSource)
	if !ok {
		writeJSON(w, http.StatusNotImplemented, ErrorResponse{Error: "diff not supported for this registry"})
		return
	}

	added, removed, modified, err := ds.Diff(r.Context(), pipeline, v1, v2)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"added": added, "removed": removed, "modified": modified,
	})
}

// handleRegistry dispatches /api/v1/registry/* routes.
func (s *Server) handleRegistry(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/registry")

	if path == "/push" || path == "/push/" {
		s.handleRegistryPush(w, r)
		return
	}

	// Routes: /{pipeline}/activate, /{pipeline}/versions, /{pipeline}/diff
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) < 2 {
		writeJSON(w, http.StatusNotFound, ErrorResponse{Error: "unknown registry route"})
		return
	}

	action := parts[1]
	switch action {
	case "activate":
		s.handleRegistryActivate(w, r)
	case "versions":
		s.handleRegistryVersions(w, r)
	case "diff":
		s.handleRegistryDiff(w, r)
	default:
		writeJSON(w, http.StatusNotFound, ErrorResponse{Error: fmt.Sprintf("unknown registry action %q", action)})
	}
}

func extractPathSegment(path, prefix, suffix string) string {
	trimmed := strings.TrimPrefix(path, prefix)
	if suffix != "" {
		idx := strings.Index(trimmed, suffix)
		if idx >= 0 {
			trimmed = trimmed[:idx]
		}
	}
	return strings.Trim(trimmed, "/")
}
