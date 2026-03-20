package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/DataDecodeHQ/granicus/internal/pipe_registry"
	"github.com/DataDecodeHQ/granicus/internal/state"
)

// --- Mock Registry ---

type mockRegistry struct {
	versions []pipe_registry.Version
	listErr  error
	actErr   error

	activatedPipeline string
	activatedVersion  int
}

func (m *mockRegistry) Fetch(ctx context.Context, pipeline, version string) (string, func(), error) {
	return "", func() {}, nil
}

func (m *mockRegistry) List(ctx context.Context, pipeline string) ([]pipe_registry.Version, error) {
	return m.versions, m.listErr
}

func (m *mockRegistry) Active(ctx context.Context, pipeline string) (pipe_registry.Version, error) {
	for _, v := range m.versions {
		if v.Active {
			return v, nil
		}
	}
	return pipe_registry.Version{}, nil
}

func (m *mockRegistry) Register(ctx context.Context, pipeline, sourceDir string) (pipe_registry.Version, error) {
	return pipe_registry.Version{}, nil
}

func (m *mockRegistry) Activate(ctx context.Context, pipeline string, version int) error {
	m.activatedPipeline = pipeline
	m.activatedVersion = version
	return m.actErr
}

// --- Mock State Backend ---

type mockStateBackend struct {
	runs     []state.RunDoc
	events   []state.EventDoc
	ivs      []state.IntervalState
	listErr  error
	evtErr   error
	ivErr    error
}

func (m *mockStateBackend) ListRuns(_ context.Context, _ string, _ []string, _ time.Time, _ int) ([]state.RunDoc, error) {
	return m.runs, m.listErr
}

func (m *mockStateBackend) ListEvents(_ context.Context, _ string, _ []string) ([]state.EventDoc, error) {
	return m.events, m.evtErr
}

func (m *mockStateBackend) GetIntervals(_ string) ([]state.IntervalState, error) {
	return m.ivs, m.ivErr
}

func (m *mockStateBackend) Close() error { return nil }

// --- Registry Tests ---

func TestHandleRegistryVersions_ReturnsVersions(t *testing.T) {
	srv, _ := setupServer(t)
	reg := &mockRegistry{
		versions: []pipe_registry.Version{
			{Pipeline: "test", Number: 2, ContentHash: "abc", Active: true, FileCount: 5},
			{Pipeline: "test", Number: 1, ContentHash: "def", Active: false, FileCount: 3},
		},
	}
	srv.SetRegistry(reg)
	handler := srv.Handler()

	req := httptest.NewRequest("GET", "/api/v1/registry/test/versions", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var versions []pipe_registry.Version
	if err := json.NewDecoder(w.Body).Decode(&versions); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(versions) != 2 {
		t.Fatalf("expected 2 versions, got %d", len(versions))
	}
	if versions[0].Number != 2 || !versions[0].Active {
		t.Errorf("expected v2 active, got v%d active=%v", versions[0].Number, versions[0].Active)
	}
}

func TestHandleRegistryVersions_NoRegistry(t *testing.T) {
	srv, _ := setupServer(t)
	handler := srv.Handler()

	req := httptest.NewRequest("GET", "/api/v1/registry/test/versions", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusNotImplemented {
		t.Errorf("expected 501, got %d", w.Code)
	}
}

func TestHandleRegistryActivate_Success(t *testing.T) {
	srv, _ := setupServer(t)
	reg := &mockRegistry{}
	srv.SetRegistry(reg)
	handler := srv.Handler()

	body := bytes.NewBufferString(`{"version": 3}`)
	req := httptest.NewRequest("POST", "/api/v1/registry/my_pipeline/activate", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	if reg.activatedPipeline != "my_pipeline" {
		t.Errorf("expected pipeline my_pipeline, got %q", reg.activatedPipeline)
	}
	if reg.activatedVersion != 3 {
		t.Errorf("expected version 3, got %d", reg.activatedVersion)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["activated"] != true {
		t.Errorf("expected activated=true, got %v", resp["activated"])
	}
}

func TestHandleRegistryActivate_InvalidVersion(t *testing.T) {
	srv, _ := setupServer(t)
	reg := &mockRegistry{}
	srv.SetRegistry(reg)
	handler := srv.Handler()

	body := bytes.NewBufferString(`{"version": 0}`)
	req := httptest.NewRequest("POST", "/api/v1/registry/test/activate", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestHandleRegistryActivate_MethodNotAllowed(t *testing.T) {
	srv, _ := setupServer(t)
	reg := &mockRegistry{}
	srv.SetRegistry(reg)
	handler := srv.Handler()

	req := httptest.NewRequest("GET", "/api/v1/registry/test/activate", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", w.Code)
	}
}

func TestHandleRegistryDiff_ParsesQueryParams(t *testing.T) {
	srv, _ := setupServer(t)
	reg := &mockRegistry{}
	srv.SetRegistry(reg)
	handler := srv.Handler()

	req := httptest.NewRequest("GET", "/api/v1/registry/test/diff?v1=1&v2=2", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	// Diff is only supported if the registry implements the diffSource interface.
	// Our mock does not, so we expect 501.
	if w.Code != http.StatusNotImplemented {
		t.Errorf("expected 501 (diff not supported), got %d: %s", w.Code, w.Body.String())
	}
}

func TestHandleRegistryDiff_MissingParams(t *testing.T) {
	srv, _ := setupServer(t)
	reg := &mockRegistry{}
	srv.SetRegistry(reg)
	handler := srv.Handler()

	req := httptest.NewRequest("GET", "/api/v1/registry/test/diff?v1=1", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestHandleRegistryDiff_NonIntegerParams(t *testing.T) {
	srv, _ := setupServer(t)
	reg := &mockRegistry{}
	srv.SetRegistry(reg)
	handler := srv.Handler()

	req := httptest.NewRequest("GET", "/api/v1/registry/test/diff?v1=abc&v2=2", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

// --- Pipeline State Tests ---

func mockStateFactory(backend PipelineStateBackend) StateBackendFactory {
	return func(ctx context.Context, pipeline string) (PipelineStateBackend, error) {
		return backend, nil
	}
}

func TestHandleStateHistory_ReturnsRuns(t *testing.T) {
	srv, _ := setupServer(t)
	now := time.Now()
	backend := &mockStateBackend{
		runs: []state.RunDoc{
			{RunID: "run-1", Pipeline: "test", Status: "succeeded", StartedAt: now},
			{RunID: "run-2", Pipeline: "test", Status: "failed", StartedAt: now.Add(-time.Hour)},
		},
	}
	srv.SetStateFactory(mockStateFactory(backend))
	handler := srv.Handler()

	req := httptest.NewRequest("GET", "/api/v1/state/test/history?limit=10&since=7d", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var runs []state.RunDoc
	if err := json.NewDecoder(w.Body).Decode(&runs); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(runs) != 2 {
		t.Fatalf("expected 2 runs, got %d", len(runs))
	}
	if runs[0].RunID != "run-1" {
		t.Errorf("expected run-1, got %q", runs[0].RunID)
	}
}

func TestHandleStateEvents_RequiresRunID(t *testing.T) {
	srv, _ := setupServer(t)
	backend := &mockStateBackend{}
	srv.SetStateFactory(mockStateFactory(backend))
	handler := srv.Handler()

	req := httptest.NewRequest("GET", "/api/v1/state/test/events", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestHandleStateEvents_ReturnsEvents(t *testing.T) {
	srv, _ := setupServer(t)
	now := time.Now()
	backend := &mockStateBackend{
		events: []state.EventDoc{
			{RunID: "run-1", Node: "node_a", EventType: "asset_succeeded", Timestamp: now},
		},
	}
	srv.SetStateFactory(mockStateFactory(backend))
	handler := srv.Handler()

	req := httptest.NewRequest("GET", "/api/v1/state/test/events?run_id=run-1", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var events []state.EventDoc
	if err := json.NewDecoder(w.Body).Decode(&events); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(events) != 1 || events[0].Node != "node_a" {
		t.Errorf("unexpected events: %+v", events)
	}
}

func TestHandleStateStatus_ReturnsRunningPipelines(t *testing.T) {
	srv, _ := setupServer(t)
	now := time.Now()
	backend := &mockStateBackend{
		runs: []state.RunDoc{
			{RunID: "run-active", Pipeline: "test", Status: "running", StartedAt: now, NodeCount: 5},
		},
	}
	srv.SetStateFactory(mockStateFactory(backend))
	handler := srv.Handler()

	req := httptest.NewRequest("GET", "/api/v1/state/test/status", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var runs []state.RunDoc
	if err := json.NewDecoder(w.Body).Decode(&runs); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(runs) != 1 || runs[0].RunID != "run-active" {
		t.Errorf("unexpected runs: %+v", runs)
	}
}

func TestHandleStateNotConfigured(t *testing.T) {
	srv, _ := setupServer(t)
	handler := srv.Handler()

	req := httptest.NewRequest("GET", "/api/v1/state/test/history", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusNotImplemented {
		t.Errorf("expected 501, got %d: %s", w.Code, w.Body.String())
	}
}

func TestHandleState_MethodNotAllowed(t *testing.T) {
	srv, _ := setupServer(t)
	backend := &mockStateBackend{}
	srv.SetStateFactory(mockStateFactory(backend))
	handler := srv.Handler()

	req := httptest.NewRequest("POST", "/api/v1/state/test/history", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d: %s", w.Code, w.Body.String())
	}
}
