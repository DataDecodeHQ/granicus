package server

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/DataDecodeHQ/granicus/internal/config"
	"github.com/DataDecodeHQ/granicus/internal/events"
	"github.com/DataDecodeHQ/granicus/internal/scheduler"
)

func newTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	db.Exec("PRAGMA journal_mode=WAL")
	t.Cleanup(func() { db.Close() })
	return db
}

func setupServer(t *testing.T) (*Server, *events.Store) {
	t.Helper()
	db := newTestDB(t)
	lockStore, err := scheduler.NewLockStore(db)
	if err != nil {
		t.Fatal(err)
	}

	eventsDBPath := filepath.Join(t.TempDir(), "events.db")
	eventStore, err := events.New(eventsDBPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { eventStore.Close() })

	srv := NewServer(8080, t.TempDir(), lockStore, eventStore, func(cfg *config.PipelineConfig, pr string, runID string, req TriggerRequest) {
		// no-op
	})

	srv.SetConfigs(map[string]*config.PipelineConfig{
		"test_pipeline": {
			Pipeline: "test_pipeline",
			Assets:   []config.AssetConfig{{Name: "a", Type: "shell", Source: "a.sh"}},
		},
	})

	return srv, eventStore
}

func TestHealth_Returns200(t *testing.T) {
	srv, _ := setupServer(t)
	handler := srv.Handler()

	req := httptest.NewRequest("GET", "/api/v1/health", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	var resp HealthResponse
	json.NewDecoder(w.Body).Decode(&resp)
	if resp.Status != "ok" {
		t.Errorf("expected ok, got %q", resp.Status)
	}
}

func TestTrigger_Returns202(t *testing.T) {
	srv, _ := setupServer(t)
	handler := srv.Handler()

	body := bytes.NewBufferString(`{"from_date": "2024-01-01"}`)
	req := httptest.NewRequest("POST", "/api/v1/trigger/test_pipeline", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Errorf("expected 202, got %d: %s", w.Code, w.Body.String())
	}

	var resp TriggerResponse
	json.NewDecoder(w.Body).Decode(&resp)
	if resp.Pipeline != "test_pipeline" {
		t.Errorf("expected test_pipeline, got %q", resp.Pipeline)
	}
	if resp.RunID == "" {
		t.Error("expected non-empty run_id")
	}
}

func TestTrigger_404ForUnknownPipeline(t *testing.T) {
	srv, _ := setupServer(t)
	handler := srv.Handler()

	req := httptest.NewRequest("POST", "/api/v1/trigger/nonexistent", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", w.Code)
	}
}

func TestTrigger_409WhenLocked(t *testing.T) {
	srv, _ := setupServer(t)
	handler := srv.Handler()

	// Use a blocking runFunc so the lock is held
	srv.runFunc = func(cfg *config.PipelineConfig, pr string, runID string, req TriggerRequest) {
		select {} // block forever
	}

	// First trigger — should succeed
	req1 := httptest.NewRequest("POST", "/api/v1/trigger/test_pipeline", nil)
	w1 := httptest.NewRecorder()
	handler.ServeHTTP(w1, req1)
	if w1.Code != http.StatusAccepted {
		t.Fatalf("first trigger: expected 202, got %d", w1.Code)
	}

	// Second trigger — should be locked
	req2 := httptest.NewRequest("POST", "/api/v1/trigger/test_pipeline", nil)
	w2 := httptest.NewRecorder()
	handler.ServeHTTP(w2, req2)
	if w2.Code != http.StatusConflict {
		t.Errorf("second trigger: expected 409, got %d: %s", w2.Code, w2.Body.String())
	}
}

func TestStatus_ReturnsRunData(t *testing.T) {
	srv, eventStore := setupServer(t)
	handler := srv.Handler()

	// Emit run events
	eventStore.Emit(events.Event{
		RunID: "run_test_123", Pipeline: "test_pipeline", EventType: "run_started",
		Severity: "info", Summary: "started",
	})
	eventStore.Emit(events.Event{
		RunID: "run_test_123", Pipeline: "test_pipeline", EventType: "run_completed",
		Severity: "info", Summary: "completed",
		Details: map[string]any{
			"status": "success", "succeeded": 3, "failed": 0, "skipped": 0,
			"total_nodes": 3, "duration_seconds": 10.0,
		},
	})

	req := httptest.NewRequest("GET", "/api/v1/status/run_test_123", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp events.RunSummary
	json.NewDecoder(w.Body).Decode(&resp)
	if resp.RunID != "run_test_123" {
		t.Errorf("expected run_test_123, got %q", resp.RunID)
	}
	if resp.Succeeded != 3 {
		t.Errorf("expected 3 succeeded, got %d", resp.Succeeded)
	}
}

func TestStatus_404ForMissingRun(t *testing.T) {
	srv, _ := setupServer(t)
	handler := srv.Handler()

	req := httptest.NewRequest("GET", "/api/v1/status/nonexistent", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", w.Code)
	}
}

func TestHandleAdminPrune_Success(t *testing.T) {
	srv, _ := setupServer(t)
	srv.SetPruneFunc(func(ctx context.Context, retentionDays int, dryRun bool) (map[string]any, error) {
		return map[string]any{
			"runs_archived":    5,
			"runs_pruned":      3,
			"intervals_pruned": 10,
		}, nil
	})
	handler := srv.Handler()

	body := bytes.NewBufferString(`{"retention_days": 30}`)
	req := httptest.NewRequest("POST", "/api/v1/admin/prune", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["runs_archived"] != float64(5) {
		t.Errorf("expected runs_archived=5, got %v", resp["runs_archived"])
	}
	if resp["runs_pruned"] != float64(3) {
		t.Errorf("expected runs_pruned=3, got %v", resp["runs_pruned"])
	}
	if resp["intervals_pruned"] != float64(10) {
		t.Errorf("expected intervals_pruned=10, got %v", resp["intervals_pruned"])
	}
}

func TestHandleAdminPrune_DryRun(t *testing.T) {
	srv, _ := setupServer(t)

	var receivedDryRun bool
	srv.SetPruneFunc(func(ctx context.Context, retentionDays int, dryRun bool) (map[string]any, error) {
		receivedDryRun = dryRun
		return map[string]any{
			"runs_archived":    0,
			"runs_pruned":      0,
			"intervals_pruned": 0,
			"dry_run":          true,
		}, nil
	})
	handler := srv.Handler()

	body := bytes.NewBufferString(`{"retention_days": 30, "dry_run": true}`)
	req := httptest.NewRequest("POST", "/api/v1/admin/prune", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if !receivedDryRun {
		t.Error("expected PruneFunc to receive dryRun=true")
	}
}

func TestHandleAdminPrune_NotConfigured(t *testing.T) {
	srv, _ := setupServer(t)
	handler := srv.Handler()

	body := bytes.NewBufferString(`{"retention_days": 30}`)
	req := httptest.NewRequest("POST", "/api/v1/admin/prune", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusNotImplemented {
		t.Errorf("expected 501, got %d: %s", w.Code, w.Body.String())
	}
}

func TestHandleAdminPrune_MethodNotAllowed(t *testing.T) {
	srv, _ := setupServer(t)
	handler := srv.Handler()

	req := httptest.NewRequest("GET", "/api/v1/admin/prune", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d: %s", w.Code, w.Body.String())
	}
}
