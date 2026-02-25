package server

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/analytehealth/granicus/internal/config"
	"github.com/analytehealth/granicus/internal/logging"
	"github.com/analytehealth/granicus/internal/scheduler"
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

func setupServer(t *testing.T) (*Server, *logging.Store) {
	t.Helper()
	db := newTestDB(t)
	lockStore, err := scheduler.NewLockStore(db)
	if err != nil {
		t.Fatal(err)
	}

	projectRoot := t.TempDir()
	logStore := logging.NewStore(projectRoot)

	srv := NewServer(8080, projectRoot, lockStore, logStore, func(cfg *config.PipelineConfig, pr string, runID string, req TriggerRequest) {
		// no-op
	})

	srv.SetConfigs(map[string]*config.PipelineConfig{
		"test_pipeline": {
			Pipeline:    "test_pipeline",
			MaxParallel: 5,
			Assets:      []config.AssetConfig{{Name: "a", Type: "shell", Source: "a.sh"}},
		},
	})

	return srv, logStore
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
	srv, logStore := setupServer(t)
	handler := srv.Handler()

	// Write a run summary
	summary := logging.RunSummary{
		RunID:     "run_test_123",
		Pipeline:  "test_pipeline",
		Status:    "success",
		Succeeded: 3,
		Failed:    0,
		Skipped:   0,
	}
	// Ensure directory exists
	os.MkdirAll(filepath.Join(logStore.BaseDir(), ".granicus", "runs", "run_test_123"), 0755)
	if err := logStore.WriteRunSummary("run_test_123", summary); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest("GET", "/api/v1/status/run_test_123", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp logging.RunSummary
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
