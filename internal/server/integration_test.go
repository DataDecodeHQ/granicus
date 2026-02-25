package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/analytehealth/granicus/internal/config"
	"github.com/analytehealth/granicus/internal/logging"
)

func TestIntegration_AuthFlow(t *testing.T) {
	srv, _ := setupServer(t)
	keys := []APIKey{
		{Name: "ci-pipeline", Key: "grnc_sk_test123"},
	}
	handler := AuthMiddleware(keys, srv.Handler())

	// Health should work without auth
	req := httptest.NewRequest("GET", "/api/v1/health", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("health: expected 200, got %d", w.Code)
	}

	// Trigger without auth -> 401
	req = httptest.NewRequest("POST", "/api/v1/trigger/test_pipeline", nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("no auth: expected 401, got %d", w.Code)
	}

	// Trigger with wrong key -> 403
	req = httptest.NewRequest("POST", "/api/v1/trigger/test_pipeline", nil)
	req.Header.Set("Authorization", "Bearer wrong_key")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusForbidden {
		t.Errorf("wrong key: expected 403, got %d", w.Code)
	}

	// Trigger with valid key -> 202
	req = httptest.NewRequest("POST", "/api/v1/trigger/test_pipeline", nil)
	req.Header.Set("Authorization", "Bearer grnc_sk_test123")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusAccepted {
		t.Errorf("valid key: expected 202, got %d", w.Code)
	}
}

func TestIntegration_TriggerAndStatus(t *testing.T) {
	srv, logStore := setupServer(t)

	var lastRunID string
	srv.runFunc = func(cfg *config.PipelineConfig, pr string, runID string, req TriggerRequest) {
		lastRunID = runID
		// Simulate writing a run summary
		summary := logging.RunSummary{
			RunID:     runID,
			Pipeline:  cfg.Pipeline,
			Status:    "success",
			Succeeded: 1,
			StartTime: time.Now(),
			EndTime:   time.Now(),
		}
		logStore.WriteRunSummary(runID, summary)
	}

	handler := srv.Handler()

	// Trigger pipeline
	req := httptest.NewRequest("POST", "/api/v1/trigger/test_pipeline", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusAccepted {
		t.Fatalf("trigger: expected 202, got %d", w.Code)
	}

	var triggerResp TriggerResponse
	json.NewDecoder(w.Body).Decode(&triggerResp)

	// Wait for goroutine to complete
	time.Sleep(500 * time.Millisecond)

	// Check status
	req = httptest.NewRequest("GET", "/api/v1/status/"+triggerResp.RunID, nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("status: expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var statusResp logging.RunSummary
	json.NewDecoder(w.Body).Decode(&statusResp)
	if statusResp.Status != "success" {
		t.Errorf("expected success, got %q", statusResp.Status)
	}
	_ = lastRunID
}
