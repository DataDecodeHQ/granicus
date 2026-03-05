package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/Andrew-DataDecode/Granicus/internal/config"
)

// waitForRequests polls until the counter reaches want, or times out.
func waitForRequests(t *testing.T, mu *sync.Mutex, count *int, want int) {
	t.Helper()
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := *count
		mu.Unlock()
		if n >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestAlertManager_NilRouting_NoOp(t *testing.T) {
	am := NewAlertManager(nil, nil)
	am.SendAlerts("critical", AlertData{Pipeline: "p", RunID: "r"})
	am.SendFailureAlerts(AlertData{Pipeline: "p", RunID: "r"})
	// No panic = pass
}

func TestAlertManager_EmptyRouting_NoOp(t *testing.T) {
	am := NewAlertManager(&config.AlertRoutingConfig{}, nil)
	am.SendAlerts("warning", AlertData{Pipeline: "p", RunID: "r"})
}

func TestAlertManager_RoutesToCriticalWebhook(t *testing.T) {
	var mu sync.Mutex
	criticalHits, defaultHits := 0, 0

	criticalSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		criticalHits++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer criticalSrv.Close()

	defaultSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defaultHits++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer defaultSrv.Close()

	routing := &config.AlertRoutingConfig{
		Critical: &config.AlertSeverityConfig{URL: criticalSrv.URL},
		Default:  &config.AlertSeverityConfig{URL: defaultSrv.URL},
	}
	am := NewAlertManager(routing, nil)
	am.SendAlerts("critical", AlertData{Pipeline: "p", RunID: "r", Status: "failed"})

	waitForRequests(t, &mu, &criticalHits, 1)

	mu.Lock()
	defer mu.Unlock()
	if criticalHits != 1 {
		t.Errorf("critical webhook: want 1, got %d", criticalHits)
	}
	if defaultHits != 0 {
		t.Errorf("default webhook: want 0 for critical severity, got %d", defaultHits)
	}
}

func TestAlertManager_RoutesToWarningWebhook(t *testing.T) {
	var mu sync.Mutex
	warnHits, defaultHits := 0, 0

	warnSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		warnHits++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer warnSrv.Close()

	defaultSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defaultHits++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer defaultSrv.Close()

	routing := &config.AlertRoutingConfig{
		Warning: &config.AlertSeverityConfig{URL: warnSrv.URL},
		Default: &config.AlertSeverityConfig{URL: defaultSrv.URL},
	}
	am := NewAlertManager(routing, nil)
	am.SendAlerts("warning", AlertData{Pipeline: "p", RunID: "r"})

	waitForRequests(t, &mu, &warnHits, 1)

	mu.Lock()
	defer mu.Unlock()
	if warnHits != 1 {
		t.Errorf("warning webhook: want 1, got %d", warnHits)
	}
	if defaultHits != 0 {
		t.Errorf("default webhook: want 0 for warning severity, got %d", defaultHits)
	}
}

func TestAlertManager_FallsBackToDefault_WhenNoSeverityConfig(t *testing.T) {
	var mu sync.Mutex
	hits := 0

	defaultSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hits++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer defaultSrv.Close()

	// warning severity has no specific URL; should fall back to default
	routing := &config.AlertRoutingConfig{
		Default: &config.AlertSeverityConfig{URL: defaultSrv.URL},
	}
	am := NewAlertManager(routing, nil)
	am.SendAlerts("warning", AlertData{Pipeline: "p", RunID: "r"})

	waitForRequests(t, &mu, &hits, 1)

	mu.Lock()
	defer mu.Unlock()
	if hits != 1 {
		t.Errorf("default webhook: want 1, got %d", hits)
	}
}

func TestAlertManager_FallsBackToDefault_ForUnknownSeverity(t *testing.T) {
	var mu sync.Mutex
	hits := 0

	defaultSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hits++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer defaultSrv.Close()

	routing := &config.AlertRoutingConfig{
		Default: &config.AlertSeverityConfig{URL: defaultSrv.URL},
	}
	am := NewAlertManager(routing, nil)
	am.SendAlerts("info", AlertData{Pipeline: "p", RunID: "r"})

	waitForRequests(t, &mu, &hits, 1)

	mu.Lock()
	defer mu.Unlock()
	if hits != 1 {
		t.Errorf("default webhook: want 1 for unknown severity, got %d", hits)
	}
}

func TestAlertManager_SendFailureAlerts_HitsDefault(t *testing.T) {
	var mu sync.Mutex
	hits := 0

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hits++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	routing := &config.AlertRoutingConfig{
		Default: &config.AlertSeverityConfig{URL: srv.URL},
	}
	am := NewAlertManager(routing, nil)
	am.SendFailureAlerts(AlertData{Pipeline: "p", RunID: "r", Failed: 3})

	waitForRequests(t, &mu, &hits, 1)

	mu.Lock()
	defer mu.Unlock()
	if hits != 1 {
		t.Errorf("want 1 webhook call, got %d", hits)
	}
}

func TestAlertManager_TemplateRendering_AllFields(t *testing.T) {
	var mu sync.Mutex
	var bodies []string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, string(b))
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	tmpl := `{"pipe":"{{.Pipeline}}","env":"{{.Environment}}","cost":{{.TotalCost}},"dur":{{.Duration}},"failed_assets":"{{index .FailedAssets 0}}"}`
	routing := &config.AlertRoutingConfig{
		Default: &config.AlertSeverityConfig{URL: srv.URL, Template: tmpl},
	}
	am := NewAlertManager(routing, nil)

	am.SendAlerts("warning", AlertData{
		Pipeline:     "my_pipeline",
		Environment:  "prod",
		TotalCost:    9.99,
		Duration:     120.5,
		FailedAssets: []string{"asset_x"},
	})

	bodyCount := 0
	waitForRequests(t, &mu, &bodyCount, 0) // just pause briefly

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(bodies)
		mu.Unlock()
		if n >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(bodies) == 0 {
		t.Fatal("no request received by webhook")
	}
	want := `{"pipe":"my_pipeline","env":"prod","cost":9.99,"dur":120.5,"failed_assets":"asset_x"}`
	if bodies[0] != want {
		t.Errorf("body mismatch\nwant: %s\n got: %s", want, bodies[0])
	}
}

func TestRenderAlertBody_DefaultTemplate(t *testing.T) {
	data := AlertData{
		Pipeline:     "pipe1",
		RunID:        "run_abc",
		Status:       "failed",
		ErrorMessage: "quota exceeded",
		Timestamp:    "2026-03-05T12:00:00Z",
	}
	body, err := renderAlertBody("", data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(body), &m); err != nil {
		t.Fatalf("default body not valid JSON: %v\nbody: %s", err, body)
	}
	cases := map[string]string{
		"pipeline": "pipe1",
		"run_id":   "run_abc",
		"status":   "failed",
		"error":    "quota exceeded",
	}
	for k, want := range cases {
		if m[k] != want {
			t.Errorf("field %q: want %q, got %v", k, want, m[k])
		}
	}
}

func TestRenderAlertBody_CustomTemplate_AllDataFields(t *testing.T) {
	tmpl := `{"p":"{{.Pipeline}}","id":"{{.RunID}}","status":"{{.Status}}","summary":"{{.Summary}}","dur":{{.Duration}},"env":"{{.Environment}}","cost":{{.TotalCost}},"msg":"{{.ErrorMessage}}","ts":"{{.Timestamp}}","failed":{{.Failed}},"ok":{{.Succeeded}},"skip":{{.Skipped}}}`
	data := AlertData{
		Pipeline:     "p1",
		RunID:        "r1",
		Status:       "completed_with_failures",
		Summary:      "2 failed",
		Duration:     30.0,
		Environment:  "staging",
		TotalCost:    5.5,
		ErrorMessage: "err msg",
		Timestamp:    "2026-03-05T00:00:00Z",
		Failed:       2,
		Succeeded:    8,
		Skipped:      1,
	}
	body, err := renderAlertBody(tmpl, data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(body), &m); err != nil {
		t.Fatalf("body not valid JSON: %v\nbody: %s", err, body)
	}
	checks := map[string]any{
		"p":       "p1",
		"id":      "r1",
		"status":  "completed_with_failures",
		"summary": "2 failed",
		"dur":     30.0,
		"env":     "staging",
		"cost":    5.5,
		"msg":     "err msg",
		"ts":      "2026-03-05T00:00:00Z",
		"failed":  2.0,
		"ok":      8.0,
		"skip":    1.0,
	}
	for k, want := range checks {
		if m[k] != want {
			t.Errorf("field %q: want %v, got %v", k, want, m[k])
		}
	}
}

func TestRenderAlertBody_InvalidTemplate_ReturnsError(t *testing.T) {
	_, err := renderAlertBody("{{.Unclosed", AlertData{})
	if err == nil {
		t.Error("expected error for invalid template, got nil")
	}
}
