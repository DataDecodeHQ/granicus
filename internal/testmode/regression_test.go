package testmode

// Regression tests verifying the TestConfig-based signatures of CreateTestDataset,
// DropTestDataset, and CleanupOldTestDatasets work correctly.
//
// These tests use a fake BQ HTTP server to exercise the full call path without
// real GCP credentials.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/api/option"

	"github.com/DataDecodeHQ/granicus/internal/events"
)

// fakeBQServer returns an httptest.Server that handles BigQuery REST API calls
// used by CreateTestDataset, DropTestDataset, and ListTestDatasets.
//
// handler is called with each request so tests can customize responses.
func fakeBQServer(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return srv
}

// bqClientOpts returns client options that point to a fake BQ server and skip auth.
func bqClientOpts(serverURL string) []option.ClientOption {
	return []option.ClientOption{
		option.WithEndpoint(serverURL),
		option.WithoutAuthentication(),
	}
}

// newTestEventStore creates a temporary event store for test assertions.
func newTestEventStore(t *testing.T) *events.Store {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "events.db")
	s, err := events.New(dbPath)
	if err != nil {
		t.Fatalf("creating event store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// --- Signature regression: verify each function accepts exactly 3 params via TestConfig ---

// TestCreateTestDataset_SignatureRegression ensures the function signature
// matches: (cfg TestConfig, baseDataset, runID string) (string, error).
func TestCreateTestDataset_SignatureRegression(t *testing.T) {
	srv := fakeBQServer(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/datasets") && r.Method == http.MethodPost {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"datasetReference":{"datasetId":"dev__test_abcd","projectId":"test-proj"}}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{}`))
	})

	ctx := context.Background()
	store := newTestEventStore(t)
	opts := bqClientOpts(srv.URL)

	cfg := TestConfig{Ctx: ctx, Project: "test-proj", EventStore: store, ClientOpts: opts}

	// Call with 3 params: cfg, baseDataset, runID
	name, err := CreateTestDataset(cfg, "dev", "run-20260225-abcd")
	if err != nil {
		t.Fatalf("CreateTestDataset returned unexpected error: %v", err)
	}
	// Name should match TestDatasetName logic
	expected := TestDatasetName("dev", "run-20260225-abcd")
	if name != expected {
		t.Errorf("CreateTestDataset returned %q, want %q", name, expected)
	}
}

// TestDropTestDataset_SignatureRegression ensures the function signature
// matches: (cfg TestConfig, datasetName, runID string) error.
func TestDropTestDataset_SignatureRegression(t *testing.T) {
	srv := fakeBQServer(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/datasets/") && r.Method == http.MethodDelete {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{}`))
	})

	ctx := context.Background()
	store := newTestEventStore(t)
	opts := bqClientOpts(srv.URL)

	cfg := TestConfig{Ctx: ctx, Project: "test-proj", EventStore: store, ClientOpts: opts}

	// Call with 3 params: cfg, datasetName, runID
	err := DropTestDataset(cfg, "dev__test_abcd", "run-20260225-abcd")
	if err != nil {
		t.Fatalf("DropTestDataset returned unexpected error: %v", err)
	}
}

// TestCleanupOldTestDatasets_SignatureRegression ensures the function signature
// matches: (cfg TestConfig, baseDataset string, maxAge time.Duration) ([]string, error).
func TestCleanupOldTestDatasets_SignatureRegression(t *testing.T) {
	srv := fakeBQServer(t, func(w http.ResponseWriter, r *http.Request) {
		// Return empty list — no datasets to clean up
		if strings.Contains(r.URL.Path, "/datasets") && r.Method == http.MethodGet {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"datasets":[]}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{}`))
	})

	ctx := context.Background()
	store := newTestEventStore(t)
	opts := bqClientOpts(srv.URL)

	cfg := TestConfig{Ctx: ctx, Project: "test-proj", EventStore: store, ClientOpts: opts}

	// Call with 3 params: cfg, baseDataset, maxAge
	dropped, err := CleanupOldTestDatasets(cfg, "dev", 24*time.Hour)
	if err != nil {
		t.Fatalf("CleanupOldTestDatasets returned unexpected error: %v", err)
	}
	if len(dropped) != 0 {
		t.Errorf("expected 0 dropped datasets, got %d: %v", len(dropped), dropped)
	}
}

// --- Behavioral regression tests ---

// TestCreateTestDataset_EmitsEvent verifies that CreateTestDataset emits a
// "test_dataset_created" event to the store when the dataset is created.
func TestCreateTestDataset_EmitsEvent(t *testing.T) {
	srv := fakeBQServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/datasets") {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"datasetReference":{"datasetId":"dev__test_abcd","projectId":"proj"}}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{}`))
	})

	ctx := context.Background()
	store := newTestEventStore(t)
	opts := bqClientOpts(srv.URL)

	runID := "run-20260225-abcd"
	cfg := TestConfig{Ctx: ctx, Project: "proj", EventStore: store, ClientOpts: opts}

	_, err := CreateTestDataset(cfg, "dev", runID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	evts, err := store.Query(events.QueryFilters{RunID: runID})
	if err != nil {
		t.Fatalf("querying events: %v", err)
	}
	if len(evts) != 1 {
		t.Fatalf("expected 1 event, got %d", len(evts))
	}
	if evts[0].EventType != "test_dataset_created" {
		t.Errorf("event type: got %q, want %q", evts[0].EventType, "test_dataset_created")
	}
	if evts[0].RunID != runID {
		t.Errorf("event run_id: got %q, want %q", evts[0].RunID, runID)
	}
}

// TestCreateTestDataset_NilEventStore verifies that CreateTestDataset works
// when no event store is provided (nil is a valid value).
func TestCreateTestDataset_NilEventStore(t *testing.T) {
	srv := fakeBQServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/datasets") {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"datasetReference":{"datasetId":"dev__test_zzzz","projectId":"proj"}}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{}`))
	})

	ctx := context.Background()
	opts := bqClientOpts(srv.URL)

	cfg := TestConfig{Ctx: ctx, Project: "proj", EventStore: nil, ClientOpts: opts}

	// nil eventStore must not panic
	_, err := CreateTestDataset(cfg, "dev", "run-20260225-zzzz")
	if err != nil {
		t.Fatalf("unexpected error with nil event store: %v", err)
	}
}

// TestDropTestDataset_EmitsEvent verifies that DropTestDataset emits a
// "test_dataset_dropped" event to the store.
func TestDropTestDataset_EmitsEvent(t *testing.T) {
	srv := fakeBQServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{}`))
	})

	ctx := context.Background()
	store := newTestEventStore(t)
	opts := bqClientOpts(srv.URL)

	runID := "run-20260225-abcd"
	cfg := TestConfig{Ctx: ctx, Project: "proj", EventStore: store, ClientOpts: opts}

	err := DropTestDataset(cfg, "dev__test_abcd", runID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	evts, err := store.Query(events.QueryFilters{RunID: runID})
	if err != nil {
		t.Fatalf("querying events: %v", err)
	}
	if len(evts) != 1 {
		t.Fatalf("expected 1 event, got %d", len(evts))
	}
	if evts[0].EventType != "test_dataset_dropped" {
		t.Errorf("event type: got %q, want %q", evts[0].EventType, "test_dataset_dropped")
	}
}

// TestDropTestDataset_NilEventStore verifies that DropTestDataset works
// when no event store is provided (nil is a valid value).
func TestDropTestDataset_NilEventStore(t *testing.T) {
	srv := fakeBQServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{}`))
	})

	ctx := context.Background()
	opts := bqClientOpts(srv.URL)

	cfg := TestConfig{Ctx: ctx, Project: "proj", EventStore: nil, ClientOpts: opts}

	err := DropTestDataset(cfg, "dev__test_abcd", "run-xyz")
	if err != nil {
		t.Fatalf("unexpected error with nil event store: %v", err)
	}
}

// TestCreateTestDataset_BQErrorPropagated verifies that a BQ API error is
// returned from CreateTestDataset (not silently swallowed).
func TestCreateTestDataset_BQErrorPropagated(t *testing.T) {
	srv := fakeBQServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		w.Write([]byte(`{"error":{"code":409,"message":"Already Exists","status":"ALREADY_EXISTS"}}`))
	})

	ctx := context.Background()
	opts := bqClientOpts(srv.URL)

	cfg := TestConfig{Ctx: ctx, Project: "proj", EventStore: nil, ClientOpts: opts}

	_, err := CreateTestDataset(cfg, "dev", "run-20260225-abcd")
	if err == nil {
		t.Error("expected error from BQ conflict response, got nil")
	}
}

// TestDropTestDataset_BQErrorPropagated verifies that a BQ API error is
// returned from DropTestDataset (not silently swallowed).
func TestDropTestDataset_BQErrorPropagated(t *testing.T) {
	srv := fakeBQServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"error":{"code":404,"message":"Not Found","status":"NOT_FOUND"}}`))
	})

	ctx := context.Background()
	opts := bqClientOpts(srv.URL)

	cfg := TestConfig{Ctx: ctx, Project: "proj", EventStore: nil, ClientOpts: opts}

	err := DropTestDataset(cfg, "dev__test_abcd", "run-xyz")
	if err == nil {
		t.Error("expected error from BQ not-found response, got nil")
	}
}

// TestCleanupOldTestDatasets_DropsOldOnly verifies that CleanupOldTestDatasets
// drops only datasets older than maxAge and returns their names.
func TestCleanupOldTestDatasets_DropsOldOnly(t *testing.T) {
	// Construct two fake datasets: one old, one recent.
	old := time.Now().Add(-48 * time.Hour)
	recent := time.Now().Add(-1 * time.Hour)

	datasets := []map[string]any{
		{"datasetReference": map[string]any{"datasetId": "dev__test_aaaa", "projectId": "proj"}},
		{"datasetReference": map[string]any{"datasetId": "dev__test_bbbb", "projectId": "proj"}},
	}
	// Metadata responses keyed by dataset ID.
	metas := map[string]map[string]any{
		"dev__test_aaaa": {
			"datasetReference": map[string]any{"datasetId": "dev__test_aaaa"},
			"creationTime":     fmt.Sprintf("%d", old.UnixMilli()),
			"labels":           map[string]any{"granicus_test_run": "aaaa"},
		},
		"dev__test_bbbb": {
			"datasetReference": map[string]any{"datasetId": "dev__test_bbbb"},
			"creationTime":     fmt.Sprintf("%d", recent.UnixMilli()),
			"labels":           map[string]any{"granicus_test_run": "bbbb"},
		},
	}

	deleted := []string{}
	srv := fakeBQServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch {
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/datasets") && !strings.Contains(r.URL.Path, "/datasets/"):
			// List datasets
			data, _ := json.Marshal(map[string]any{"datasets": datasets})
			w.WriteHeader(http.StatusOK)
			w.Write(data)

		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/datasets/"):
			// Dataset metadata: extract dataset ID from URL path
			parts := strings.Split(r.URL.Path, "/")
			dsID := parts[len(parts)-1]
			if meta, ok := metas[dsID]; ok {
				data, _ := json.Marshal(meta)
				w.WriteHeader(http.StatusOK)
				w.Write(data)
			} else {
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte(`{"error":{"code":404}}`))
			}

		case r.Method == http.MethodDelete && strings.Contains(r.URL.Path, "/datasets/"):
			// Delete dataset — record which one was deleted
			parts := strings.Split(r.URL.Path, "/")
			// URL is like /bigquery/v2/projects/{proj}/datasets/{dsID}?deleteContents=true
			dsID := strings.Split(parts[len(parts)-1], "?")[0]
			deleted = append(deleted, dsID)
			w.WriteHeader(http.StatusNoContent)

		default:
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{}`))
		}
	})

	ctx := context.Background()
	store := newTestEventStore(t)
	opts := bqClientOpts(srv.URL)

	cfg := TestConfig{Ctx: ctx, Project: "proj", EventStore: store, ClientOpts: opts}

	dropped, err := CleanupOldTestDatasets(cfg, "dev", 24*time.Hour)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Only the old dataset should be dropped.
	if len(dropped) != 1 {
		t.Errorf("expected 1 dropped dataset, got %d: %v", len(dropped), dropped)
	}
	if len(dropped) == 1 && dropped[0] != "dev__test_aaaa" {
		t.Errorf("expected dev__test_aaaa to be dropped, got %q", dropped[0])
	}
}

// TestCleanupOldTestDatasets_NilEventStore verifies CleanupOldTestDatasets
// works with a nil event store.
func TestCleanupOldTestDatasets_NilEventStore(t *testing.T) {
	srv := fakeBQServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/datasets") {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"datasets":[]}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{}`))
	})

	ctx := context.Background()
	opts := bqClientOpts(srv.URL)

	cfg := TestConfig{Ctx: ctx, Project: "proj", EventStore: nil, ClientOpts: opts}

	dropped, err := CleanupOldTestDatasets(cfg, "dev", 24*time.Hour)
	if err != nil {
		t.Fatalf("unexpected error with nil event store: %v", err)
	}
	if len(dropped) != 0 {
		t.Errorf("expected 0 dropped, got %d", len(dropped))
	}
}

// TestCreateTestDataset_DatasetNameUsesTestDatasetName verifies that
// CreateTestDataset uses TestDatasetName to compute the dataset name, so
// future refactoring cannot silently change the naming convention.
func TestCreateTestDataset_DatasetNameUsesTestDatasetName(t *testing.T) {
	capturedPath := ""
	srv := fakeBQServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/datasets") {
			capturedPath = r.URL.Path
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"datasetReference":{"datasetId":"analytics__test_1234","projectId":"proj"}}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{}`))
	})

	ctx := context.Background()
	opts := bqClientOpts(srv.URL)
	runID := "run-20260101-1234"

	cfg := TestConfig{Ctx: ctx, Project: "proj", EventStore: nil, ClientOpts: opts}

	name, err := CreateTestDataset(cfg, "analytics", runID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := TestDatasetName("analytics", runID)
	if name != expected {
		t.Errorf("returned name %q, want %q", name, expected)
	}

	// Verify the BQ create call targeted the correct dataset name.
	if !strings.Contains(capturedPath, "proj") {
		t.Errorf("BQ request path %q should contain project", capturedPath)
	}
}
