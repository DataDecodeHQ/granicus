package runner

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

// captureHandler collects slog records for inspection in tests.
type captureHandler struct {
	mu      sync.Mutex
	records []slog.Record
	attrs   [][]slog.Attr
}

func (h *captureHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }

func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	var as []slog.Attr
	r.Attrs(func(a slog.Attr) bool {
		as = append(as, a)
		return true
	})
	h.records = append(h.records, r)
	h.attrs = append(h.attrs, as)
	return nil
}

func (h *captureHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return h
}

func (h *captureHandler) WithGroup(name string) slog.Handler {
	return h
}

func (h *captureHandler) lastMessage() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.records) == 0 {
		return ""
	}
	return h.records[len(h.records)-1].Message
}

func (h *captureHandler) lastAttrs() []slog.Attr {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.attrs) == 0 {
		return nil
	}
	return h.attrs[len(h.attrs)-1]
}

func attrValue(attrs []slog.Attr, key string) (slog.Value, bool) {
	for _, a := range attrs {
		if a.Key == key {
			return a.Value, true
		}
	}
	return slog.Value{}, false
}

func setupCapture(t *testing.T) *captureHandler {
	t.Helper()
	h := &captureHandler{}
	prev := slog.Default()
	slog.SetDefault(slog.New(h))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return h
}

func TestLogCredentialCrossing(t *testing.T) {
	h := setupCapture(t)

	LogCredentialCrossing("process-boundary", "bigquery-sa", "my_asset", "run-001")

	msg := h.lastMessage()
	if !strings.Contains(msg, "credential_crossing") {
		t.Errorf("expected message to contain credential_crossing, got %q", msg)
	}

	attrs := h.lastAttrs()
	cases := map[string]string{
		"boundary":        "process-boundary",
		"credential_type": "bigquery-sa",
		"asset":           "my_asset",
		"run_id":          "run-001",
	}
	for key, want := range cases {
		v, ok := attrValue(attrs, key)
		if !ok {
			t.Errorf("missing attr %q", key)
			continue
		}
		if got := v.String(); got != want {
			t.Errorf("attr %q: got %q, want %q", key, got, want)
		}
	}
}

func TestLogSubprocessLaunch(t *testing.T) {
	h := setupCapture(t)

	LogSubprocessLaunch("my_asset", "python", 8, true)

	msg := h.lastMessage()
	if !strings.Contains(msg, "subprocess_launch") {
		t.Errorf("expected message to contain subprocess_launch, got %q", msg)
	}

	attrs := h.lastAttrs()
	if v, ok := attrValue(attrs, "asset"); !ok || v.String() != "my_asset" {
		t.Errorf("asset attr: got %v, ok=%v", v, ok)
	}
	if v, ok := attrValue(attrs, "runner_type"); !ok || v.String() != "python" {
		t.Errorf("runner_type attr: got %v, ok=%v", v, ok)
	}
	if v, ok := attrValue(attrs, "env_var_count"); !ok || v.Int64() != 8 {
		t.Errorf("env_var_count attr: got %v, ok=%v", v, ok)
	}
	if v, ok := attrValue(attrs, "has_credentials"); !ok || !v.Bool() {
		t.Errorf("has_credentials attr: got %v, ok=%v", v, ok)
	}
}

func TestLogSubprocessComplete(t *testing.T) {
	h := setupCapture(t)

	LogSubprocessComplete("my_asset", "python", 0, 3*time.Second, true)

	msg := h.lastMessage()
	if !strings.Contains(msg, "subprocess_complete") {
		t.Errorf("expected message to contain subprocess_complete, got %q", msg)
	}

	attrs := h.lastAttrs()
	if v, ok := attrValue(attrs, "exit_code"); !ok || v.Int64() != 0 {
		t.Errorf("exit_code attr: got %v, ok=%v", v, ok)
	}
	if v, ok := attrValue(attrs, "duration_ms"); !ok || v.Int64() != 3000 {
		t.Errorf("duration_ms attr: got %v, ok=%v", v, ok)
	}
	if v, ok := attrValue(attrs, "has_metadata"); !ok || !v.Bool() {
		t.Errorf("has_metadata attr: got %v, ok=%v", v, ok)
	}
}

func TestLogSQLExecution(t *testing.T) {
	h := setupCapture(t)

	LogSQLExecution("my_asset", "dev_staging", true, 1024*1024)

	msg := h.lastMessage()
	if !strings.Contains(msg, "sql_execution") {
		t.Errorf("expected message to contain sql_execution, got %q", msg)
	}

	attrs := h.lastAttrs()
	if v, ok := attrValue(attrs, "dataset"); !ok || v.String() != "dev_staging" {
		t.Errorf("dataset attr: got %v, ok=%v", v, ok)
	}
	if v, ok := attrValue(attrs, "dry_run_passed"); !ok || !v.Bool() {
		t.Errorf("dry_run_passed attr: got %v, ok=%v", v, ok)
	}
	if v, ok := attrValue(attrs, "estimated_bytes"); !ok || v.Int64() != 1024*1024 {
		t.Errorf("estimated_bytes attr: got %v, ok=%v", v, ok)
	}
}

func TestLogCloudRunDispatch(t *testing.T) {
	h := setupCapture(t)

	LogCloudRunDispatch("my_asset", "us-central1-docker.pkg.dev/proj/img:latest", "v1.2.3", "run-xyz")

	msg := h.lastMessage()
	if !strings.Contains(msg, "cloud_run_dispatch") {
		t.Errorf("expected message to contain cloud_run_dispatch, got %q", msg)
	}

	attrs := h.lastAttrs()
	if v, ok := attrValue(attrs, "asset"); !ok || v.String() != "my_asset" {
		t.Errorf("asset attr: got %v, ok=%v", v, ok)
	}
	if v, ok := attrValue(attrs, "version"); !ok || v.String() != "v1.2.3" {
		t.Errorf("version attr: got %v, ok=%v", v, ok)
	}
	if v, ok := attrValue(attrs, "run_id"); !ok || v.String() != "run-xyz" {
		t.Errorf("run_id attr: got %v, ok=%v", v, ok)
	}
}

func TestLogFunctionsNoPanic(t *testing.T) {
	// Verify none of the functions panic with zero/empty values.
	setupCapture(t)

	LogCredentialCrossing("", "", "", "")
	LogSubprocessLaunch("", "", 0, false)
	LogSubprocessComplete("", "", -1, 0, false)
	LogSQLExecution("", "", false, 0)
	LogCloudRunDispatch("", "", "", "")
}
