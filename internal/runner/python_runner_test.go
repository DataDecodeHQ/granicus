package runner

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Andrew-DataDecode/Granicus/internal/config"
	"github.com/Andrew-DataDecode/Granicus/internal/events"
)

func TestPythonRunner_EnvVarInjection(t *testing.T) {
	dir := t.TempDir()
	script := `import os
print("DEST=" + os.environ.get("GRANICUS_DEST_CONNECTION", ""))
print("META=" + os.environ.get("GRANICUS_METADATA_PATH", ""))
print("NAME=" + os.environ.get("GRANICUS_ASSET_NAME", ""))
`
	os.WriteFile(filepath.Join(dir, "test.py"), []byte(script), 0644)

	conn := &config.ConnectionConfig{Name: "bq", Type: "bigquery", Properties: map[string]string{"project": "p"}}
	r := NewPythonRunner(conn, nil, nil, "")
	result := r.Run(&Asset{Name: "envtest", Type: "python", Source: "test.py"}, dir, "run1")

	if result.Status != "success" {
		t.Fatalf("expected success, got %s: %s\nstderr: %s", result.Status, result.Error, result.Stderr)
	}
	if !strings.Contains(result.Stdout, "DEST=") {
		t.Errorf("missing DEST env: %q", result.Stdout)
	}
	if !strings.Contains(result.Stdout, "META=") {
		t.Errorf("missing META env: %q", result.Stdout)
	}
	if !strings.Contains(result.Stdout, "NAME=envtest") {
		t.Errorf("missing NAME env: %q", result.Stdout)
	}
}

func TestPythonRunner_MetadataFile(t *testing.T) {
	dir := t.TempDir()
	script := `import os, json
path = os.environ["GRANICUS_METADATA_PATH"]
with open(path, "w") as f:
    json.dump({"rows_loaded": "100", "tables_created": "2"}, f)
print("done")
`
	os.WriteFile(filepath.Join(dir, "meta.py"), []byte(script), 0644)

	r := NewPythonRunner(nil, nil, nil, "")
	result := r.Run(&Asset{Name: "metatest", Type: "python", Source: "meta.py"}, dir, "run1")

	if result.Status != "success" {
		t.Fatalf("expected success, got %s: %s\nstderr: %s", result.Status, result.Error, result.Stderr)
	}
	if result.Metadata == nil {
		t.Fatal("expected metadata")
	}
	if result.Metadata["rows_loaded"] != "100" {
		t.Errorf("rows_loaded: %q", result.Metadata["rows_loaded"])
	}
	if result.Metadata["tables_created"] != "2" {
		t.Errorf("tables_created: %q", result.Metadata["tables_created"])
	}
}

func TestPythonRunner_NoMetadata(t *testing.T) {
	dir := t.TempDir()
	script := `print("no metadata written")`
	os.WriteFile(filepath.Join(dir, "noop.py"), []byte(script), 0644)

	r := NewPythonRunner(nil, nil, nil, "")
	result := r.Run(&Asset{Name: "noop", Type: "python", Source: "noop.py"}, dir, "run1")

	if result.Status != "success" {
		t.Fatalf("expected success, got %s: %s", result.Status, result.Error)
	}
	// Metadata should be nil/empty when script doesn't write it
	if len(result.Metadata) > 0 {
		t.Errorf("expected no metadata, got %v", result.Metadata)
	}
}

func TestPythonRunner_RefsEnvVar(t *testing.T) {
	dir := t.TempDir()
	script := `import os, json
refs_raw = os.environ.get("GRANICUS_REFS", "")
print("REFS=" + refs_raw)
if refs_raw:
    refs = json.loads(refs_raw)
    for k, v in sorted(refs.items()):
        print(f"REF:{k}={v}")
`
	os.WriteFile(filepath.Join(dir, "refs.py"), []byte(script), 0644)

	refFunc := BuildRefFunc(RefContext{
		Assets: []RefAsset{
			{Name: "orders", Layer: "staging"},
			{Name: "payments", Layer: "intermediate"},
		},
		Datasets:       map[string]string{"staging": "proj.stg_data", "intermediate": "proj.int_data"},
		DefaultDataset: "proj.default",
	})

	r := NewPythonRunner(nil, nil, nil, "")
	r.RefFunc = refFunc
	result := r.Run(&Asset{
		Name:      "my_asset",
		Type:      "python",
		Source:    "refs.py",
		DependsOn: []string{"orders", "payments"},
	}, dir, "run1")

	if result.Status != "success" {
		t.Fatalf("expected success, got %s: %s\nstderr: %s", result.Status, result.Error, result.Stderr)
	}

	if !strings.Contains(result.Stdout, "REFS=") {
		t.Fatalf("missing REFS output: %q", result.Stdout)
	}
	refsLine := ""
	for _, line := range strings.Split(result.Stdout, "\n") {
		if strings.HasPrefix(line, "REFS=") {
			refsLine = strings.TrimPrefix(line, "REFS=")
			break
		}
	}
	var refs map[string]string
	if err := json.Unmarshal([]byte(refsLine), &refs); err != nil {
		t.Fatalf("GRANICUS_REFS not valid JSON: %v\nraw: %q", err, refsLine)
	}
	if refs["orders"] != "proj.stg_data.orders" {
		t.Errorf("orders ref: got %q, want %q", refs["orders"], "proj.stg_data.orders")
	}
	if refs["payments"] != "proj.int_data.payments" {
		t.Errorf("payments ref: got %q, want %q", refs["payments"], "proj.int_data.payments")
	}
	for k, v := range refs {
		if strings.Contains(v, "`") {
			t.Errorf("ref %q contains backticks: %q", k, v)
		}
	}
}

func TestPythonRunner_MissingVenvFallback(t *testing.T) {
	dir := t.TempDir()
	// No .venv directory — should fall back to python3 on PATH
	script := `print("hello")`
	os.WriteFile(filepath.Join(dir, "fb.py"), []byte(script), 0644)

	r := NewPythonRunner(nil, nil, nil, "")
	result := r.Run(&Asset{Name: "fb", Type: "python", Source: "fb.py"}, dir, "run1")

	if result.Status != "success" {
		t.Fatalf("expected success, got %s: %s", result.Status, result.Error)
	}
}

func newTestEventStore(t *testing.T) *events.Store {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "events.db")
	store, err := events.New(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func TestPythonRunner_ProgressMonitoring(t *testing.T) {
	dir := t.TempDir()
	script := `import os, json
path = os.environ["GRANICUS_METADATA_PATH"]
with open(path, "w") as f:
    json.dump({"step": "complete", "detail": "done", "row_count": "100"}, f)
`
	os.WriteFile(filepath.Join(dir, "progress.py"), []byte(script), 0644)

	store := newTestEventStore(t)
	r := NewPythonRunner(nil, nil, store, "test_pipeline")
	result := r.Run(&Asset{Name: "progress", Type: "python", Source: "progress.py"}, dir, "run_prog")

	if result.Status != "success" {
		t.Fatalf("expected success, got %s: %s\nstderr: %s", result.Status, result.Error, result.Stderr)
	}
	if result.Metadata["row_count"] != "100" {
		t.Errorf("expected row_count=100, got %q", result.Metadata["row_count"])
	}
}

func TestPythonRunner_ProgressMonitoring_NoEventStore(t *testing.T) {
	dir := t.TempDir()
	script := `import os, json
path = os.environ["GRANICUS_METADATA_PATH"]
with open(path, "w") as f:
    json.dump({"step": "done", "row_count": "10"}, f)
`
	os.WriteFile(filepath.Join(dir, "noes.py"), []byte(script), 0644)

	r := NewPythonRunner(nil, nil, nil, "")
	result := r.Run(&Asset{Name: "noes", Type: "python", Source: "noes.py"}, dir, "run1")
	if result.Status != "success" {
		t.Fatalf("expected success, got %s: %s", result.Status, result.Error)
	}
	if result.Metadata["row_count"] != "10" {
		t.Errorf("expected row_count=10, got %q", result.Metadata["row_count"])
	}
}

func TestPythonRunner_MonitorProgressDirect(t *testing.T) {
	store := newTestEventStore(t)
	r := NewPythonRunner(nil, nil, store, "test_pipeline")

	metadataFile, _ := os.CreateTemp("", "granicus-test-meta-*.json")
	metadataPath := metadataFile.Name()
	metadataFile.Close()
	defer os.Remove(metadataPath)

	meta := map[string]string{"step": "loading", "detail": "processing", "row_count": "42"}
	data, _ := json.Marshal(meta)
	os.WriteFile(metadataPath, data, 0644)

	done := make(chan struct{})
	go r.monitorProgress(metadataPath, "test_asset", "run_test", done)
	time.Sleep(50 * time.Millisecond)
	close(done)
	time.Sleep(50 * time.Millisecond)
}

func TestPythonRunner_MonitorProgressMissingFile(t *testing.T) {
	store := newTestEventStore(t)
	r := NewPythonRunner(nil, nil, store, "test_pipeline")

	done := make(chan struct{})
	go r.monitorProgress("/nonexistent/path", "test_asset", "run1", done)
	time.Sleep(50 * time.Millisecond)
	close(done)
	time.Sleep(50 * time.Millisecond)
}
