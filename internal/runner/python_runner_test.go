package runner

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/analytehealth/granicus/internal/config"
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
	r := NewPythonRunner(conn, nil)
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

	r := NewPythonRunner(nil, nil)
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

	r := NewPythonRunner(nil, nil)
	result := r.Run(&Asset{Name: "noop", Type: "python", Source: "noop.py"}, dir, "run1")

	if result.Status != "success" {
		t.Fatalf("expected success, got %s: %s", result.Status, result.Error)
	}
	// Metadata should be nil/empty when script doesn't write it
	if len(result.Metadata) > 0 {
		t.Errorf("expected no metadata, got %v", result.Metadata)
	}
}

func TestPythonRunner_MissingVenvFallback(t *testing.T) {
	dir := t.TempDir()
	// No .venv directory — should fall back to python3 on PATH
	script := `print("hello")`
	os.WriteFile(filepath.Join(dir, "fb.py"), []byte(script), 0644)

	r := NewPythonRunner(nil, nil)
	result := r.Run(&Asset{Name: "fb", Type: "python", Source: "fb.py"}, dir, "run1")

	if result.Status != "success" {
		t.Fatalf("expected success, got %s: %s", result.Status, result.Error)
	}
}
