package runner

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDLTRunner_Metadata(t *testing.T) {
	dir := t.TempDir()
	script := `import os, json
path = os.environ["GRANICUS_METADATA_PATH"]
with open(path, "w") as f:
    json.dump({"rows_loaded": "50", "tables_created": "1", "load_duration": "2.5s"}, f)
print("dlt done")
`
	os.WriteFile(filepath.Join(dir, "dlt_test.py"), []byte(script), 0644)

	r := NewDLTRunner(nil, nil)
	result := r.Run(&Asset{Name: "dlt_test", Type: "dlt", Source: "dlt_test.py"}, dir, "run1")

	if result.Status != "success" {
		t.Fatalf("expected success, got %s: %s", result.Status, result.Error)
	}
	if result.Metadata["rows_loaded"] != "50" {
		t.Errorf("rows_loaded: %q", result.Metadata["rows_loaded"])
	}
	if result.Metadata["tables_created"] != "1" {
		t.Errorf("tables_created: %q", result.Metadata["tables_created"])
	}
}

func TestDLTRunner_FallbackNoMetadata(t *testing.T) {
	dir := t.TempDir()
	script := `print("no dlt metadata")`
	os.WriteFile(filepath.Join(dir, "plain.py"), []byte(script), 0644)

	r := NewDLTRunner(nil, nil)
	result := r.Run(&Asset{Name: "plain", Type: "dlt", Source: "plain.py"}, dir, "run1")

	if result.Status != "success" {
		t.Fatalf("expected success, got %s: %s", result.Status, result.Error)
	}
	if len(result.Metadata) > 0 {
		t.Errorf("expected no metadata, got %v", result.Metadata)
	}
}
