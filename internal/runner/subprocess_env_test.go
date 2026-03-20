package runner

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/DataDecodeHQ/granicus/internal/config"
)

// envMap converts a []string env slice into a map for easier assertion.
func envMap(env []string) map[string]string {
	m := make(map[string]string, len(env))
	for _, e := range env {
		k, v, _ := strings.Cut(e, "=")
		m[k] = v
	}
	return m
}

func TestBuildSubprocessEnv_RequiredOnly(t *testing.T) {
	asset := &Asset{Name: "my_asset"}
	env := buildSubprocessEnv(SubprocessEnvConfig{
		Asset:        asset,
		ProjectRoot:  "/proj",
		RunID:        "run-001",
		MetadataPath: "/tmp/meta.json",
	})

	m := envMap(env)

	if m["GRANICUS_ASSET_NAME"] != "my_asset" {
		t.Errorf("GRANICUS_ASSET_NAME: got %q", m["GRANICUS_ASSET_NAME"])
	}
	if m["GRANICUS_RUN_ID"] != "run-001" {
		t.Errorf("GRANICUS_RUN_ID: got %q", m["GRANICUS_RUN_ID"])
	}
	if m["GRANICUS_PROJECT_ROOT"] != "/proj" {
		t.Errorf("GRANICUS_PROJECT_ROOT: got %q", m["GRANICUS_PROJECT_ROOT"])
	}
	if m["GRANICUS_METADATA_PATH"] != "/tmp/meta.json" {
		t.Errorf("GRANICUS_METADATA_PATH: got %q", m["GRANICUS_METADATA_PATH"])
	}

	if _, ok := m["GRANICUS_INTERVAL_START"]; ok {
		t.Error("GRANICUS_INTERVAL_START should not be set")
	}
	if _, ok := m["GRANICUS_DEST_RESOURCE"]; ok {
		t.Error("GRANICUS_DEST_RESOURCE should not be set")
	}
	if _, ok := m["GRANICUS_SOURCE_RESOURCE"]; ok {
		t.Error("GRANICUS_SOURCE_RESOURCE should not be set")
	}
	if _, ok := m["GRANICUS_REFS"]; ok {
		t.Error("GRANICUS_REFS should not be set")
	}
}

func TestBuildSubprocessEnv_WithInterval(t *testing.T) {
	asset := &Asset{
		Name:          "interval_asset",
		IntervalStart: "2025-01-01T00:00:00Z",
		IntervalEnd:   "2025-01-02T00:00:00Z",
	}
	env := buildSubprocessEnv(SubprocessEnvConfig{
		Asset:        asset,
		ProjectRoot:  "/proj",
		RunID:        "run-002",
		MetadataPath: "/tmp/meta.json",
	})

	m := envMap(env)

	if m["GRANICUS_INTERVAL_START"] != "2025-01-01T00:00:00Z" {
		t.Errorf("GRANICUS_INTERVAL_START: got %q", m["GRANICUS_INTERVAL_START"])
	}
	if m["GRANICUS_INTERVAL_END"] != "2025-01-02T00:00:00Z" {
		t.Errorf("GRANICUS_INTERVAL_END: got %q", m["GRANICUS_INTERVAL_END"])
	}
}

func TestBuildSubprocessEnv_WithConnections(t *testing.T) {
	asset := &Asset{Name: "conn_asset"}
	destConn := &config.ResourceConfig{
		Name:       "bq_dest",
		Type:       "bigquery",
		Properties: map[string]string{"project": "my-project", "dataset": "my_dataset"},
	}
	srcConn := &config.ResourceConfig{
		Name:       "pg_src",
		Type:       "postgres",
		Properties: map[string]string{"host": "localhost"},
	}

	env := buildSubprocessEnv(SubprocessEnvConfig{
		Asset:        asset,
		ProjectRoot:  "/proj",
		RunID:        "run-003",
		MetadataPath: "/tmp/meta.json",
		DestConn:     destConn,
		SrcConn:      srcConn,
	})

	m := envMap(env)

	destRaw, ok := m["GRANICUS_DEST_RESOURCE"]
	if !ok {
		t.Fatal("GRANICUS_DEST_RESOURCE missing")
	}
	var dest map[string]string
	if err := json.Unmarshal([]byte(destRaw), &dest); err != nil {
		t.Fatalf("GRANICUS_DEST_RESOURCE not valid JSON: %v", err)
	}
	if dest["name"] != "bq_dest" {
		t.Errorf("dest name: got %q", dest["name"])
	}
	if dest["type"] != "bigquery" {
		t.Errorf("dest type: got %q", dest["type"])
	}
	if dest["project"] != "my-project" {
		t.Errorf("dest project: got %q", dest["project"])
	}

	srcRaw, ok := m["GRANICUS_SOURCE_RESOURCE"]
	if !ok {
		t.Fatal("GRANICUS_SOURCE_RESOURCE missing")
	}
	var src map[string]string
	if err := json.Unmarshal([]byte(srcRaw), &src); err != nil {
		t.Fatalf("GRANICUS_SOURCE_RESOURCE not valid JSON: %v", err)
	}
	if src["name"] != "pg_src" {
		t.Errorf("src name: got %q", src["name"])
	}
	if src["host"] != "localhost" {
		t.Errorf("src host: got %q", src["host"])
	}
}

func TestBuildSubprocessEnv_WithRefs(t *testing.T) {
	asset := &Asset{Name: "refs_asset"}
	refs := map[string]string{
		"orders":   "proj.stg.orders",
		"payments": "proj.int.payments",
	}

	env := buildSubprocessEnv(SubprocessEnvConfig{
		Asset:        asset,
		ProjectRoot:  "/proj",
		RunID:        "run-004",
		MetadataPath: "/tmp/meta.json",
		Refs:         refs,
	})

	m := envMap(env)

	refsRaw, ok := m["GRANICUS_REFS"]
	if !ok {
		t.Fatal("GRANICUS_REFS missing")
	}
	var got map[string]string
	if err := json.Unmarshal([]byte(refsRaw), &got); err != nil {
		t.Fatalf("GRANICUS_REFS not valid JSON: %v", err)
	}
	if got["orders"] != "proj.stg.orders" {
		t.Errorf("orders ref: got %q", got["orders"])
	}
	if got["payments"] != "proj.int.payments" {
		t.Errorf("payments ref: got %q", got["payments"])
	}
}

func TestReadMetadata_Missing(t *testing.T) {
	meta, err := readMetadata("/nonexistent/path/meta.json")
	if err != nil {
		t.Fatalf("expected nil error for missing file, got %v", err)
	}
	if meta != nil {
		t.Errorf("expected nil map for missing file, got %v", meta)
	}
}

func TestReadMetadata_Valid(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "meta.json")
	content := `{"rows_loaded":"42","step":"done"}`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	meta, err := readMetadata(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if meta == nil {
		t.Fatal("expected non-nil map")
	}
	if meta["rows_loaded"] != "42" {
		t.Errorf("rows_loaded: got %q", meta["rows_loaded"])
	}
	if meta["step"] != "done" {
		t.Errorf("step: got %q", meta["step"])
	}
}

func TestReadMetadata_InvalidJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(path, []byte(`{not valid json`), 0644); err != nil {
		t.Fatal(err)
	}

	meta, err := readMetadata(path)
	if err == nil {
		t.Fatal("expected error for invalid JSON, got nil")
	}
	if meta != nil {
		t.Errorf("expected nil map on error, got %v", meta)
	}
}
