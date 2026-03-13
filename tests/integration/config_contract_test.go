package integration

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Andrew-DataDecode/Granicus/internal/config"
)

func TestPipelineDiscoveryFromRoot(t *testing.T) {
	tmpRoot := t.TempDir()

	pipelinesDir := filepath.Join(tmpRoot, "project", "granicus_pipeline")

	pipelines := []struct {
		name     string
		schedule string
	}{
		{"alpha", "0 6 * * *"},
		{"beta", "0 12 * * *"},
	}

	for _, p := range pipelines {
		dir := filepath.Join(pipelinesDir, p.name)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		yaml := "pipeline: " + p.name + "\nschedule: \"" + p.schedule + "\"\nconnections:\n  bq:\n    type: bigquery\n    project: test\n    dataset: test\nassets:\n  - name: x\n    type: sql\n    source: x.sql\n    destination_connection: bq\n"
		if err := os.WriteFile(filepath.Join(dir, "pipeline.yaml"), []byte(yaml), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "x.sql"), []byte("SELECT 1"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	t.Setenv("ANALYTEHEALTH_ROOT", tmpRoot)

	root := os.Getenv("ANALYTEHEALTH_ROOT")
	scanDir := filepath.Join(root, "project", "granicus_pipeline")

	entries, err := os.ReadDir(scanDir)
	if err != nil {
		t.Fatalf("reading pipeline dir: %v", err)
	}

	found := map[string]bool{}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		cfgPath := filepath.Join(scanDir, entry.Name(), "pipeline.yaml")
		if _, err := os.Stat(cfgPath); os.IsNotExist(err) {
			continue
		}
		cfg, err := config.LoadConfig(cfgPath)
		if err != nil {
			t.Errorf("loading %s: %v", entry.Name(), err)
			continue
		}
		found[cfg.Pipeline] = true
	}

	for _, p := range pipelines {
		if !found[p.name] {
			t.Errorf("pipeline %q not discovered", p.name)
		}
	}

	if len(found) != len(pipelines) {
		t.Errorf("expected %d pipelines, found %d", len(pipelines), len(found))
	}
}

func TestPipelineDiscoveryIgnoresNonPipelineDirs(t *testing.T) {
	tmpRoot := t.TempDir()

	pipelinesDir := filepath.Join(tmpRoot, "project", "granicus_pipeline")

	// Create one valid pipeline
	dir := filepath.Join(pipelinesDir, "valid")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	yaml := "pipeline: valid\nconnections:\n  bq:\n    type: bigquery\n    project: test\n    dataset: test\nassets:\n  - name: x\n    type: sql\n    source: x.sql\n    destination_connection: bq\n"
	if err := os.WriteFile(filepath.Join(dir, "pipeline.yaml"), []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "x.sql"), []byte("SELECT 1"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Create a directory without pipeline.yaml
	if err := os.MkdirAll(filepath.Join(pipelinesDir, "not_a_pipeline"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(pipelinesDir, "not_a_pipeline", "readme.txt"), []byte("hi"), 0o644); err != nil {
		t.Fatal(err)
	}

	t.Setenv("ANALYTEHEALTH_ROOT", tmpRoot)

	scanDir := filepath.Join(tmpRoot, "project", "granicus_pipeline")
	entries, err := os.ReadDir(scanDir)
	if err != nil {
		t.Fatalf("reading pipeline dir: %v", err)
	}

	count := 0
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		cfgPath := filepath.Join(scanDir, entry.Name(), "pipeline.yaml")
		if _, err := os.Stat(cfgPath); os.IsNotExist(err) {
			continue
		}
		_, err := config.LoadConfig(cfgPath)
		if err != nil {
			t.Errorf("loading %s: %v", entry.Name(), err)
			continue
		}
		count++
	}

	if count != 1 {
		t.Errorf("expected 1 pipeline, found %d", count)
	}
}

func TestCredentialPathResolution(t *testing.T) {
	tmpRoot := t.TempDir()

	// Create credential file at root/.credentials/bigquery/service.json
	credsDir := filepath.Join(tmpRoot, ".credentials", "bigquery")
	if err := os.MkdirAll(credsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	credsFile := filepath.Join(credsDir, "service.json")
	if err := os.WriteFile(credsFile, []byte(`{"type":"service_account"}`), 0o644); err != nil {
		t.Fatal(err)
	}

	// Create pipeline with relative credential path
	pipelineDir := filepath.Join(tmpRoot, "project", "granicus_pipeline", "test_pipeline")
	if err := os.MkdirAll(pipelineDir, 0o755); err != nil {
		t.Fatal(err)
	}

	relCredsPath := "../../../.credentials/bigquery/service.json"
	yaml := "pipeline: test_pipeline\nconnections:\n  bq:\n    type: bigquery\n    project: test\n    dataset: test\n    credentials: " + relCredsPath + "\nassets:\n  - name: x\n    type: sql\n    source: x.sql\n    destination_connection: bq\n"
	if err := os.WriteFile(filepath.Join(pipelineDir, "pipeline.yaml"), []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(pipelineDir, "x.sql"), []byte("SELECT 1"), 0o644); err != nil {
		t.Fatal(err)
	}

	t.Setenv("ANALYTEHEALTH_ROOT", tmpRoot)

	cfg, err := config.LoadConfig(filepath.Join(pipelineDir, "pipeline.yaml"))
	if err != nil {
		t.Fatalf("loading config: %v", err)
	}

	conn := cfg.Connections["bq"]
	if conn == nil {
		t.Fatal("bq connection not found")
	}

	creds := conn.Properties["credentials"]
	if creds == "" {
		t.Fatal("credentials property not found in connection")
	}

	// Resolve relative path from pipeline directory (same as engine does)
	resolved := creds
	if !filepath.IsAbs(creds) {
		resolved = filepath.Join(pipelineDir, creds)
	}
	resolved = filepath.Clean(resolved)

	if _, err := os.Stat(resolved); os.IsNotExist(err) {
		t.Errorf("credential file not found at resolved path: %s", resolved)
	}

	// Verify the resolved path is under ANALYTEHEALTH_ROOT
	expectedPath := filepath.Clean(credsFile)
	if resolved != expectedPath {
		t.Errorf("resolved path %q does not match expected %q", resolved, expectedPath)
	}
}

func TestAbsoluteCredentialPathPassedThrough(t *testing.T) {
	tmpRoot := t.TempDir()

	credsFile := filepath.Join(tmpRoot, "creds.json")
	if err := os.WriteFile(credsFile, []byte(`{"type":"service_account"}`), 0o644); err != nil {
		t.Fatal(err)
	}

	pipelineDir := filepath.Join(tmpRoot, "project", "granicus_pipeline", "abs_test")
	if err := os.MkdirAll(pipelineDir, 0o755); err != nil {
		t.Fatal(err)
	}

	yaml := "pipeline: abs_test\nconnections:\n  bq:\n    type: bigquery\n    project: test\n    dataset: test\n    credentials: " + credsFile + "\nassets:\n  - name: x\n    type: sql\n    source: x.sql\n    destination_connection: bq\n"
	if err := os.WriteFile(filepath.Join(pipelineDir, "pipeline.yaml"), []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(pipelineDir, "x.sql"), []byte("SELECT 1"), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg, err := config.LoadConfig(filepath.Join(pipelineDir, "pipeline.yaml"))
	if err != nil {
		t.Fatalf("loading config: %v", err)
	}

	creds := cfg.Connections["bq"].Properties["credentials"]
	if !filepath.IsAbs(creds) {
		t.Errorf("absolute credential path not preserved: %s", creds)
	}

	if _, err := os.Stat(creds); os.IsNotExist(err) {
		t.Errorf("credential file not found: %s", creds)
	}
}
