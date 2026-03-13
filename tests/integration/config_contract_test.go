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
