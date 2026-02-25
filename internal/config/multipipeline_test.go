package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadMultiPipelineConfig(t *testing.T) {
	dir := t.TempDir()

	// Write pipeline configs
	os.WriteFile(filepath.Join(dir, "pipeline_a.yaml"), []byte(`
pipeline: pipeline_a
assets:
  - name: extract
    type: shell
    source: extract.sh
`), 0644)

	os.WriteFile(filepath.Join(dir, "pipeline_b.yaml"), []byte(`
pipeline: pipeline_b
assets:
  - name: transform
    type: shell
    source: transform.sh
`), 0644)

	// Write multi-pipeline config
	multiCfgPath := filepath.Join(dir, "granicus.yaml")
	os.WriteFile(multiCfgPath, []byte(`
pipelines:
  - config: pipeline_a.yaml
  - config: pipeline_b.yaml
cross_dependencies:
  - upstream: pipeline_a
    downstream: pipeline_b
    type: blocks
`), 0644)

	cfg, err := LoadMultiPipelineConfig(multiCfgPath)
	if err != nil {
		t.Fatal(err)
	}

	if len(cfg.Pipelines) != 2 {
		t.Fatalf("expected 2 pipelines, got %d", len(cfg.Pipelines))
	}
	if len(cfg.CrossDependencies) != 1 {
		t.Fatalf("expected 1 cross dep, got %d", len(cfg.CrossDependencies))
	}
}

func TestLoadAllPipelines(t *testing.T) {
	dir := t.TempDir()

	os.WriteFile(filepath.Join(dir, "a.yaml"), []byte(`
pipeline: a
assets:
  - name: x
    type: shell
    source: x.sh
`), 0644)

	os.WriteFile(filepath.Join(dir, "granicus.yaml"), []byte(`
pipelines:
  - config: a.yaml
`), 0644)

	pipelines, err := LoadAllPipelines(filepath.Join(dir, "granicus.yaml"))
	if err != nil {
		t.Fatal(err)
	}

	if len(pipelines) != 1 {
		t.Fatalf("expected 1 pipeline, got %d", len(pipelines))
	}
	if pipelines[0].Pipeline != "a" {
		t.Errorf("expected pipeline name 'a', got %q", pipelines[0].Pipeline)
	}
}

func TestLoadMultiPipelineConfig_Empty(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "granicus.yaml")
	os.WriteFile(path, []byte(`pipelines: []`), 0644)

	_, err := LoadMultiPipelineConfig(path)
	if err == nil {
		t.Error("expected error for empty pipelines")
	}
}
