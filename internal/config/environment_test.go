package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadEnvironmentConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "granicus-env.yaml")
	os.WriteFile(path, []byte(`
environments:
  dev:
    prefix: dev_
    connections:
      bq:
        type: bigquery
        project: my-dev-project
        dataset: dev_dataset
  prod:
    prefix: ""
    connections:
      bq:
        type: bigquery
        project: my-prod-project
        dataset: prod_dataset
`), 0644)

	cfg, err := LoadEnvironmentConfig(path)
	if err != nil {
		t.Fatal(err)
	}

	if len(cfg.Environments) != 2 {
		t.Fatalf("expected 2 environments, got %d", len(cfg.Environments))
	}
	if cfg.Environments["dev"].Prefix != "dev_" {
		t.Errorf("dev prefix: %q", cfg.Environments["dev"].Prefix)
	}
	if cfg.Environments["prod"].Connections["bq"].Properties["project"] != "my-prod-project" {
		t.Errorf("prod project: %q", cfg.Environments["prod"].Connections["bq"].Properties["project"])
	}
}

func TestMergeEnvironment(t *testing.T) {
	base := &PipelineConfig{
		Pipeline:    "test",
		MaxParallel: 5,
		Connections: map[string]*ConnectionConfig{
			"bq": {Name: "bq", Type: "bigquery", Properties: map[string]string{
				"project": "base-project",
				"dataset": "base_ds",
			}},
		},
		Assets: []AssetConfig{
			{Name: "a", Type: "shell", Source: "a.sh"},
		},
	}

	envCfg := &EnvironmentConfig{
		Environments: map[string]*EnvironmentOverride{
			"dev": {
				Prefix: "dev_",
				Connections: map[string]*ConnectionConfig{
					"bq": {Type: "bigquery", Properties: map[string]string{
						"project": "dev-project",
					}},
				},
			},
		},
	}

	merged, err := MergeEnvironment(base, envCfg, "dev")
	if err != nil {
		t.Fatal(err)
	}

	// project overridden
	if merged.Connections["bq"].Properties["project"] != "dev-project" {
		t.Errorf("project: %q", merged.Connections["bq"].Properties["project"])
	}
	// dataset kept from base
	if merged.Connections["bq"].Properties["dataset"] != "base_ds" {
		t.Errorf("dataset: %q", merged.Connections["bq"].Properties["dataset"])
	}
	// prefix set
	if merged.Prefix != "dev_" {
		t.Errorf("prefix: %q", merged.Prefix)
	}
	// base not mutated
	if base.Connections["bq"].Properties["project"] != "base-project" {
		t.Error("base was mutated")
	}
}

func TestMergeEnvironment_MissingEnv(t *testing.T) {
	base := &PipelineConfig{Pipeline: "test"}
	envCfg := &EnvironmentConfig{
		Environments: map[string]*EnvironmentOverride{},
	}

	_, err := MergeEnvironment(base, envCfg, "nonexistent")
	if err == nil {
		t.Error("expected error for missing environment")
	}
}

func TestStateDBPath(t *testing.T) {
	if got := StateDBPath("/proj", "dev"); got != "/proj/.granicus/dev-state.db" {
		t.Errorf("dev: %q", got)
	}
	if got := StateDBPath("/proj", "prod"); got != "/proj/.granicus/prod-state.db" {
		t.Errorf("prod: %q", got)
	}
	if got := StateDBPath("/proj", ""); got != "/proj/.granicus/dev-state.db" {
		t.Errorf("default: %q", got)
	}
}
