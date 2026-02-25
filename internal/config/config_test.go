package config

import (
	"os"
	"path/filepath"
	"testing"
)

func writeTestConfig(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestParseConfig_ValidYAML(t *testing.T) {
	cfg, err := LoadConfig(writeTestConfig(t, `
pipeline: revenue_daily
max_parallel: 5
connections:
  bq_main:
    type: bigquery
    project: my-project
    dataset: my_dataset
assets:
  - name: extract
    type: shell
    source: scripts/extract.sh
  - name: transform
    type: sql
    source: sql/transform.sql
    destination_connection: bq_main
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Pipeline != "revenue_daily" {
		t.Errorf("got pipeline %q", cfg.Pipeline)
	}
	if cfg.MaxParallel != 5 {
		t.Errorf("got max_parallel %d", cfg.MaxParallel)
	}
	if len(cfg.Assets) != 2 {
		t.Fatalf("got %d assets", len(cfg.Assets))
	}
	if cfg.Assets[0].Name != "extract" || cfg.Assets[0].Type != "shell" {
		t.Errorf("asset 0: %+v", cfg.Assets[0])
	}
	if cfg.Assets[1].Name != "transform" || cfg.Assets[1].Type != "sql" {
		t.Errorf("asset 1: %+v", cfg.Assets[1])
	}
	if cfg.Assets[1].DestinationConnection != "bq_main" {
		t.Errorf("asset 1 dest conn: %q", cfg.Assets[1].DestinationConnection)
	}
	if conn, ok := cfg.Connections["bq_main"]; !ok {
		t.Error("missing bq_main connection")
	} else if conn.Name != "bq_main" {
		t.Errorf("connection name: %q", conn.Name)
	}
}

func TestParseConfig_MissingFields(t *testing.T) {
	// Missing pipeline name
	_, err := LoadConfig(writeTestConfig(t, `
assets:
  - name: x
    type: shell
    source: x.sh
`))
	if err == nil {
		t.Error("expected error for missing pipeline")
	}

	// No assets
	_, err = LoadConfig(writeTestConfig(t, `
pipeline: test
assets: []
`))
	if err == nil {
		t.Error("expected error for no assets")
	}

	// Missing source
	_, err = LoadConfig(writeTestConfig(t, `
pipeline: test
assets:
  - name: x
    type: shell
`))
	if err == nil {
		t.Error("expected error for missing source")
	}

	// Invalid type
	_, err = LoadConfig(writeTestConfig(t, `
pipeline: test
assets:
  - name: x
    type: invalid
    source: x.sh
`))
	if err == nil {
		t.Error("expected error for invalid type")
	}
}

func TestParseConfig_Defaults(t *testing.T) {
	cfg, err := LoadConfig(writeTestConfig(t, `
pipeline: test
assets:
  - type: shell
    source: scripts/my_script.sh
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxParallel != 10 {
		t.Errorf("expected default max_parallel=10, got %d", cfg.MaxParallel)
	}
	if cfg.Assets[0].Name != "my_script" {
		t.Errorf("expected inferred name 'my_script', got %q", cfg.Assets[0].Name)
	}
}

func TestParseConfig_DuplicateNames(t *testing.T) {
	_, err := LoadConfig(writeTestConfig(t, `
pipeline: test
assets:
  - name: dup
    type: shell
    source: a.sh
  - name: dup
    type: shell
    source: b.sh
`))
	if err == nil {
		t.Error("expected error for duplicate names")
	}
}

func TestParseConfig_Phase0Compat(t *testing.T) {
	// Phase 0 config with no connections still works
	cfg, err := LoadConfig(writeTestConfig(t, `
pipeline: simple
assets:
  - name: hello
    type: shell
    source: hello.sh
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Pipeline != "simple" {
		t.Errorf("got pipeline %q", cfg.Pipeline)
	}
	if cfg.Connections != nil && len(cfg.Connections) > 0 {
		t.Error("expected no connections")
	}
}

func TestParseConfig_NonExistentConnection(t *testing.T) {
	_, err := LoadConfig(writeTestConfig(t, `
pipeline: test
assets:
  - name: x
    type: shell
    source: x.sh
    destination_connection: missing
`))
	if err == nil {
		t.Error("expected error for non-existent connection")
	}
}

func TestParseConfig_SQLRequiresDestConnection(t *testing.T) {
	_, err := LoadConfig(writeTestConfig(t, `
pipeline: test
assets:
  - name: x
    type: sql
    source: x.sql
`))
	if err == nil {
		t.Error("expected error for sql without destination_connection")
	}
}

func TestValidateConnections_MissingProperty(t *testing.T) {
	_, err := LoadConfig(writeTestConfig(t, `
pipeline: test
connections:
  bq:
    type: bigquery
    project: my-project
assets:
  - name: x
    type: sql
    source: x.sql
    destination_connection: bq
`))
	if err == nil {
		t.Error("expected error for missing dataset")
	}
}

func TestValidateConnections_UnknownType(t *testing.T) {
	// Unknown connection type should pass through without error
	cfg, err := LoadConfig(writeTestConfig(t, `
pipeline: test
connections:
  custom:
    type: custom_db
    host: localhost
assets:
  - name: x
    type: shell
    source: x.sh
    destination_connection: custom
`))
	if err != nil {
		t.Fatalf("unknown type should not error: %v", err)
	}
	if cfg.Connections["custom"].Type != "custom_db" {
		t.Errorf("type: %q", cfg.Connections["custom"].Type)
	}
}

func TestParseConfig_ConnectionProperties(t *testing.T) {
	cfg, err := LoadConfig(writeTestConfig(t, `
pipeline: test
connections:
  bq:
    type: bigquery
    project: my-project
    dataset: my_ds
    credentials: /path/to/creds.json
assets:
  - name: x
    type: sql
    source: x.sql
    destination_connection: bq
`))
	if err != nil {
		t.Fatal(err)
	}
	conn := cfg.Connections["bq"]
	if conn.Type != "bigquery" {
		t.Errorf("type: %q", conn.Type)
	}
	if conn.Properties["project"] != "my-project" {
		t.Errorf("project: %q", conn.Properties["project"])
	}
	if conn.Properties["dataset"] != "my_ds" {
		t.Errorf("dataset: %q", conn.Properties["dataset"])
	}
}

func TestParseConfig_LayerValidation(t *testing.T) {
	// Valid layers should pass
	_, err := LoadConfig(writeTestConfig(t, `
pipeline: test
assets:
  - name: x
    type: shell
    source: x.sh
    layer: staging
  - name: y
    type: shell
    source: y.sh
    layer: entity
`))
	if err != nil {
		t.Fatalf("valid layers should pass: %v", err)
	}

	// Invalid layer should fail
	_, err = LoadConfig(writeTestConfig(t, `
pipeline: test
assets:
  - name: x
    type: shell
    source: x.sh
    layer: bogus
`))
	if err == nil {
		t.Error("expected error for invalid layer")
	}
}

func TestParseConfig_GrainAndDefaultChecks(t *testing.T) {
	cfg, err := LoadConfig(writeTestConfig(t, `
pipeline: test
connections:
  bq:
    type: bigquery
    project: p
    dataset: d
assets:
  - name: stg
    type: sql
    source: stg.sql
    destination_connection: bq
    layer: staging
    grain: order_id
    default_checks: true
  - name: ent
    type: sql
    source: ent.sql
    destination_connection: bq
    layer: entity
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Assets[0].Grain != "order_id" {
		t.Errorf("grain: %q", cfg.Assets[0].Grain)
	}
	if cfg.Assets[0].DefaultChecks == nil || *cfg.Assets[0].DefaultChecks != true {
		t.Errorf("default_checks: %v", cfg.Assets[0].DefaultChecks)
	}
	if cfg.Assets[1].DefaultChecks != nil {
		t.Errorf("expected nil default_checks for ent, got %v", cfg.Assets[1].DefaultChecks)
	}
}
