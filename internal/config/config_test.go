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

func TestDatasetForAsset(t *testing.T) {
	cfg := &PipelineConfig{
		Connections: map[string]*ConnectionConfig{
			"bq_main": {Name: "bq_main", Type: "bigquery", Properties: map[string]string{
				"project": "p", "dataset": "default_ds",
			}},
			"bq_staging": {Name: "bq_staging", Type: "bigquery", Properties: map[string]string{
				"project": "p", "dataset": "staging_ds",
			}},
		},
		Datasets: map[string]string{
			"staging":   "legacy_staging",
			"analytics": "legacy_analytics",
			"report":    "legacy_report",
		},
	}

	tests := []struct {
		name     string
		asset    AssetConfig
		defDS    string
		expected string
	}{
		{
			name:     "layer in datasets map",
			asset:    AssetConfig{Name: "orders", Layer: "analytics"},
			defDS:    "fallback",
			expected: "legacy_analytics",
		},
		{
			name:     "explicit destination_connection overrides layer",
			asset:    AssetConfig{Name: "stg", Layer: "analytics", DestinationConnection: "bq_staging"},
			defDS:    "fallback",
			expected: "staging_ds",
		},
		{
			name:     "no layer falls back to default",
			asset:    AssetConfig{Name: "extract", Layer: ""},
			defDS:    "fallback",
			expected: "fallback",
		},
		{
			name:     "layer not in datasets map falls back to default",
			asset:    AssetConfig{Name: "ent", Layer: "entity"},
			defDS:    "fallback",
			expected: "fallback",
		},
		{
			name:     "empty datasets block uses default",
			asset:    AssetConfig{Name: "x", Layer: "staging"},
			defDS:    "fallback",
			expected: "fallback",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := cfg
			if tt.name == "empty datasets block uses default" {
				c = &PipelineConfig{Connections: cfg.Connections}
			}
			got := c.DatasetForAsset(tt.asset, tt.defDS)
			if got != tt.expected {
				t.Errorf("DatasetForAsset() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestParseConfig_DatasetsBlock(t *testing.T) {
	cfg, err := LoadConfig(writeTestConfig(t, `
pipeline: test
connections:
  bq:
    type: bigquery
    project: p
    dataset: default_ds
datasets:
  staging: legacy_staging
  analytics: legacy_analytics
assets:
  - name: stg
    type: sql
    source: stg.sql
    destination_connection: bq
    layer: staging
  - name: rpt
    type: sql
    source: rpt.sql
    destination_connection: bq
    layer: analytics
`))
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Datasets) != 2 {
		t.Fatalf("expected 2 datasets, got %d", len(cfg.Datasets))
	}
	if cfg.Datasets["staging"] != "legacy_staging" {
		t.Errorf("staging dataset: %q", cfg.Datasets["staging"])
	}
	if cfg.Datasets["analytics"] != "legacy_analytics" {
		t.Errorf("analytics dataset: %q", cfg.Datasets["analytics"])
	}
}

func TestParseConfig_PoolValidation(t *testing.T) {
	// Valid pool config
	cfg, err := LoadConfig(writeTestConfig(t, `
pipeline: test
connections:
  bq:
    type: bigquery
    project: p
    dataset: d
pools:
  bq_pool:
    slots: 4
    timeout: 5m
    default_for: bigquery
assets:
  - name: x
    type: sql
    source: x.sql
    destination_connection: bq
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Pools["bq_pool"].Slots != 4 {
		t.Errorf("slots: %d", cfg.Pools["bq_pool"].Slots)
	}
	if cfg.Pools["bq_pool"].Timeout != "5m" {
		t.Errorf("timeout: %q", cfg.Pools["bq_pool"].Timeout)
	}
	if cfg.Pools["bq_pool"].DefaultFor != "bigquery" {
		t.Errorf("default_for: %q", cfg.Pools["bq_pool"].DefaultFor)
	}
}

func TestParseConfig_PoolSlotsZero(t *testing.T) {
	_, err := LoadConfig(writeTestConfig(t, `
pipeline: test
pools:
  bad:
    slots: 0
assets:
  - name: x
    type: shell
    source: x.sh
`))
	if err == nil {
		t.Error("expected error for pool slots=0")
	}
}

func TestParseConfig_PoolBadTimeout(t *testing.T) {
	_, err := LoadConfig(writeTestConfig(t, `
pipeline: test
pools:
  bad:
    slots: 1
    timeout: not-a-duration
assets:
  - name: x
    type: shell
    source: x.sh
`))
	if err == nil {
		t.Error("expected error for invalid pool timeout")
	}
}

func TestParseConfig_PoolReference(t *testing.T) {
	_, err := LoadConfig(writeTestConfig(t, `
pipeline: test
assets:
  - name: x
    type: shell
    source: x.sh
    pool: nonexistent
`))
	if err == nil {
		t.Error("expected error for non-existent pool reference")
	}
}

func TestParseConfig_PoolNone(t *testing.T) {
	// pool: none should not require the pool to exist
	_, err := LoadConfig(writeTestConfig(t, `
pipeline: test
assets:
  - name: x
    type: shell
    source: x.sh
    pool: none
`))
	if err != nil {
		t.Fatalf("pool=none should be valid: %v", err)
	}
}

func TestResolveAssetPool(t *testing.T) {
	pools := map[string]PoolConfig{
		"bq_pool": {Slots: 4, DefaultFor: "bigquery"},
		"pg_pool": {Slots: 2, DefaultFor: "postgres"},
	}
	conns := map[string]*ConnectionConfig{
		"bq":  {Name: "bq", Type: "bigquery", Properties: map[string]string{"project": "p", "dataset": "d"}},
		"pg":  {Name: "pg", Type: "postgres", Properties: map[string]string{"host": "h", "database": "db"}},
	}

	tests := []struct {
		name     string
		asset    AssetConfig
		expected string
	}{
		{"explicit pool", AssetConfig{Pool: "bq_pool"}, "bq_pool"},
		{"pool none", AssetConfig{Pool: "none"}, ""},
		{"auto from bq connection", AssetConfig{DestinationConnection: "bq"}, "bq_pool"},
		{"auto from pg connection", AssetConfig{DestinationConnection: "pg"}, "pg_pool"},
		{"no connection no pool", AssetConfig{}, ""},
		{"unknown connection", AssetConfig{DestinationConnection: "unknown"}, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ResolveAssetPool(tt.asset, pools, conns)
			if got != tt.expected {
				t.Errorf("ResolveAssetPool() = %q, want %q", got, tt.expected)
			}
		})
	}
}
