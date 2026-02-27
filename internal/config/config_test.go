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

func TestParseConfig_SourceExtendedFields(t *testing.T) {
	cfg, err := LoadConfig(writeTestConfig(t, `
pipeline: test
connections:
  bq:
    type: bigquery
    project: p
    dataset: d
sources:
  orders:
    connection: bq
    identifier: project.dataset.orders
    primary_key: order_id
    expected_freshness: 24h
    expected_columns:
      - order_id
      - customer_id
      - created_at
assets:
  - name: x
    type: sql
    source: x.sql
    destination_connection: bq
`))
	if err != nil {
		t.Fatal(err)
	}
	src := cfg.Sources["orders"]
	if src.PrimaryKey != "order_id" {
		t.Errorf("primary_key: %q", src.PrimaryKey)
	}
	if src.ExpectedFresh != "24h" {
		t.Errorf("expected_freshness: %q", src.ExpectedFresh)
	}
	if len(src.ExpectedColumns) != 3 || src.ExpectedColumns[0] != "order_id" {
		t.Errorf("expected_columns: %v", src.ExpectedColumns)
	}
}

func TestParseConfig_SourceBadFreshness(t *testing.T) {
	_, err := LoadConfig(writeTestConfig(t, `
pipeline: test
connections:
  bq:
    type: bigquery
    project: p
    dataset: d
sources:
  orders:
    connection: bq
    identifier: project.dataset.orders
    expected_freshness: not-a-duration
assets:
  - name: x
    type: sql
    source: x.sql
    destination_connection: bq
`))
	if err == nil {
		t.Error("expected error for invalid expected_freshness")
	}
}

func TestParseConfig_ForeignKeys(t *testing.T) {
	cfg, err := LoadConfig(writeTestConfig(t, `
pipeline: test
connections:
  bq:
    type: bigquery
    project: p
    dataset: d
assets:
  - name: orders
    type: sql
    source: orders.sql
    destination_connection: bq
    foreign_keys:
      - column: customer_id
        references: customers.customer_id
      - column: product_id
        references: products.product_id
        nullable: true
`))
	if err != nil {
		t.Fatal(err)
	}
	fks := cfg.Assets[0].ForeignKeys
	if len(fks) != 2 {
		t.Fatalf("expected 2 foreign keys, got %d", len(fks))
	}
	if fks[0].Column != "customer_id" || fks[0].References != "customers.customer_id" || fks[0].Nullable {
		t.Errorf("fk[0]: %+v", fks[0])
	}
	if fks[1].Column != "product_id" || fks[1].References != "products.product_id" || !fks[1].Nullable {
		t.Errorf("fk[1]: %+v", fks[1])
	}
}

func TestParseConfig_ForeignKeyBadReferences(t *testing.T) {
	_, err := LoadConfig(writeTestConfig(t, `
pipeline: test
connections:
  bq:
    type: bigquery
    project: p
    dataset: d
assets:
  - name: orders
    type: sql
    source: orders.sql
    destination_connection: bq
    foreign_keys:
      - column: customer_id
        references: customers
`))
	if err == nil {
		t.Error("expected error for references without table.column format")
	}
}

func TestParseConfig_CompletenessConfig(t *testing.T) {
	cfg, err := LoadConfig(writeTestConfig(t, `
pipeline: test
connections:
  bq:
    type: bigquery
    project: p
    dataset: d
assets:
  - name: orders
    type: sql
    source: orders.sql
    destination_connection: bq
    completeness:
      source_table: raw_orders
      source_pk: order_id
      exclusions:
        - table: cancelled
          pk: order_id
          filter: "status = 'cancelled'"
      tolerance: 0.05
`))
	if err != nil {
		t.Fatal(err)
	}
	c := cfg.Assets[0].Completeness
	if c == nil {
		t.Fatal("expected completeness config")
	}
	if c.SourceTable != "raw_orders" {
		t.Errorf("source_table: %q", c.SourceTable)
	}
	if c.SourcePK != "order_id" {
		t.Errorf("source_pk: %q", c.SourcePK)
	}
	if len(c.Exclusions) != 1 || c.Exclusions[0].Table != "cancelled" {
		t.Errorf("exclusions: %+v", c.Exclusions)
	}
	if c.Tolerance == nil || *c.Tolerance != 0.05 {
		t.Errorf("tolerance: %v", c.Tolerance)
	}
}

func TestParseConfig_CompletenessDefaults(t *testing.T) {
	cfg, err := LoadConfig(writeTestConfig(t, `
pipeline: test
connections:
  bq:
    type: bigquery
    project: p
    dataset: d
assets:
  - name: orders
    type: sql
    source: orders.sql
    destination_connection: bq
    completeness:
      source_table: raw_orders
      source_pk: order_id
`))
	if err != nil {
		t.Fatal(err)
	}
	c := cfg.Assets[0].Completeness
	if c.Tolerance == nil || *c.Tolerance != 0.01 {
		t.Errorf("expected default tolerance 0.01, got %v", c.Tolerance)
	}
	if cfg.Assets[0].MinRetentionRatio == nil || *cfg.Assets[0].MinRetentionRatio != 0.5 {
		t.Errorf("expected default min_retention_ratio 0.5, got %v", cfg.Assets[0].MinRetentionRatio)
	}
}

func TestParseConfig_CompletenessValidation(t *testing.T) {
	// Missing source_table
	_, err := LoadConfig(writeTestConfig(t, `
pipeline: test
connections:
  bq:
    type: bigquery
    project: p
    dataset: d
assets:
  - name: orders
    type: sql
    source: orders.sql
    destination_connection: bq
    completeness:
      source_pk: order_id
`))
	if err == nil {
		t.Error("expected error for missing completeness source_table")
	}

	// Missing source_pk
	_, err = LoadConfig(writeTestConfig(t, `
pipeline: test
connections:
  bq:
    type: bigquery
    project: p
    dataset: d
assets:
  - name: orders
    type: sql
    source: orders.sql
    destination_connection: bq
    completeness:
      source_table: raw_orders
`))
	if err == nil {
		t.Error("expected error for missing completeness source_pk")
	}
}

func TestParseConfig_AssetUpstream(t *testing.T) {
	cfg, err := LoadConfig(writeTestConfig(t, `
pipeline: test
connections:
  bq:
    type: bigquery
    project: p
    dataset: d
assets:
  - name: stg_orders
    type: sql
    source: stg.sql
    destination_connection: bq
  - name: ent_orders
    type: sql
    source: ent.sql
    destination_connection: bq
    upstream:
      - stg_orders
      - stg_customers
    primary_upstream: stg_orders
`))
	if err != nil {
		t.Fatal(err)
	}
	a := cfg.Assets[1]
	if len(a.Upstream) != 2 || a.Upstream[0] != "stg_orders" {
		t.Errorf("upstream: %v", a.Upstream)
	}
	if a.PrimaryUpstream != "stg_orders" {
		t.Errorf("primary_upstream: %q", a.PrimaryUpstream)
	}
}

func TestParseConfig_FanOutCheck(t *testing.T) {
	cfg, err := LoadConfig(writeTestConfig(t, `
pipeline: test
connections:
  bq:
    type: bigquery
    project: p
    dataset: d
assets:
  - name: orders
    type: sql
    source: orders.sql
    destination_connection: bq
    fan_out_check: true
  - name: lines
    type: sql
    source: lines.sql
    destination_connection: bq
    fan_out_check: false
  - name: plain
    type: sql
    source: plain.sql
    destination_connection: bq
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Assets[0].FanOutCheck == nil || *cfg.Assets[0].FanOutCheck != true {
		t.Errorf("fan_out_check[0]: %v", cfg.Assets[0].FanOutCheck)
	}
	if cfg.Assets[1].FanOutCheck == nil || *cfg.Assets[1].FanOutCheck != false {
		t.Errorf("fan_out_check[1]: %v", cfg.Assets[1].FanOutCheck)
	}
	if cfg.Assets[2].FanOutCheck != nil {
		t.Errorf("expected nil fan_out_check[2], got %v", cfg.Assets[2].FanOutCheck)
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

func TestOutputDatasets(t *testing.T) {
	cfg := &PipelineConfig{
		Pipeline: "test",
		Connections: map[string]*ConnectionConfig{
			"bq": {Name: "bq", Type: "bigquery", Properties: map[string]string{"project": "p", "dataset": "dev_default"}},
			"bq2": {Name: "bq2", Type: "bigquery", Properties: map[string]string{"project": "p", "dataset": "dev_other"}},
			"bq_nods": {Name: "bq_nods", Type: "bigquery", Properties: map[string]string{"project": "p"}},
		},
		Datasets: map[string]string{
			"intermediate": "dev_intermediate",
		},
		Assets: []AssetConfig{
			{Name: "a1", Type: "sql", Source: "a.sql", DestinationConnection: "bq"},
			{Name: "a2", Type: "sql", Source: "b.sql", DestinationConnection: "bq_nods", Layer: "intermediate"},
			{Name: "a3", Type: "sql", Source: "c.sql", DestinationConnection: "bq2"},
			{Name: "a4", Type: "python", Source: "d.py"},
		},
	}

	datasets := cfg.OutputDatasets()
	got := make(map[string]bool)
	for _, ds := range datasets {
		got[ds] = true
	}

	expected := map[string]bool{
		"dev_default":      true,
		"dev_intermediate": true,
		"dev_other":        true,
	}

	if len(got) != len(expected) {
		t.Errorf("expected %d datasets, got %d: %v", len(expected), len(got), datasets)
	}
	for ds := range expected {
		if !got[ds] {
			t.Errorf("missing expected dataset %q in %v", ds, datasets)
		}
	}
}

func TestParseConfig_AssetTimeout(t *testing.T) {
	cfg, err := LoadConfig(writeTestConfig(t, `
pipeline: test
connections:
  bq:
    type: bigquery
    project: p
    dataset: d
assets:
  - name: slow_model
    type: sql
    source: sql/slow.sql
    destination_connection: bq
    timeout: 30m
  - name: fast_model
    type: shell
    source: scripts/fast.sh
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Assets[0].Timeout != "30m" {
		t.Errorf("expected timeout 30m, got %q", cfg.Assets[0].Timeout)
	}
	if cfg.Assets[1].Timeout != "" {
		t.Errorf("expected empty timeout, got %q", cfg.Assets[1].Timeout)
	}
}

func TestParseConfig_AssetTimeoutInvalid(t *testing.T) {
	_, err := LoadConfig(writeTestConfig(t, `
pipeline: test
assets:
  - name: bad
    type: shell
    source: x.sh
    timeout: not-a-duration
`))
	if err == nil {
		t.Error("expected error for invalid timeout")
	}
}
