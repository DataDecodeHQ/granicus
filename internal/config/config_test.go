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
	if len(cfg.Connections) > 0 {
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
  - name: z
    type: shell
    source: z.sh
    layer: publish
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

func TestParseConfig_RetryDefaults(t *testing.T) {
	cfg, err := LoadConfig(writeTestConfig(t, `
pipeline: test
assets:
  - name: x
    type: shell
    source: x.sh
`))
	if err != nil {
		t.Fatal(err)
	}
	r := cfg.Assets[0].Retry
	if r == nil {
		t.Fatal("expected retry config to be set by defaults")
	}
	if r.MaxAttempts != 3 {
		t.Errorf("expected default max_attempts=3, got %d", r.MaxAttempts)
	}
	if r.BackoffBase != "10s" {
		t.Errorf("expected default backoff_base=10s, got %q", r.BackoffBase)
	}
	if len(r.RetryableErrors) != 3 || r.RetryableErrors[0] != "rate_limit" || r.RetryableErrors[1] != "quota" || r.RetryableErrors[2] != "network" {
		t.Errorf("expected default retryable_errors=[rate_limit quota network], got %v", r.RetryableErrors)
	}
}

func TestParseConfig_RetryExplicit(t *testing.T) {
	cfg, err := LoadConfig(writeTestConfig(t, `
pipeline: test
assets:
  - name: x
    type: shell
    source: x.sh
    retry:
      max_attempts: 5
      backoff_base: 30s
      retryable_errors:
        - rate_limit
        - timeout
        - server
`))
	if err != nil {
		t.Fatal(err)
	}
	r := cfg.Assets[0].Retry
	if r == nil {
		t.Fatal("expected retry config")
	}
	if r.MaxAttempts != 5 {
		t.Errorf("max_attempts: %d", r.MaxAttempts)
	}
	if r.BackoffBase != "30s" {
		t.Errorf("backoff_base: %q", r.BackoffBase)
	}
	if len(r.RetryableErrors) != 3 || r.RetryableErrors[0] != "rate_limit" || r.RetryableErrors[1] != "timeout" || r.RetryableErrors[2] != "server" {
		t.Errorf("retryable_errors: %v", r.RetryableErrors)
	}
}

func TestParseConfig_RetryPartialDefaults(t *testing.T) {
	// Only backoff_base specified — other fields get defaults
	cfg, err := LoadConfig(writeTestConfig(t, `
pipeline: test
assets:
  - name: x
    type: shell
    source: x.sh
    retry:
      backoff_base: 1m
`))
	if err != nil {
		t.Fatal(err)
	}
	r := cfg.Assets[0].Retry
	if r.MaxAttempts != 3 {
		t.Errorf("expected default max_attempts=3, got %d", r.MaxAttempts)
	}
	if r.BackoffBase != "1m" {
		t.Errorf("backoff_base: %q", r.BackoffBase)
	}
	if len(r.RetryableErrors) != 3 {
		t.Errorf("expected 3 default retryable_errors, got %v", r.RetryableErrors)
	}
}

func TestParseConfig_RetryBadBackoffBase(t *testing.T) {
	_, err := LoadConfig(writeTestConfig(t, `
pipeline: test
assets:
  - name: x
    type: shell
    source: x.sh
    retry:
      backoff_base: not-a-duration
`))
	if err == nil {
		t.Error("expected error for invalid backoff_base")
	}
}

func TestParseConfig_RetryMaxAttemptsZero(t *testing.T) {
	// max_attempts: 0 means unset, should use default of 3
	cfg, err := LoadConfig(writeTestConfig(t, `
pipeline: test
assets:
  - name: x
    type: shell
    source: x.sh
    retry:
      max_attempts: 0
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Assets[0].Retry.MaxAttempts != 3 {
		t.Errorf("expected default max_attempts=3 for 0, got %d", cfg.Assets[0].Retry.MaxAttempts)
	}
}

func TestParseConfig_RetryUnknownCategory(t *testing.T) {
	_, err := LoadConfig(writeTestConfig(t, `
pipeline: test
assets:
  - name: x
    type: shell
    source: x.sh
    retry:
      retryable_errors:
        - rate_limit
        - bogus_category
`))
	if err == nil {
		t.Error("expected error for unknown retryable_errors category")
	}
}

func TestParseConfig_RetryAllValidCategories(t *testing.T) {
	cfg, err := LoadConfig(writeTestConfig(t, `
pipeline: test
assets:
  - name: x
    type: shell
    source: x.sh
    retry:
      max_attempts: 1
      retryable_errors:
        - rate_limit
        - quota
        - network
        - timeout
        - server
`))
	if err != nil {
		t.Fatalf("all valid categories should pass: %v", err)
	}
	if len(cfg.Assets[0].Retry.RetryableErrors) != 5 {
		t.Errorf("expected 5 categories, got %v", cfg.Assets[0].Retry.RetryableErrors)
	}
}

func TestParseConfig_CheckSeverityDefault(t *testing.T) {
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
    checks:
      - name: check_no_nulls
        type: sql
        source: checks/no_nulls.sql
`))
	if err != nil {
		t.Fatal(err)
	}
	check := cfg.Assets[0].Checks[0]
	if check.Severity != "error" {
		t.Errorf("expected default severity 'error', got %q", check.Severity)
	}
}

func TestParseConfig_CheckSeverityExplicit(t *testing.T) {
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
    checks:
      - name: check_warn
        type: sql
        source: checks/warn.sql
        severity: warning
      - name: check_crit
        type: sql
        source: checks/crit.sql
        severity: critical
      - name: check_info
        type: sql
        source: checks/info.sql
        severity: info
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Assets[0].Checks[0].Severity != "warning" {
		t.Errorf("check[0] severity: %q", cfg.Assets[0].Checks[0].Severity)
	}
	if cfg.Assets[0].Checks[1].Severity != "critical" {
		t.Errorf("check[1] severity: %q", cfg.Assets[0].Checks[1].Severity)
	}
	if cfg.Assets[0].Checks[2].Severity != "info" {
		t.Errorf("check[2] severity: %q", cfg.Assets[0].Checks[2].Severity)
	}
}

func TestParseConfig_CheckSeverityInvalid(t *testing.T) {
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
    checks:
      - name: check_bad
        type: sql
        source: checks/bad.sql
        severity: invalid
`))
	if err == nil {
		t.Error("expected error for invalid severity")
	}
}

func TestParseConfig_ContractValid(t *testing.T) {
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
    contract:
      primary_key: order_id
      not_null:
        - order_id
        - created_at
      accepted_values:
        status:
          - pending
          - complete
          - refunded
        payment_type:
          - card
          - cash
`))
	if err != nil {
		t.Fatal(err)
	}
	c := cfg.Assets[0].Contract
	if c == nil {
		t.Fatal("expected contract config")
	}
	if c.PrimaryKey != "order_id" {
		t.Errorf("primary_key: %q", c.PrimaryKey)
	}
	if len(c.NotNull) != 2 || c.NotNull[0] != "order_id" || c.NotNull[1] != "created_at" {
		t.Errorf("not_null: %v", c.NotNull)
	}
	if len(c.AcceptedValues) != 2 {
		t.Errorf("accepted_values len: %d", len(c.AcceptedValues))
	}
	if vals := c.AcceptedValues["status"]; len(vals) != 3 || vals[0] != "pending" {
		t.Errorf("accepted_values[status]: %v", vals)
	}
	if vals := c.AcceptedValues["payment_type"]; len(vals) != 2 || vals[0] != "card" {
		t.Errorf("accepted_values[payment_type]: %v", vals)
	}
}

func TestParseConfig_ContractMinimal(t *testing.T) {
	// Contract with only primary_key, no not_null or accepted_values
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
    contract:
      primary_key: order_id
`))
	if err != nil {
		t.Fatal(err)
	}
	c := cfg.Assets[0].Contract
	if c == nil {
		t.Fatal("expected contract config")
	}
	if c.PrimaryKey != "order_id" {
		t.Errorf("primary_key: %q", c.PrimaryKey)
	}
	if len(c.NotNull) != 0 {
		t.Errorf("expected empty not_null, got %v", c.NotNull)
	}
	if len(c.AcceptedValues) != 0 {
		t.Errorf("expected empty accepted_values, got %v", c.AcceptedValues)
	}
}

func TestParseConfig_ContractNilWhenAbsent(t *testing.T) {
	cfg, err := LoadConfig(writeTestConfig(t, `
pipeline: test
assets:
  - name: x
    type: shell
    source: x.sh
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Assets[0].Contract != nil {
		t.Errorf("expected nil contract, got %+v", cfg.Assets[0].Contract)
	}
}

func TestParseConfig_ContractAcceptedValuesEmpty(t *testing.T) {
	// accepted_values entry with empty slice should fail
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
    contract:
      accepted_values:
        status: []
`))
	if err == nil {
		t.Error("expected error for accepted_values entry with empty slice")
	}
}

func TestParseConfig_ContractNotNullEmptyString(t *testing.T) {
	// not_null entry with empty string should fail
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
    contract:
      not_null:
        - ""
`))
	if err == nil {
		t.Error("expected error for not_null entry with empty column name")
	}
}

func TestParseConfig_CheckBackwardsCompatible(t *testing.T) {
	// Old config without severity field should still work
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
    checks:
      - name: old_check
        type: sql
        source: checks/old.sql
        blocking: true
`))
	if err != nil {
		t.Fatal(err)
	}
	check := cfg.Assets[0].Checks[0]
	if check.Blocking != true {
		t.Errorf("expected blocking=true, got %v", check.Blocking)
	}
	if check.Severity != "error" {
		t.Errorf("expected default severity 'error', got %q", check.Severity)
	}
}

func TestParseConfig_AlertsAbsent(t *testing.T) {
	cfg, err := LoadConfig(writeTestConfig(t, `
pipeline: test
assets:
  - name: x
    type: shell
    source: x.sh
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Alerts != nil {
		t.Errorf("expected nil alerts, got %+v", cfg.Alerts)
	}
}

func TestParseConfig_AlertsFullRouting(t *testing.T) {
	cfg, err := LoadConfig(writeTestConfig(t, `
pipeline: test
assets:
  - name: x
    type: shell
    source: x.sh
alerts:
  critical:
    url: https://hooks.example.com/critical
    template: '{"text":"CRITICAL: {{.Pipeline}}"}'
  warning:
    url: https://hooks.example.com/warning
  default:
    url: https://hooks.example.com/default
    template: '{"text":"{{.Pipeline}} failed"}'
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Alerts == nil {
		t.Fatal("expected alerts config")
	}
	if cfg.Alerts.Critical == nil || cfg.Alerts.Critical.URL != "https://hooks.example.com/critical" {
		t.Errorf("critical: %+v", cfg.Alerts.Critical)
	}
	if cfg.Alerts.Critical.Template != `{"text":"CRITICAL: {{.Pipeline}}"}` {
		t.Errorf("critical template: %q", cfg.Alerts.Critical.Template)
	}
	if cfg.Alerts.Warning == nil || cfg.Alerts.Warning.URL != "https://hooks.example.com/warning" {
		t.Errorf("warning: %+v", cfg.Alerts.Warning)
	}
	if cfg.Alerts.Warning.Template != "" {
		t.Errorf("warning template should be empty, got %q", cfg.Alerts.Warning.Template)
	}
	if cfg.Alerts.Default == nil || cfg.Alerts.Default.URL != "https://hooks.example.com/default" {
		t.Errorf("default: %+v", cfg.Alerts.Default)
	}
}

func TestParseConfig_AlertsDefaultOnly(t *testing.T) {
	cfg, err := LoadConfig(writeTestConfig(t, `
pipeline: test
assets:
  - name: x
    type: shell
    source: x.sh
alerts:
  default:
    url: https://hooks.example.com/default
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Alerts == nil || cfg.Alerts.Default == nil {
		t.Fatal("expected default alert config")
	}
	if cfg.Alerts.Critical != nil {
		t.Errorf("expected nil critical, got %+v", cfg.Alerts.Critical)
	}
	if cfg.Alerts.Warning != nil {
		t.Errorf("expected nil warning, got %+v", cfg.Alerts.Warning)
	}
}

func TestParseConfig_AlertsMissingURL(t *testing.T) {
	_, err := LoadConfig(writeTestConfig(t, `
pipeline: test
assets:
  - name: x
    type: shell
    source: x.sh
alerts:
  critical:
    template: '{"text":"no url here"}'
`))
	if err == nil {
		t.Error("expected error for critical block with missing url")
	}
}

func TestAlertRoutingResolve(t *testing.T) {
	critical := &AlertSeverityConfig{URL: "https://critical.example.com"}
	warning := &AlertSeverityConfig{URL: "https://warning.example.com"}
	dflt := &AlertSeverityConfig{URL: "https://default.example.com"}

	r := &AlertRoutingConfig{
		Critical: critical,
		Warning:  warning,
		Default:  dflt,
	}

	if got := r.Resolve("critical"); got != critical {
		t.Errorf("Resolve(critical) = %+v, want %+v", got, critical)
	}
	if got := r.Resolve("warning"); got != warning {
		t.Errorf("Resolve(warning) = %+v, want %+v", got, warning)
	}
	if got := r.Resolve("error"); got != dflt {
		t.Errorf("Resolve(error) should fall back to default, got %+v", got)
	}
	if got := r.Resolve("info"); got != dflt {
		t.Errorf("Resolve(info) should fall back to default, got %+v", got)
	}
	if got := r.Resolve("unknown"); got != dflt {
		t.Errorf("Resolve(unknown) should fall back to default, got %+v", got)
	}
}

func TestAlertRoutingResolveNoSeveritySpecific(t *testing.T) {
	dflt := &AlertSeverityConfig{URL: "https://default.example.com"}
	r := &AlertRoutingConfig{Default: dflt}

	if got := r.Resolve("critical"); got != dflt {
		t.Errorf("Resolve(critical) without critical config should fall back to default")
	}
	if got := r.Resolve("warning"); got != dflt {
		t.Errorf("Resolve(warning) without warning config should fall back to default")
	}
}

func TestAlertRoutingResolveNilDefault(t *testing.T) {
	r := &AlertRoutingConfig{}
	if got := r.Resolve("critical"); got != nil {
		t.Errorf("Resolve(critical) with no config should return nil, got %+v", got)
	}
}
