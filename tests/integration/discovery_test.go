package integration

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/DataDecodeHQ/granicus/internal/checker"
	"github.com/DataDecodeHQ/granicus/internal/config"
)

func TestCheckFileDiscovery(t *testing.T) {
	tmpRoot := t.TempDir()

	pipelineDir := filepath.Join(tmpRoot, "project", "granicus_pipeline", "test_pipeline")
	checksDir := filepath.Join(pipelineDir, "checks")
	if err := os.MkdirAll(checksDir, 0o755); err != nil {
		t.Fatal(err)
	}

	checkFiles := map[string]string{
		"check_stg_orders_not_null.sql": "SELECT * FROM stg_orders WHERE id IS NULL",
		"check_stg_orders_unique.sql":   "SELECT id, COUNT(*) FROM stg_orders GROUP BY id HAVING COUNT(*) > 1",
	}
	for name, content := range checkFiles {
		if err := os.WriteFile(filepath.Join(checksDir, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	// Source SQL file
	if err := os.WriteFile(filepath.Join(pipelineDir, "stg_orders.sql"), []byte("SELECT * FROM raw_orders"), 0o644); err != nil {
		t.Fatal(err)
	}

	yaml := `pipeline: test_pipeline
connections:
  bq:
    type: bigquery
    project: test
    dataset: test
assets:
  - name: stg_orders
    type: sql
    source: stg_orders.sql
    destination_connection: bq
`
	if err := os.WriteFile(filepath.Join(pipelineDir, "pipeline.yaml"), []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg, err := config.LoadConfig(filepath.Join(pipelineDir, "pipeline.yaml"))
	if err != nil {
		t.Fatalf("loading config: %v", err)
	}

	assets := checker.DiscoverChecks(pipelineDir, cfg.Assets)

	var stgOrders *config.AssetConfig
	for i := range assets {
		if assets[i].Name == "stg_orders" {
			stgOrders = &assets[i]
			break
		}
	}
	if stgOrders == nil {
		t.Fatal("stg_orders asset not found after discovery")
	}

	if len(stgOrders.Checks) != 2 {
		t.Fatalf("expected 2 checks discovered for stg_orders, got %d", len(stgOrders.Checks))
	}

	checkNames := map[string]bool{}
	for _, c := range stgOrders.Checks {
		checkNames[c.Name] = true
	}

	expectedChecks := []string{"check_stg_orders_not_null", "check_stg_orders_unique"}
	for _, name := range expectedChecks {
		if !checkNames[name] {
			t.Errorf("check %q not discovered", name)
		}
	}
}

func TestCheckFileReferencesResolve(t *testing.T) {
	tmpRoot := t.TempDir()

	pipelineDir := filepath.Join(tmpRoot, "project", "granicus_pipeline", "test_pipeline")
	checksDir := filepath.Join(pipelineDir, "checks")
	if err := os.MkdirAll(checksDir, 0o755); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(checksDir, "check_stg_data_not_null.sql"), []byte("SELECT 1"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(pipelineDir, "stg_data.sql"), []byte("SELECT 1"), 0o644); err != nil {
		t.Fatal(err)
	}

	yaml := `pipeline: test_pipeline
connections:
  bq:
    type: bigquery
    project: test
    dataset: test
assets:
  - name: stg_data
    type: sql
    source: stg_data.sql
    destination_connection: bq
`
	if err := os.WriteFile(filepath.Join(pipelineDir, "pipeline.yaml"), []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg, err := config.LoadConfig(filepath.Join(pipelineDir, "pipeline.yaml"))
	if err != nil {
		t.Fatalf("loading config: %v", err)
	}

	assets := checker.DiscoverChecks(pipelineDir, cfg.Assets)

	for _, asset := range assets {
		for _, chk := range asset.Checks {
			resolved := filepath.Join(pipelineDir, chk.Source)
			if _, err := os.Stat(resolved); os.IsNotExist(err) {
				t.Errorf("check %q source %q does not resolve to existing file at %s", chk.Name, chk.Source, resolved)
			}
		}
	}
}

func TestContractFileDiscovery(t *testing.T) {
	tmpRoot := t.TempDir()

	pipelineDir := filepath.Join(tmpRoot, "project", "granicus_pipeline", "test_pipeline")
	contractsDir := filepath.Join(pipelineDir, "contracts")
	if err := os.MkdirAll(contractsDir, 0o755); err != nil {
		t.Fatal(err)
	}

	contractJSON := `{
  "stg_orders": {
    "primary_key": "order_id",
    "not_null": ["order_id", "status"],
    "accepted_values": {
      "status": ["pending", "complete", "cancelled"]
    }
  }
}`
	if err := os.WriteFile(filepath.Join(contractsDir, "contracts.json"), []byte(contractJSON), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(pipelineDir, "stg_orders.sql"), []byte("SELECT 1"), 0o644); err != nil {
		t.Fatal(err)
	}

	yaml := `pipeline: test_pipeline
connections:
  bq:
    type: bigquery
    project: test
    dataset: test
assets:
  - name: stg_orders
    type: sql
    source: stg_orders.sql
    destination_connection: bq
`
	if err := os.WriteFile(filepath.Join(pipelineDir, "pipeline.yaml"), []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}

	// Verify contract file exists at the expected location
	contractPath := filepath.Join(pipelineDir, "contracts", "contracts.json")
	if _, err := os.Stat(contractPath); os.IsNotExist(err) {
		t.Fatalf("contracts.json not found at %s", contractPath)
	}

	// Verify the pipeline config loads successfully
	_, err := config.LoadConfig(filepath.Join(pipelineDir, "pipeline.yaml"))
	if err != nil {
		t.Fatalf("loading config: %v", err)
	}
}

func TestInlineContractOnAsset(t *testing.T) {
	tmpRoot := t.TempDir()

	pipelineDir := filepath.Join(tmpRoot, "project", "granicus_pipeline", "test_pipeline")
	if err := os.MkdirAll(pipelineDir, 0o755); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(pipelineDir, "stg_orders.sql"), []byte("SELECT 1"), 0o644); err != nil {
		t.Fatal(err)
	}

	yaml := `pipeline: test_pipeline
connections:
  bq:
    type: bigquery
    project: test
    dataset: test
assets:
  - name: stg_orders
    type: sql
    source: stg_orders.sql
    destination_connection: bq
    contract:
      primary_key: order_id
      not_null:
        - order_id
        - status
      accepted_values:
        status:
          - pending
          - complete
`
	if err := os.WriteFile(filepath.Join(pipelineDir, "pipeline.yaml"), []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg, err := config.LoadConfig(filepath.Join(pipelineDir, "pipeline.yaml"))
	if err != nil {
		t.Fatalf("loading config: %v", err)
	}

	var stgOrders *config.AssetConfig
	for i := range cfg.Assets {
		if cfg.Assets[i].Name == "stg_orders" {
			stgOrders = &cfg.Assets[i]
			break
		}
	}
	if stgOrders == nil {
		t.Fatal("stg_orders not found")
	}
	if stgOrders.Contract == nil {
		t.Fatal("contract not loaded on stg_orders")
	}
	if stgOrders.Contract.PrimaryKey != "order_id" {
		t.Errorf("contract primary_key = %q, want order_id", stgOrders.Contract.PrimaryKey)
	}
	if len(stgOrders.Contract.NotNull) != 2 {
		t.Errorf("contract not_null has %d entries, want 2", len(stgOrders.Contract.NotNull))
	}
	if len(stgOrders.Contract.AcceptedValues["status"]) != 2 {
		t.Errorf("contract accepted_values[status] has %d entries, want 2", len(stgOrders.Contract.AcceptedValues["status"]))
	}
}

func TestSourceFileReferencesResolve(t *testing.T) {
	tmpRoot := t.TempDir()

	pipelineDir := filepath.Join(tmpRoot, "project", "granicus_pipeline", "test_pipeline")
	sqlDir := filepath.Join(pipelineDir, "sql")
	if err := os.MkdirAll(sqlDir, 0o755); err != nil {
		t.Fatal(err)
	}

	sources := []string{"stg_orders.sql", "stg_accounts.sql", "int_summary.sql"}
	for _, name := range sources {
		if err := os.WriteFile(filepath.Join(sqlDir, name), []byte("SELECT 1"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	yaml := `pipeline: test_pipeline
connections:
  bq:
    type: bigquery
    project: test
    dataset: test
assets:
  - name: stg_orders
    type: sql
    source: sql/stg_orders.sql
    destination_connection: bq
  - name: stg_accounts
    type: sql
    source: sql/stg_accounts.sql
    destination_connection: bq
  - name: int_summary
    type: sql
    source: sql/int_summary.sql
    destination_connection: bq
`
	if err := os.WriteFile(filepath.Join(pipelineDir, "pipeline.yaml"), []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg, err := config.LoadConfig(filepath.Join(pipelineDir, "pipeline.yaml"))
	if err != nil {
		t.Fatalf("loading config: %v", err)
	}

	for _, asset := range cfg.Assets {
		resolved := filepath.Join(pipelineDir, asset.Source)
		if _, err := os.Stat(resolved); os.IsNotExist(err) {
			t.Errorf("asset %q source %q does not resolve to existing file at %s", asset.Name, asset.Source, resolved)
		}
	}
}

func TestDiscoveryIgnoresCheckFiles(t *testing.T) {
	tmpRoot := t.TempDir()

	pipelineDir := filepath.Join(tmpRoot, "project", "granicus_pipeline", "test_pipeline")
	checksDir := filepath.Join(pipelineDir, "checks")
	if err := os.MkdirAll(checksDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// check files should NOT become assets
	if err := os.WriteFile(filepath.Join(checksDir, "check_stg_data_not_null.sql"), []byte("SELECT 1"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(checksDir, "check_stg_data_unique.sql"), []byte("SELECT 1"), 0o644); err != nil {
		t.Fatal(err)
	}

	discoveryPaths := []config.DiscoveryPath{
		{Path: "checks"},
	}

	discovered, err := config.DiscoverAssets(pipelineDir, discoveryPaths)
	if err != nil {
		t.Fatalf("discovering assets: %v", err)
	}

	if len(discovered) != 0 {
		names := make([]string, len(discovered))
		for i, a := range discovered {
			names[i] = a.Name
		}
		t.Errorf("check files should not be discovered as assets, got: %v", names)
	}
}
