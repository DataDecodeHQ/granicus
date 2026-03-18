package checker

import (
	"strings"
	"testing"

	"github.com/DataDecodeHQ/granicus/internal/config"
)

func TestGenerateContractCheckNodes_NoContract(t *testing.T) {
	cfg := &config.PipelineConfig{
		Pipeline: "test",
		Assets: []config.AssetConfig{
			{Name: "orders", Type: "shell", Source: "orders.sh"},
		},
	}

	nodes, deps := GenerateContractCheckNodes(cfg)

	if len(nodes) != 0 {
		t.Errorf("expected 0 nodes for asset with no contract, got %d", len(nodes))
	}
	if len(deps) != 0 {
		t.Errorf("expected 0 deps, got %d", len(deps))
	}
}

func TestGenerateContractCheckNodes_PrimaryKey(t *testing.T) {
	cfg := &config.PipelineConfig{
		Pipeline: "test",
		Assets: []config.AssetConfig{
			{
				Name:                  "stg_orders",
				Type:                  "sql",
				Source:                "orders.sql",
				DestinationConnection: "bq",
				Contract: &config.ContractConfig{
					PrimaryKey: "order_id",
				},
			},
		},
	}

	nodes, deps := GenerateContractCheckNodes(cfg)

	if len(nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(nodes))
	}

	n := nodes[0]
	expectedName := "check:stg_orders:default:contract_pk_order_id"
	if n.Name != expectedName {
		t.Errorf("expected name %q, got %q", expectedName, n.Name)
	}
	if n.Type != "sql_check" {
		t.Errorf("expected type sql_check, got %q", n.Type)
	}
	if n.Severity != "error" {
		t.Errorf("expected severity error, got %q", n.Severity)
	}
	if !n.Blocking {
		t.Error("expected Blocking=true")
	}
	if n.DestinationConnection != "bq" {
		t.Errorf("expected DestinationConnection bq, got %q", n.DestinationConnection)
	}
	if n.SourceAsset != "stg_orders" {
		t.Errorf("expected SourceAsset stg_orders, got %q", n.SourceAsset)
	}

	// SQL should check uniqueness
	if !strings.Contains(n.InlineSQL, "order_id") {
		t.Errorf("SQL missing pk column: %s", n.InlineSQL)
	}
	if !strings.Contains(n.InlineSQL, "HAVING COUNT(*) > 1") {
		t.Errorf("SQL missing HAVING COUNT(*) > 1: %s", n.InlineSQL)
	}
	if !strings.Contains(n.InlineSQL, "PK_NOT_UNIQUE") {
		t.Errorf("SQL missing PK_NOT_UNIQUE issue type: %s", n.InlineSQL)
	}
	if !strings.Contains(n.InlineSQL, `ref "stg_orders"`) {
		t.Errorf("SQL missing ref to asset: %s", n.InlineSQL)
	}

	d := deps[n.Name]
	if len(d) != 1 || d[0] != "stg_orders" {
		t.Errorf("expected deps [stg_orders], got %v", d)
	}
}

func TestGenerateContractCheckNodes_NotNull(t *testing.T) {
	cfg := &config.PipelineConfig{
		Pipeline: "test",
		Assets: []config.AssetConfig{
			{
				Name:                  "stg_orders",
				Type:                  "sql",
				Source:                "orders.sql",
				DestinationConnection: "bq",
				Contract: &config.ContractConfig{
					NotNull: []string{"order_id", "created_at"},
				},
			},
		},
	}

	nodes, deps := GenerateContractCheckNodes(cfg)

	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(nodes))
	}

	names := map[string]bool{}
	for _, n := range nodes {
		names[n.Name] = true
	}

	if !names["check:stg_orders:default:contract_not_null_order_id"] {
		t.Error("missing not_null check for order_id")
	}
	if !names["check:stg_orders:default:contract_not_null_created_at"] {
		t.Error("missing not_null check for created_at")
	}

	for _, n := range nodes {
		if n.Severity != "error" {
			t.Errorf("expected severity error, got %q for %s", n.Severity, n.Name)
		}
		if !strings.Contains(n.InlineSQL, "IS NULL") {
			t.Errorf("not_null SQL missing IS NULL: %s", n.InlineSQL)
		}
		if !strings.Contains(n.InlineSQL, "NULL_VALUE") {
			t.Errorf("not_null SQL missing NULL_VALUE issue type: %s", n.InlineSQL)
		}
		d := deps[n.Name]
		if len(d) != 1 || d[0] != "stg_orders" {
			t.Errorf("check %s deps: %v", n.Name, d)
		}
	}
}

func TestGenerateContractCheckNodes_AcceptedValues(t *testing.T) {
	cfg := &config.PipelineConfig{
		Pipeline: "test",
		Assets: []config.AssetConfig{
			{
				Name:                  "stg_orders",
				Type:                  "sql",
				Source:                "orders.sql",
				DestinationConnection: "bq",
				Contract: &config.ContractConfig{
					AcceptedValues: map[string][]string{
						"status": {"pending", "complete", "refunded"},
					},
				},
			},
		},
	}

	nodes, deps := GenerateContractCheckNodes(cfg)

	if len(nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(nodes))
	}

	n := nodes[0]
	expectedName := "check:stg_orders:default:contract_accepted_values_status"
	if n.Name != expectedName {
		t.Errorf("expected name %q, got %q", expectedName, n.Name)
	}
	if n.Severity != "error" {
		t.Errorf("expected severity error, got %q", n.Severity)
	}
	if !strings.Contains(n.InlineSQL, "NOT IN") {
		t.Errorf("accepted_values SQL missing NOT IN: %s", n.InlineSQL)
	}
	if !strings.Contains(n.InlineSQL, "'pending'") {
		t.Errorf("accepted_values SQL missing 'pending': %s", n.InlineSQL)
	}
	if !strings.Contains(n.InlineSQL, "'complete'") {
		t.Errorf("accepted_values SQL missing 'complete': %s", n.InlineSQL)
	}
	if !strings.Contains(n.InlineSQL, "'refunded'") {
		t.Errorf("accepted_values SQL missing 'refunded': %s", n.InlineSQL)
	}
	if !strings.Contains(n.InlineSQL, "UNACCEPTED_VALUE") {
		t.Errorf("accepted_values SQL missing UNACCEPTED_VALUE issue type: %s", n.InlineSQL)
	}
	if !strings.Contains(n.InlineSQL, `ref "stg_orders"`) {
		t.Errorf("accepted_values SQL missing ref to asset: %s", n.InlineSQL)
	}

	d := deps[n.Name]
	if len(d) != 1 || d[0] != "stg_orders" {
		t.Errorf("expected deps [stg_orders], got %v", d)
	}
}

func TestGenerateContractCheckNodes_AllContracts(t *testing.T) {
	cfg := &config.PipelineConfig{
		Pipeline: "test",
		Assets: []config.AssetConfig{
			{
				Name:                  "stg_orders",
				Type:                  "sql",
				Source:                "orders.sql",
				DestinationConnection: "bq",
				Contract: &config.ContractConfig{
					PrimaryKey: "order_id",
					NotNull:    []string{"created_at", "status"},
					AcceptedValues: map[string][]string{
						"status": {"active", "closed"},
					},
				},
			},
		},
	}

	nodes, _ := GenerateContractCheckNodes(cfg)

	// 1 pk + 2 not_null + 1 accepted_values = 4
	if len(nodes) != 4 {
		t.Fatalf("expected 4 nodes, got %d", len(nodes))
	}

	names := map[string]bool{}
	for _, n := range nodes {
		names[n.Name] = true
	}

	expected := []string{
		"check:stg_orders:default:contract_pk_order_id",
		"check:stg_orders:default:contract_not_null_created_at",
		"check:stg_orders:default:contract_not_null_status",
		"check:stg_orders:default:contract_accepted_values_status",
	}
	for _, e := range expected {
		if !names[e] {
			t.Errorf("missing check: %s", e)
		}
	}
}

func TestGenerateContractCheckNodes_MultipleAssets(t *testing.T) {
	cfg := &config.PipelineConfig{
		Pipeline: "test",
		Assets: []config.AssetConfig{
			{
				Name:   "no_contract",
				Type:   "shell",
				Source: "x.sh",
			},
			{
				Name:                  "with_contract",
				Type:                  "sql",
				Source:                "y.sql",
				DestinationConnection: "bq",
				Contract: &config.ContractConfig{
					PrimaryKey: "id",
				},
			},
		},
	}

	nodes, deps := GenerateContractCheckNodes(cfg)

	if len(nodes) != 1 {
		t.Fatalf("expected 1 node (only from asset with contract), got %d", len(nodes))
	}
	if nodes[0].SourceAsset != "with_contract" {
		t.Errorf("expected SourceAsset with_contract, got %q", nodes[0].SourceAsset)
	}
	if len(deps) != 1 {
		t.Errorf("expected 1 dep entry, got %d", len(deps))
	}
}

func TestGenerateContractCheckNodes_AcceptedValues_QuoteEscaping(t *testing.T) {
	cfg := &config.PipelineConfig{
		Pipeline: "test",
		Assets: []config.AssetConfig{
			{
				Name:                  "stg_orders",
				Type:                  "sql",
				Source:                "orders.sql",
				DestinationConnection: "bq",
				Contract: &config.ContractConfig{
					AcceptedValues: map[string][]string{
						"note": {"it's done", "complete"},
					},
				},
			},
		},
	}

	nodes, _ := GenerateContractCheckNodes(cfg)

	if len(nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(nodes))
	}

	// Single quote in value should be escaped as ''
	if !strings.Contains(nodes[0].InlineSQL, "it''s done") {
		t.Errorf("SQL should escape single quotes, got: %s", nodes[0].InlineSQL)
	}
}

func TestGenerateContractCheckNodes_PrimaryKey_SQLContainsGroupBy(t *testing.T) {
	cfg := &config.PipelineConfig{
		Pipeline: "test",
		Assets: []config.AssetConfig{
			{
				Name:                  "payments",
				Type:                  "sql",
				Source:                "payments.sql",
				DestinationConnection: "bq",
				Contract: &config.ContractConfig{
					PrimaryKey: "payment_id",
				},
			},
		},
	}

	nodes, _ := GenerateContractCheckNodes(cfg)

	sql := nodes[0].InlineSQL
	if !strings.Contains(sql, "GROUP BY payment_id") {
		t.Errorf("PK check SQL missing GROUP BY: %s", sql)
	}
	if !strings.Contains(sql, "SELECT payment_id") {
		t.Errorf("PK check SQL should SELECT the pk column: %s", sql)
	}
}
