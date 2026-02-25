package checker

import (
	"testing"

	"github.com/analytehealth/granicus/internal/config"
)

func TestGenerateCheckNodes_Basic(t *testing.T) {
	cfg := &config.PipelineConfig{
		Pipeline: "test",
		Assets: []config.AssetConfig{
			{
				Name:                  "transactions",
				Type:                  "sql",
				Source:                "sql/transactions.sql",
				DestinationConnection: "bq",
				Checks: []config.CheckConfig{
					{Source: "tests/check_transactions_not_null.sql"},
					{Source: "tests/check_transactions_fresh.py"},
				},
			},
			{
				Name:   "extract",
				Type:   "shell",
				Source: "scripts/extract.sh",
			},
		},
	}

	nodes, deps := GenerateCheckNodes(cfg)

	if len(nodes) != 2 {
		t.Fatalf("expected 2 check nodes, got %d", len(nodes))
	}

	// Check node names
	if nodes[0].Name != "check:transactions:check_transactions_not_null" {
		t.Errorf("node 0 name: %q", nodes[0].Name)
	}
	if nodes[1].Name != "check:transactions:check_transactions_fresh" {
		t.Errorf("node 1 name: %q", nodes[1].Name)
	}

	// Check types inferred from extension
	if nodes[0].Type != "sql_check" {
		t.Errorf("node 0 type: %q", nodes[0].Type)
	}
	if nodes[1].Type != "python_check" {
		t.Errorf("node 1 type: %q", nodes[1].Type)
	}

	// Check dependencies
	if d, ok := deps["check:transactions:check_transactions_not_null"]; !ok || len(d) != 1 || d[0] != "transactions" {
		t.Errorf("node 0 deps: %v", d)
	}

	// Check connection inheritance
	if nodes[0].DestinationConnection != "bq" {
		t.Errorf("node 0 dest conn: %q", nodes[0].DestinationConnection)
	}
}

func TestGenerateCheckNodes_NoChecks(t *testing.T) {
	cfg := &config.PipelineConfig{
		Pipeline: "test",
		Assets: []config.AssetConfig{
			{Name: "x", Type: "shell", Source: "x.sh"},
		},
	}

	nodes, _ := GenerateCheckNodes(cfg)
	if len(nodes) != 0 {
		t.Errorf("expected 0, got %d", len(nodes))
	}
}

func TestGenerateCheckNodes_ExplicitName(t *testing.T) {
	cfg := &config.PipelineConfig{
		Pipeline: "test",
		Assets: []config.AssetConfig{
			{
				Name: "tbl",
				Type: "shell",
				Source: "x.sh",
				Checks: []config.CheckConfig{
					{Name: "custom_check", Source: "checks/custom.sh"},
				},
			},
		},
	}

	nodes, _ := GenerateCheckNodes(cfg)
	if len(nodes) != 1 || nodes[0].Name != "check:tbl:custom_check" {
		t.Errorf("expected check:tbl:custom_check, got %q", nodes[0].Name)
	}
}
