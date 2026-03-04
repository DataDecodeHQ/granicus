package checker

import (
	"strings"
	"testing"

	"github.com/Andrew-DataDecode/Granicus/internal/config"
)

func TestGenerateFKCheckNodes_SingleFK(t *testing.T) {
	cfg := &config.PipelineConfig{
		Pipeline: "test",
		Assets: []config.AssetConfig{
			{
				Name:                  "stg_orders",
				Type:                  "sql",
				Layer:                 "staging",
				Grain:                 "order_id",
				DestinationConnection: "bq",
				ForeignKeys: []config.ForeignKeyConfig{
					{Column: "account_id", References: "stg_accounts.account_id"},
				},
			},
		},
	}

	nodes, deps := GenerateFKCheckNodes(cfg)

	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes (not_null + integrity), got %d", len(nodes))
	}

	names := map[string]bool{}
	for _, n := range nodes {
		names[n.Name] = true
	}

	notNullName := "check:stg_orders:default:fk_not_null_account_id"
	integrityName := "check:stg_orders:default:fk_integrity_account_id"

	if !names[notNullName] {
		t.Errorf("missing %s", notNullName)
	}
	if !names[integrityName] {
		t.Errorf("missing %s", integrityName)
	}

	for _, n := range nodes {
		if n.Type != "sql_check" {
			t.Errorf("expected type sql_check, got %q for %s", n.Type, n.Name)
		}
		if n.InlineSQL == "" {
			t.Errorf("expected InlineSQL for %s", n.Name)
		}
		if n.DestinationConnection != "bq" {
			t.Errorf("expected DestinationConnection bq, got %q for %s", n.DestinationConnection, n.Name)
		}
		if n.SourceAsset != "stg_orders" {
			t.Errorf("expected SourceAsset stg_orders, got %q for %s", n.SourceAsset, n.Name)
		}
		d := deps[n.Name]
		if len(d) != 1 || d[0] != "stg_orders" {
			t.Errorf("check %s deps: %v", n.Name, d)
		}
	}

	// Verify SQL content for not_null check
	for _, n := range nodes {
		if strings.Contains(n.Name, "fk_not_null") {
			if !strings.Contains(n.InlineSQL, "FK_IS_NULL") {
				t.Errorf("not_null SQL missing FK_IS_NULL: %s", n.InlineSQL)
			}
			if !strings.Contains(n.InlineSQL, "account_id IS NULL") {
				t.Errorf("not_null SQL missing IS NULL check: %s", n.InlineSQL)
			}
			if !strings.Contains(n.InlineSQL, "order_id") {
				t.Errorf("not_null SQL missing grain: %s", n.InlineSQL)
			}
		}
		if strings.Contains(n.Name, "fk_integrity") {
			if !strings.Contains(n.InlineSQL, "ORPHAN_FK") {
				t.Errorf("integrity SQL missing ORPHAN_FK: %s", n.InlineSQL)
			}
			if !strings.Contains(n.InlineSQL, "LEFT JOIN") {
				t.Errorf("integrity SQL missing LEFT JOIN: %s", n.InlineSQL)
			}
			if !strings.Contains(n.InlineSQL, "stg_accounts") {
				t.Errorf("integrity SQL missing referenced table: %s", n.InlineSQL)
			}
		}
	}
}

func TestGenerateFKCheckNodes_MultipleFKs(t *testing.T) {
	cfg := &config.PipelineConfig{
		Pipeline: "test",
		Assets: []config.AssetConfig{
			{
				Name:                  "stg_orders",
				Type:                  "sql",
				Layer:                 "staging",
				Grain:                 "order_id",
				DestinationConnection: "bq",
				ForeignKeys: []config.ForeignKeyConfig{
					{Column: "account_id", References: "stg_accounts.account_id"},
					{Column: "product_id", References: "stg_products.product_id"},
				},
			},
		},
	}

	nodes, deps := GenerateFKCheckNodes(cfg)

	// 2 FKs * 2 checks each = 4 nodes
	if len(nodes) != 4 {
		t.Fatalf("expected 4 nodes for 2 FKs, got %d", len(nodes))
	}

	names := map[string]bool{}
	for _, n := range nodes {
		names[n.Name] = true
	}

	expected := []string{
		"check:stg_orders:default:fk_not_null_account_id",
		"check:stg_orders:default:fk_integrity_account_id",
		"check:stg_orders:default:fk_not_null_product_id",
		"check:stg_orders:default:fk_integrity_product_id",
	}
	for _, e := range expected {
		if !names[e] {
			t.Errorf("missing check: %s", e)
		}
	}

	for _, n := range nodes {
		d := deps[n.Name]
		if len(d) != 1 || d[0] != "stg_orders" {
			t.Errorf("check %s deps: %v", n.Name, d)
		}
	}
}

func TestGenerateFKCheckNodes_NullableFK(t *testing.T) {
	cfg := &config.PipelineConfig{
		Pipeline: "test",
		Assets: []config.AssetConfig{
			{
				Name:                  "stg_orders",
				Type:                  "sql",
				Layer:                 "staging",
				Grain:                 "order_id",
				DestinationConnection: "bq",
				ForeignKeys: []config.ForeignKeyConfig{
					{Column: "account_id", References: "stg_accounts.account_id", Nullable: true},
				},
			},
		},
	}

	nodes, deps := GenerateFKCheckNodes(cfg)

	// nullable FK: only integrity check, no not_null check
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node for nullable FK (integrity only), got %d", len(nodes))
	}

	n := nodes[0]
	if !strings.Contains(n.Name, "fk_integrity_account_id") {
		t.Errorf("expected fk_integrity_account_id, got %s", n.Name)
	}
	if strings.Contains(n.Name, "fk_not_null") {
		t.Errorf("nullable FK should not generate fk_not_null check, got %s", n.Name)
	}

	d := deps[n.Name]
	if len(d) != 1 || d[0] != "stg_orders" {
		t.Errorf("check %s deps: %v", n.Name, d)
	}
}

func TestGenerateFKCheckNodes_NoFKs(t *testing.T) {
	cfg := &config.PipelineConfig{
		Pipeline: "test",
		Assets: []config.AssetConfig{
			{
				Name:                  "stg_orders",
				Type:                  "sql",
				Layer:                 "staging",
				Grain:                 "order_id",
				DestinationConnection: "bq",
			},
			{
				Name:  "raw_data",
				Type:  "shell",
				Layer: "source",
			},
		},
	}

	nodes, deps := GenerateFKCheckNodes(cfg)

	if len(nodes) != 0 {
		t.Errorf("expected 0 nodes for assets with no FKs, got %d", len(nodes))
	}
	if len(deps) != 0 {
		t.Errorf("expected 0 deps for assets with no FKs, got %d", len(deps))
	}
}
