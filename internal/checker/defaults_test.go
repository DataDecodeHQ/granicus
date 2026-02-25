package checker

import (
	"strings"
	"testing"

	"github.com/analytehealth/granicus/internal/config"
)

func boolPtr(b bool) *bool { return &b }

func TestGenerateDefaultCheckNodes_Staging(t *testing.T) {
	cfg := &config.PipelineConfig{
		Pipeline: "test",
		Assets: []config.AssetConfig{
			{Name: "stg_orders", Type: "sql", Layer: "staging", Grain: "order_id", DestinationConnection: "bq"},
		},
	}

	nodes, deps := GenerateDefaultCheckNodes(cfg)
	if len(nodes) != 2 {
		t.Fatalf("expected 2 checks for staging, got %d", len(nodes))
	}

	names := map[string]bool{}
	for _, n := range nodes {
		names[n.Name] = true
		if n.Type != "sql_check" {
			t.Errorf("expected type sql_check, got %q", n.Type)
		}
		if n.InlineSQL == "" {
			t.Errorf("expected InlineSQL for %s", n.Name)
		}
	}

	if !names["check:stg_orders:default:unique_grain"] {
		t.Error("missing unique_grain check")
	}
	if !names["check:stg_orders:default:not_null_grain"] {
		t.Error("missing not_null_grain check")
	}

	for _, n := range nodes {
		d := deps[n.Name]
		if len(d) != 1 || d[0] != "stg_orders" {
			t.Errorf("check %s deps: %v", n.Name, d)
		}
	}
}

func TestGenerateDefaultCheckNodes_Entity(t *testing.T) {
	cfg := &config.PipelineConfig{
		Pipeline: "test",
		Assets: []config.AssetConfig{
			{Name: "ent_user", Type: "sql", Layer: "entity", Grain: "user_id", DestinationConnection: "bq"},
		},
	}

	nodes, _ := GenerateDefaultCheckNodes(cfg)
	if len(nodes) != 3 {
		t.Fatalf("expected 3 checks for entity, got %d", len(nodes))
	}

	names := map[string]bool{}
	for _, n := range nodes {
		names[n.Name] = true
	}
	if !names["check:ent_user:default:row_count"] {
		t.Error("missing row_count check for entity")
	}
}

func TestGenerateDefaultCheckNodes_Report(t *testing.T) {
	cfg := &config.PipelineConfig{
		Pipeline: "test",
		Assets: []config.AssetConfig{
			{Name: "rpt_summary", Type: "sql", Layer: "report", Grain: "period", DestinationConnection: "bq"},
		},
	}

	nodes, _ := GenerateDefaultCheckNodes(cfg)
	if len(nodes) != 2 {
		t.Fatalf("expected 2 checks for report, got %d", len(nodes))
	}
}

func TestGenerateDefaultCheckNodes_NoLayerOrGrain(t *testing.T) {
	cfg := &config.PipelineConfig{
		Pipeline: "test",
		Assets: []config.AssetConfig{
			{Name: "raw_data", Type: "shell"},
			{Name: "stg_no_grain", Type: "sql", Layer: "staging"},
		},
	}

	nodes, _ := GenerateDefaultCheckNodes(cfg)
	if len(nodes) != 0 {
		t.Errorf("expected 0 checks, got %d", len(nodes))
	}
}

func TestGenerateDefaultCheckNodes_DisabledByFlag(t *testing.T) {
	cfg := &config.PipelineConfig{
		Pipeline: "test",
		Assets: []config.AssetConfig{
			{Name: "stg_orders", Type: "sql", Layer: "staging", Grain: "order_id", DefaultChecks: boolPtr(false)},
		},
	}

	nodes, _ := GenerateDefaultCheckNodes(cfg)
	if len(nodes) != 0 {
		t.Errorf("expected 0 checks when disabled, got %d", len(nodes))
	}
}

func TestGenerateDefaultCheckNodes_SQLContent(t *testing.T) {
	cfg := &config.PipelineConfig{
		Pipeline: "test",
		Assets: []config.AssetConfig{
			{Name: "stg_orders", Type: "sql", Layer: "staging", Grain: "order_id", DestinationConnection: "bq"},
		},
	}

	nodes, _ := GenerateDefaultCheckNodes(cfg)
	for _, n := range nodes {
		if strings.Contains(n.Name, "unique_grain") {
			if !strings.Contains(n.InlineSQL, "GROUP BY") || !strings.Contains(n.InlineSQL, "HAVING") {
				t.Errorf("unique_grain SQL missing expected clauses: %s", n.InlineSQL)
			}
			if !strings.Contains(n.InlineSQL, "order_id") {
				t.Errorf("unique_grain SQL missing grain column: %s", n.InlineSQL)
			}
		}
		if strings.Contains(n.Name, "not_null_grain") {
			if !strings.Contains(n.InlineSQL, "IS NULL") {
				t.Errorf("not_null_grain SQL missing IS NULL: %s", n.InlineSQL)
			}
		}
	}
}
