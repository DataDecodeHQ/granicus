package checker

import (
	"strings"
	"testing"

	"github.com/DataDecodeHQ/granicus/internal/config"
	"github.com/DataDecodeHQ/granicus/internal/graph"
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
	if len(nodes) != 5 {
		t.Fatalf("expected 5 checks for staging, got %d", len(nodes))
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
	if !names["check:stg_orders:default:not_empty"] {
		t.Error("missing not_empty check")
	}
	if !names["check:stg_orders:default:no_future_timestamps"] {
		t.Error("missing no_future_timestamps check")
	}
	if !names["check:stg_orders:default:updated_at_gte_created_at"] {
		t.Error("missing updated_at_gte_created_at check")
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
	if len(nodes) != 1 {
		t.Fatalf("expected 1 check for entity, got %d", len(nodes))
	}
	if nodes[0].Name != "check:ent_user:default:unique_grain" {
		t.Errorf("expected unique_grain, got %s", nodes[0].Name)
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
	if len(nodes) != 1 {
		t.Fatalf("expected 1 check for report, got %d", len(nodes))
	}
	if nodes[0].Name != "check:rpt_summary:default:row_count" {
		t.Errorf("expected row_count, got %s", nodes[0].Name)
	}
}

func TestGenerateDefaultCheckNodes_Intermediate_NoUpstream(t *testing.T) {
	ratio := 0.5
	cfg := &config.PipelineConfig{
		Pipeline: "test",
		Assets: []config.AssetConfig{
			{Name: "int_orders", Type: "sql", Layer: "intermediate", Grain: "order_id", DestinationConnection: "bq", MinRetentionRatio: &ratio},
		},
	}

	nodes, _ := GenerateDefaultCheckNodes(cfg)
	if len(nodes) != 1 {
		t.Fatalf("expected 1 check for intermediate with no upstream, got %d", len(nodes))
	}

	names := map[string]bool{}
	for _, n := range nodes {
		names[n.Name] = true
	}
	if !names["check:int_orders:default:unique_grain"] {
		t.Error("missing unique_grain check for intermediate")
	}
	if names["check:int_orders:default:not_null_grain"] {
		t.Error("intermediate should not have not_null_grain check")
	}
}

func TestGenerateDefaultCheckNodes_Intermediate_WithUpstream(t *testing.T) {
	ratio := 0.5
	cfg := &config.PipelineConfig{
		Pipeline: "test",
		Assets: []config.AssetConfig{
			{
				Name:              "int_orders",
				Type:              "sql",
				Layer:             "intermediate",
				Grain:             "order_id",
				DestinationConnection: "bq",
				Upstream:          []string{"stg_orders"},
				MinRetentionRatio: &ratio,
			},
		},
	}

	nodes, deps := GenerateDefaultCheckNodes(cfg)
	if len(nodes) != 3 {
		t.Fatalf("expected 3 checks for intermediate with upstream, got %d", len(nodes))
	}

	names := map[string]bool{}
	for _, n := range nodes {
		names[n.Name] = true
	}
	if !names["check:int_orders:default:unique_grain"] {
		t.Error("missing unique_grain check")
	}
	if !names["check:int_orders:default:fan_out"] {
		t.Error("missing fan_out check")
	}
	if !names["check:int_orders:default:row_retention"] {
		t.Error("missing row_retention check")
	}
	if names["check:int_orders:default:not_null_grain"] {
		t.Error("intermediate should not have not_null_grain check")
	}

	for _, n := range nodes {
		d := deps[n.Name]
		if len(d) != 1 || d[0] != "int_orders" {
			t.Errorf("check %s deps: %v", n.Name, d)
		}
	}
}

func TestGenerateDefaultCheckNodes_Intermediate_PrimaryUpstream(t *testing.T) {
	ratio := 0.5
	cfg := &config.PipelineConfig{
		Pipeline: "test",
		Assets: []config.AssetConfig{
			{
				Name:              "int_orders",
				Type:              "sql",
				Layer:             "intermediate",
				Grain:             "order_id",
				DestinationConnection: "bq",
				Upstream:          []string{"stg_orders", "stg_customers"},
				PrimaryUpstream:   "stg_customers",
				MinRetentionRatio: &ratio,
			},
		},
	}

	nodes, _ := GenerateDefaultCheckNodes(cfg)
	if len(nodes) != 3 {
		t.Fatalf("expected 3 checks for intermediate with primary_upstream, got %d", len(nodes))
	}

	for _, n := range nodes {
		if strings.Contains(n.Name, "fan_out") || strings.Contains(n.Name, "row_retention") {
			if !strings.Contains(n.InlineSQL, "stg_customers") {
				t.Errorf("check %s should reference primary_upstream stg_customers: %s", n.Name, n.InlineSQL)
			}
		}
	}
}

func TestGenerateDefaultCheckNodes_Intermediate_FanOutDisabled(t *testing.T) {
	ratio := 0.5
	fanOut := false
	cfg := &config.PipelineConfig{
		Pipeline: "test",
		Assets: []config.AssetConfig{
			{
				Name:              "int_orders",
				Type:              "sql",
				Layer:             "intermediate",
				Grain:             "order_id",
				DestinationConnection: "bq",
				Upstream:          []string{"stg_orders"},
				MinRetentionRatio: &ratio,
				FanOutCheck:       &fanOut,
			},
		},
	}

	nodes, _ := GenerateDefaultCheckNodes(cfg)
	if len(nodes) != 2 {
		t.Fatalf("expected 2 checks for intermediate with fan_out disabled, got %d", len(nodes))
	}

	names := map[string]bool{}
	for _, n := range nodes {
		names[n.Name] = true
	}
	if !names["check:int_orders:default:unique_grain"] {
		t.Error("missing unique_grain check")
	}
	if !names["check:int_orders:default:row_retention"] {
		t.Error("missing row_retention check")
	}
	if names["check:int_orders:default:fan_out"] {
		t.Error("fan_out check should be skipped when FanOutCheck is false")
	}
}

func TestGenerateDefaultCheckNodes_Intermediate_SQLContent(t *testing.T) {
	ratio := 0.5
	cfg := &config.PipelineConfig{
		Pipeline: "test",
		Assets: []config.AssetConfig{
			{
				Name:              "int_orders",
				Type:              "sql",
				Layer:             "intermediate",
				Grain:             "order_id",
				DestinationConnection: "bq",
				Upstream:          []string{"stg_orders"},
				MinRetentionRatio: &ratio,
			},
		},
	}

	nodes, _ := GenerateDefaultCheckNodes(cfg)
	nameToSQL := map[string]string{}
	for _, n := range nodes {
		nameToSQL[n.Name] = n.InlineSQL
	}

	fanOutSQL := nameToSQL["check:int_orders:default:fan_out"]
	if fanOutSQL == "" {
		t.Fatal("missing fan_out check")
	}
	if !strings.Contains(fanOutSQL, "FAN_OUT_DETECTED") {
		t.Errorf("fan_out SQL missing FAN_OUT_DETECTED: %s", fanOutSQL)
	}
	if !strings.Contains(fanOutSQL, "stg_orders") {
		t.Errorf("fan_out SQL missing upstream table: %s", fanOutSQL)
	}
	if !strings.Contains(fanOutSQL, "int_orders") {
		t.Errorf("fan_out SQL missing asset table: %s", fanOutSQL)
	}

	rowRetentionSQL := nameToSQL["check:int_orders:default:row_retention"]
	if rowRetentionSQL == "" {
		t.Fatal("missing row_retention check")
	}
	if !strings.Contains(rowRetentionSQL, "SUSPICIOUS_ROW_LOSS") {
		t.Errorf("row_retention SQL missing SUSPICIOUS_ROW_LOSS: %s", rowRetentionSQL)
	}
	if !strings.Contains(rowRetentionSQL, "SAFE_DIVIDE") {
		t.Errorf("row_retention SQL missing SAFE_DIVIDE: %s", rowRetentionSQL)
	}
	if !strings.Contains(rowRetentionSQL, "0.5") {
		t.Errorf("row_retention SQL missing retention ratio: %s", rowRetentionSQL)
	}
	if !strings.Contains(rowRetentionSQL, "stg_orders") {
		t.Errorf("row_retention SQL missing upstream table: %s", rowRetentionSQL)
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

func TestGenerateDefaultCheckNodesWithDirectives_SourceCompleteness(t *testing.T) {
	cfg := &config.PipelineConfig{
		Pipeline: "test",
		Assets: []config.AssetConfig{
			{Name: "stg_orders", Type: "sql", Layer: "staging", Grain: "order_id", DestinationConnection: "bq"},
		},
	}
	directives := map[string]graph.Directives{
		"stg_orders": {
			SourceTable: "raw.orders",
			SourcePK:    "id",
		},
	}

	nodes, _ := GenerateDefaultCheckNodesWithDirectives(cfg, directives)
	if len(nodes) != 6 {
		t.Fatalf("expected 6 checks for staging with source_table directive, got %d", len(nodes))
	}

	names := map[string]bool{}
	for _, n := range nodes {
		names[n.Name] = true
	}
	if !names["check:stg_orders:default:source_completeness"] {
		t.Error("missing source_completeness check")
	}

	for _, n := range nodes {
		if strings.Contains(n.Name, "source_completeness") {
			if !strings.Contains(n.InlineSQL, "raw.orders") {
				t.Errorf("source_completeness SQL missing source_table: %s", n.InlineSQL)
			}
			if !strings.Contains(n.InlineSQL, "MISSING_FROM_STAGING") {
				t.Errorf("source_completeness SQL missing MISSING_FROM_STAGING: %s", n.InlineSQL)
			}
			if !strings.Contains(n.InlineSQL, "MISSING_FROM_SOURCE") {
				t.Errorf("source_completeness SQL missing MISSING_FROM_SOURCE: %s", n.InlineSQL)
			}
		}
	}
}

func TestGenerateDefaultCheckNodesWithDirectives_SourceCompletenessDefaultPK(t *testing.T) {
	cfg := &config.PipelineConfig{
		Pipeline: "test",
		Assets: []config.AssetConfig{
			{Name: "stg_orders", Type: "sql", Layer: "staging", Grain: "order_id", DestinationConnection: "bq"},
		},
	}
	directives := map[string]graph.Directives{
		"stg_orders": {
			SourceTable: "raw.orders",
			// SourcePK not set — should default to grain
		},
	}

	nodes, _ := GenerateDefaultCheckNodesWithDirectives(cfg, directives)
	if len(nodes) != 6 {
		t.Fatalf("expected 6 checks, got %d", len(nodes))
	}

	for _, n := range nodes {
		if strings.Contains(n.Name, "source_completeness") {
			// source_pk defaults to grain (order_id)
			if !strings.Contains(n.InlineSQL, "order_id AS pk") {
				t.Errorf("source_completeness SQL should use grain as default source_pk: %s", n.InlineSQL)
			}
		}
	}
}

func TestGenerateDefaultCheckNodesWithDirectives_NoSourceTable(t *testing.T) {
	cfg := &config.PipelineConfig{
		Pipeline: "test",
		Assets: []config.AssetConfig{
			{Name: "stg_orders", Type: "sql", Layer: "staging", Grain: "order_id", DestinationConnection: "bq"},
		},
	}
	directives := map[string]graph.Directives{
		"stg_orders": {
			// No SourceTable
		},
	}

	nodes, _ := GenerateDefaultCheckNodesWithDirectives(cfg, directives)
	if len(nodes) != 5 {
		t.Fatalf("expected 5 checks without source_table directive, got %d", len(nodes))
	}

	names := map[string]bool{}
	for _, n := range nodes {
		names[n.Name] = true
	}
	if names["check:stg_orders:default:source_completeness"] {
		t.Error("should not have source_completeness check without source_table directive")
	}
}

func TestGenerateDefaultCheckNodes_StagingSQLContent(t *testing.T) {
	cfg := &config.PipelineConfig{
		Pipeline: "test",
		Assets: []config.AssetConfig{
			{Name: "stg_users", Type: "sql", Layer: "staging", Grain: "user_id", DestinationConnection: "bq"},
		},
	}

	nodes, _ := GenerateDefaultCheckNodes(cfg)
	nameToSQL := map[string]string{}
	for _, n := range nodes {
		nameToSQL[n.Name] = n.InlineSQL
	}

	notEmptySQL := nameToSQL["check:stg_users:default:not_empty"]
	if notEmptySQL == "" {
		t.Fatal("missing not_empty check")
	}
	if !strings.Contains(notEmptySQL, "EMPTY_TABLE") {
		t.Errorf("not_empty SQL missing EMPTY_TABLE: %s", notEmptySQL)
	}
	if !strings.Contains(notEmptySQL, "NOT EXISTS") {
		t.Errorf("not_empty SQL missing NOT EXISTS: %s", notEmptySQL)
	}
	if !strings.Contains(notEmptySQL, "stg_users") {
		t.Errorf("not_empty SQL missing asset name: %s", notEmptySQL)
	}

	noFutureSQL := nameToSQL["check:stg_users:default:no_future_timestamps"]
	if noFutureSQL == "" {
		t.Fatal("missing no_future_timestamps check")
	}
	if !strings.Contains(noFutureSQL, "CURRENT_TIMESTAMP()") {
		t.Errorf("no_future_timestamps SQL missing CURRENT_TIMESTAMP(): %s", noFutureSQL)
	}
	if !strings.Contains(noFutureSQL, "FUTURE_CREATED_AT") {
		t.Errorf("no_future_timestamps SQL missing FUTURE_CREATED_AT: %s", noFutureSQL)
	}
	if !strings.Contains(noFutureSQL, "user_id") {
		t.Errorf("no_future_timestamps SQL missing grain: %s", noFutureSQL)
	}

	updatedAtSQL := nameToSQL["check:stg_users:default:updated_at_gte_created_at"]
	if updatedAtSQL == "" {
		t.Fatal("missing updated_at_gte_created_at check")
	}
	if !strings.Contains(updatedAtSQL, "updated_at < created_at") {
		t.Errorf("updated_at_gte_created_at SQL missing condition: %s", updatedAtSQL)
	}
	if !strings.Contains(updatedAtSQL, "2024-01-01") {
		t.Errorf("updated_at_gte_created_at SQL missing cutoff date: %s", updatedAtSQL)
	}
	if !strings.Contains(updatedAtSQL, "user_id") {
		t.Errorf("updated_at_gte_created_at SQL missing grain: %s", updatedAtSQL)
	}
}

func TestGenerateDefaultCheckNodesWithDirectives_NilDirectives(t *testing.T) {
	cfg := &config.PipelineConfig{
		Pipeline: "test",
		Assets: []config.AssetConfig{
			{Name: "stg_orders", Type: "sql", Layer: "staging", Grain: "order_id", DestinationConnection: "bq"},
		},
	}

	nodes, _ := GenerateDefaultCheckNodesWithDirectives(cfg, nil)
	if len(nodes) != 5 {
		t.Fatalf("expected 5 checks with nil directives, got %d", len(nodes))
	}
}
