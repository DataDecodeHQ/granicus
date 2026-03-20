package context

import (
	"testing"

	"github.com/DataDecodeHQ/granicus/internal/config"
	"github.com/DataDecodeHQ/granicus/internal/graph"
)

func testGraph(t *testing.T) (*graph.Graph, *config.PipelineConfig) {
	t.Helper()
	cfg := &config.PipelineConfig{
		Pipeline: "test",
		Resources: map[string]*config.ResourceConfig{
			"bq": {Name: "bq", Type: "bigquery", Properties: map[string]string{"project": "p", "dataset": "dev_analytics"}},
		},
		Datasets: map[string]string{
			"staging":      "dev_staging",
			"intermediate": "dev_analytics",
		},
		Assets: []config.AssetConfig{
			{Name: "stg_orders", Type: "sql", Source: "models/stg_orders.sql", DestinationResource: "bq", Layer: "staging"},
			{Name: "int_orders", Type: "sql", Source: "models/int_orders.sql", DestinationResource: "bq", Layer: "intermediate"},
		},
	}

	inputs := []graph.AssetInput{
		{Name: "stg_orders", Type: "sql", Source: "models/stg_orders.sql", DestinationResource: "bq", Layer: "staging"},
		{Name: "int_orders", Type: "sql", Source: "models/int_orders.sql", DestinationResource: "bq", Layer: "intermediate"},
		{Name: "check:int_orders_pk", Type: "sql_check", InlineSQL: "SELECT 1", Blocking: true},
	}
	deps := map[string][]string{
		"int_orders":          {"stg_orders"},
		"check:int_orders_pk": {"int_orders"},
	}
	g, err := graph.BuildGraph(inputs, deps)
	if err != nil {
		t.Fatal(err)
	}
	return g, cfg
}

func TestExtractLineage(t *testing.T) {
	g, cfg := testGraph(t)
	lineage := ExtractLineage(g, cfg)

	if len(lineage) != 1 {
		t.Fatalf("expected 1 lineage edge, got %d", len(lineage))
	}

	l := lineage[0]
	if l.SourceAsset != "stg_orders" || l.TargetAsset != "int_orders" {
		t.Errorf("unexpected edge: %s -> %s", l.SourceAsset, l.TargetAsset)
	}
	if l.SourceDataset != "dev_analytics" {
		t.Errorf("source dataset: expected dev_analytics, got %q", l.SourceDataset)
	}
	if l.TargetDataset != "dev_analytics" {
		t.Errorf("target dataset: expected dev_analytics, got %q", l.TargetDataset)
	}
}

func TestExtractAssets(t *testing.T) {
	g, cfg := testGraph(t)
	assets := ExtractAssets(g, cfg, t.TempDir())

	if len(assets) != 2 {
		t.Fatalf("expected 2 assets (no checks), got %d", len(assets))
	}

	found := map[string]bool{}
	for _, a := range assets {
		found[a.AssetName] = true
		if a.AssetName == "int_orders" {
			if a.Layer != "intermediate" {
				t.Errorf("int_orders layer: %q", a.Layer)
			}
			if a.Dataset != "dev_analytics" {
				t.Errorf("int_orders dataset: %q", a.Dataset)
			}
		}
	}
	if found["check:int_orders_pk"] {
		t.Error("check nodes should be excluded from assets")
	}
}

func TestExtractLineage_SkipsChecks(t *testing.T) {
	g, cfg := testGraph(t)
	lineage := ExtractLineage(g, cfg)

	for _, l := range lineage {
		if l.SourceAsset == "check:int_orders_pk" || l.TargetAsset == "check:int_orders_pk" {
			t.Error("check nodes should be excluded from lineage")
		}
	}
}
