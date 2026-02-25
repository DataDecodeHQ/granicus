package graph

import (
	"testing"
)

func TestExpandMultiOutput_Basic(t *testing.T) {
	inputs := []AssetInput{
		{Name: "extract_events", Type: "python", Source: "scripts/extract.py"},
	}
	directives := map[string]*Directives{
		"extract_events": {Produces: []string{"user_events", "order_events", "page_views"}},
	}

	result := ExpandMultiOutput(inputs, directives)

	if len(result) != 3 {
		t.Fatalf("expected 3 outputs, got %d", len(result))
	}

	if result[0].Name != "user_events" || result[1].Name != "order_events" || result[2].Name != "page_views" {
		t.Errorf("names: %v, %v, %v", result[0].Name, result[1].Name, result[2].Name)
	}

	for _, r := range result {
		if r.Source != "scripts/extract.py" {
			t.Errorf("source should be shared: %q", r.Source)
		}
		if r.Type != "python" {
			t.Errorf("type should be inherited: %q", r.Type)
		}
		if r.SourceAsset != "extract_events" {
			t.Errorf("source_asset should reference parent: %q", r.SourceAsset)
		}
	}
}

func TestExpandMultiOutput_NoProduces(t *testing.T) {
	inputs := []AssetInput{
		{Name: "simple", Type: "shell", Source: "x.sh"},
	}
	directives := map[string]*Directives{
		"simple": {},
	}

	result := ExpandMultiOutput(inputs, directives)
	if len(result) != 1 || result[0].Name != "simple" {
		t.Errorf("single output should pass through: %v", result)
	}
	if result[0].SourceAsset != "" {
		t.Errorf("SourceAsset should be empty for non-multi-output: %q", result[0].SourceAsset)
	}
}

func TestExpandMultiOutput_NilDirectives(t *testing.T) {
	inputs := []AssetInput{
		{Name: "a", Type: "shell", Source: "a.sh"},
	}
	directives := map[string]*Directives{}

	result := ExpandMultiOutput(inputs, directives)
	if len(result) != 1 || result[0].Name != "a" {
		t.Errorf("should pass through without directives: %v", result)
	}
}

func TestExpandMultiOutput_DownstreamDeps(t *testing.T) {
	inputs := []AssetInput{
		{Name: "extract", Type: "python", Source: "extract.py"},
		{Name: "transform", Type: "sql", Source: "transform.sql"},
	}
	directives := map[string]*Directives{
		"extract":   {Produces: []string{"users", "orders"}},
		"transform": {DependsOn: []string{"users"}},
	}

	result := ExpandMultiOutput(inputs, directives)

	// 2 from extract + 1 from transform
	if len(result) != 3 {
		t.Fatalf("expected 3, got %d", len(result))
	}

	// Build graph to verify deps work
	deps := map[string][]string{
		"transform": {"users"},
	}
	g, err := BuildGraph(result, deps)
	if err != nil {
		t.Fatalf("BuildGraph: %v", err)
	}

	if len(g.Assets["transform"].DependsOn) != 1 || g.Assets["transform"].DependsOn[0] != "users" {
		t.Errorf("transform deps: %v", g.Assets["transform"].DependsOn)
	}
}

func TestExpandMultiOutput_InheritsFields(t *testing.T) {
	inputs := []AssetInput{
		{
			Name: "multi", Type: "python", Source: "m.py",
			DestinationConnection: "bq", TimeColumn: "created_at",
			IntervalUnit: "day", StartDate: "2024-01-01",
		},
	}
	directives := map[string]*Directives{
		"multi": {Produces: []string{"out_a", "out_b"}},
	}

	result := ExpandMultiOutput(inputs, directives)
	for _, r := range result {
		if r.DestinationConnection != "bq" {
			t.Errorf("dest_conn: %q", r.DestinationConnection)
		}
		if r.TimeColumn != "created_at" {
			t.Errorf("time_column: %q", r.TimeColumn)
		}
		if r.IntervalUnit != "day" {
			t.Errorf("interval_unit: %q", r.IntervalUnit)
		}
	}
}
