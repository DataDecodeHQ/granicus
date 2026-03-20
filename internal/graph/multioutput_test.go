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

func TestExpandMultiOutputWithDeps_RewritesDeps(t *testing.T) {
	inputs := []AssetInput{
		{Name: "_config_loader", Type: "python", Source: "loader.py"},
		{Name: "downstream", Type: "sql", Source: "downstream.sql"},
	}
	directives := map[string]*Directives{
		"_config_loader": {Produces: []string{"table_a", "table_b", "table_c"}},
	}
	deps := map[string][]string{
		"downstream": {"_config_loader", "other_dep"},
	}

	expanded, newDeps := ExpandMultiOutputWithDeps(inputs, directives, deps)

	// Should have 4 nodes: table_a, table_b, table_c, downstream
	if len(expanded) != 4 {
		t.Fatalf("expected 4 expanded inputs, got %d", len(expanded))
	}

	// downstream should now depend on table_a, table_b, table_c, other_dep
	dsDeps := newDeps["downstream"]
	if len(dsDeps) != 4 {
		t.Fatalf("expected 4 deps for downstream, got %d: %v", len(dsDeps), dsDeps)
	}
	expected := map[string]bool{"table_a": true, "table_b": true, "table_c": true, "other_dep": true}
	for _, d := range dsDeps {
		if !expected[d] {
			t.Errorf("unexpected dep: %q", d)
		}
	}

	// Parent entry should be gone from deps
	if _, ok := newDeps["_config_loader"]; ok {
		t.Error("parent dep entry should be removed")
	}
}

func TestExpandMultiOutputWithDeps_ParentDepsInherited(t *testing.T) {
	inputs := []AssetInput{
		{Name: "source_data", Type: "sql", Source: "source.sql"},
		{Name: "multi_writer", Type: "python", Source: "writer.py"},
		{Name: "consumer", Type: "sql", Source: "consumer.sql"},
	}
	directives := map[string]*Directives{
		"multi_writer": {Produces: []string{"out_x", "out_y"}},
	}
	deps := map[string][]string{
		"multi_writer": {"source_data"},
		"consumer":     {"multi_writer"},
	}

	expanded, newDeps := ExpandMultiOutputWithDeps(inputs, directives, deps)

	// 4 nodes: source_data, out_x, out_y, consumer
	if len(expanded) != 4 {
		t.Fatalf("expected 4, got %d", len(expanded))
	}

	// out_x and out_y should inherit parent's dep on source_data
	for _, out := range []string{"out_x", "out_y"} {
		if len(newDeps[out]) != 1 || newDeps[out][0] != "source_data" {
			t.Errorf("%s deps: %v, expected [source_data]", out, newDeps[out])
		}
	}

	// consumer should depend on out_x, out_y (expanded from multi_writer)
	cDeps := newDeps["consumer"]
	if len(cDeps) != 2 {
		t.Fatalf("consumer deps: %v, expected 2", cDeps)
	}

	// Build graph to verify it's valid
	g, err := BuildGraph(expanded, newDeps)
	if err != nil {
		t.Fatalf("BuildGraph: %v", err)
	}
	if _, ok := g.Assets["out_x"]; !ok {
		t.Error("out_x missing from graph")
	}
	if _, ok := g.Assets["out_y"]; !ok {
		t.Error("out_y missing from graph")
	}
}

func TestExpandMultiOutput_InheritsFields(t *testing.T) {
	inputs := []AssetInput{
		{
			Name: "multi", Type: "python", Source: "m.py",
			DestinationResource: "bq", TimeColumn: "created_at",
			IntervalUnit: "day", StartDate: "2024-01-01",
		},
	}
	directives := map[string]*Directives{
		"multi": {Produces: []string{"out_a", "out_b"}},
	}

	result := ExpandMultiOutput(inputs, directives)
	for _, r := range result {
		if r.DestinationResource != "bq" {
			t.Errorf("dest_conn: %q", r.DestinationResource)
		}
		if r.TimeColumn != "created_at" {
			t.Errorf("time_column: %q", r.TimeColumn)
		}
		if r.IntervalUnit != "day" {
			t.Errorf("interval_unit: %q", r.IntervalUnit)
		}
	}
}
