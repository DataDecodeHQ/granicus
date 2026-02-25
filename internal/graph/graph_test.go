package graph

import (
	"sort"
	"strings"
	"testing"
)

func TestBuildGraph_Valid(t *testing.T) {
	assets := []AssetInput{
		{Name: "A", Type: "shell", Source: "a.sh"},
		{Name: "B", Type: "shell", Source: "b.sh"},
		{Name: "C", Type: "shell", Source: "c.sh"},
	}
	deps := map[string][]string{
		"B": {"A"},
		"C": {"A", "B"},
	}

	g, err := BuildGraph(assets, deps)
	if err != nil {
		t.Fatal(err)
	}

	if len(g.Assets) != 3 {
		t.Errorf("expected 3 assets, got %d", len(g.Assets))
	}
	if len(g.Assets["B"].DependsOn) != 1 || g.Assets["B"].DependsOn[0] != "A" {
		t.Errorf("B deps: %v", g.Assets["B"].DependsOn)
	}
	if len(g.Assets["C"].DependsOn) != 2 {
		t.Errorf("C deps: %v", g.Assets["C"].DependsOn)
	}
	// A should have DependedOnBy = [B, C]
	dob := g.Assets["A"].DependedOnBy
	sort.Strings(dob)
	if len(dob) != 2 || dob[0] != "B" || dob[1] != "C" {
		t.Errorf("A dependedOnBy: %v", dob)
	}
}

func TestBuildGraph_CycleDetection(t *testing.T) {
	// A -> B -> C -> A
	assets := []AssetInput{
		{Name: "A", Type: "shell", Source: "a.sh"},
		{Name: "B", Type: "shell", Source: "b.sh"},
		{Name: "C", Type: "shell", Source: "c.sh"},
	}
	deps := map[string][]string{
		"A": {"C"},
		"B": {"A"},
		"C": {"B"},
	}

	_, err := BuildGraph(assets, deps)
	if err == nil {
		t.Fatal("expected cycle error")
	}
	if !strings.Contains(err.Error(), "cycle") {
		t.Errorf("error should mention cycle: %v", err)
	}
}

func TestBuildGraph_SelfLoop(t *testing.T) {
	assets := []AssetInput{
		{Name: "A", Type: "shell", Source: "a.sh"},
	}
	deps := map[string][]string{
		"A": {"A"},
	}

	_, err := BuildGraph(assets, deps)
	if err == nil {
		t.Fatal("expected cycle error for self-loop")
	}
}

func TestBuildGraph_MissingDependency(t *testing.T) {
	assets := []AssetInput{
		{Name: "A", Type: "shell", Source: "a.sh"},
	}
	deps := map[string][]string{
		"A": {"nonexistent"},
	}

	_, err := BuildGraph(assets, deps)
	if err == nil {
		t.Fatal("expected error for missing dependency")
	}
	if !strings.Contains(err.Error(), "nonexistent") {
		t.Errorf("error should mention missing dep: %v", err)
	}
}

func TestBuildGraph_DuplicateAssetName(t *testing.T) {
	assets := []AssetInput{
		{Name: "A", Type: "shell", Source: "a.sh"},
		{Name: "A", Type: "shell", Source: "b.sh"},
	}

	_, err := BuildGraph(assets, nil)
	if err == nil {
		t.Fatal("expected error for duplicate")
	}
}

func TestBuildGraph_RootNodes(t *testing.T) {
	assets := []AssetInput{
		{Name: "root1", Type: "shell", Source: "r1.sh"},
		{Name: "root2", Type: "shell", Source: "r2.sh"},
		{Name: "child", Type: "shell", Source: "c.sh"},
	}
	deps := map[string][]string{
		"child": {"root1", "root2"},
	}

	g, err := BuildGraph(assets, deps)
	if err != nil {
		t.Fatal(err)
	}

	roots := g.RootNodes
	sort.Strings(roots)
	if len(roots) != 2 || roots[0] != "root1" || roots[1] != "root2" {
		t.Errorf("expected [root1 root2], got %v", roots)
	}
}

func TestBuildGraph_Descendants(t *testing.T) {
	// A -> B -> C -> D
	assets := []AssetInput{
		{Name: "A", Type: "shell", Source: "a.sh"},
		{Name: "B", Type: "shell", Source: "b.sh"},
		{Name: "C", Type: "shell", Source: "c.sh"},
		{Name: "D", Type: "shell", Source: "d.sh"},
	}
	deps := map[string][]string{
		"B": {"A"},
		"C": {"B"},
		"D": {"C"},
	}

	g, err := BuildGraph(assets, deps)
	if err != nil {
		t.Fatal(err)
	}

	desc := g.Descendants("A")
	sort.Strings(desc)
	if len(desc) != 3 {
		t.Fatalf("expected 3 descendants, got %v", desc)
	}
	if desc[0] != "B" || desc[1] != "C" || desc[2] != "D" {
		t.Errorf("expected [B C D], got %v", desc)
	}

	// Leaf has no descendants
	desc = g.Descendants("D")
	if len(desc) != 0 {
		t.Errorf("D should have no descendants, got %v", desc)
	}
}

func TestBuildGraph_Subgraph(t *testing.T) {
	// A -> B -> C, A -> D
	assets := []AssetInput{
		{Name: "A", Type: "shell", Source: "a.sh"},
		{Name: "B", Type: "shell", Source: "b.sh"},
		{Name: "C", Type: "shell", Source: "c.sh"},
		{Name: "D", Type: "shell", Source: "d.sh"},
	}
	deps := map[string][]string{
		"B": {"A"},
		"C": {"B"},
		"D": {"A"},
	}

	g, err := BuildGraph(assets, deps)
	if err != nil {
		t.Fatal(err)
	}

	// Subgraph targeting C should include A, B, C
	sub := g.Subgraph([]string{"C"})
	sort.Strings(sub)
	if len(sub) != 3 || sub[0] != "A" || sub[1] != "B" || sub[2] != "C" {
		t.Errorf("expected [A B C], got %v", sub)
	}

	// Subgraph targeting D should include A, D
	sub = g.Subgraph([]string{"D"})
	sort.Strings(sub)
	if len(sub) != 2 || sub[0] != "A" || sub[1] != "D" {
		t.Errorf("expected [A D], got %v", sub)
	}
}

func TestBuildGraph_LayerGrainDefaultChecks(t *testing.T) {
	dc := true
	assets := []AssetInput{
		{Name: "stg", Type: "sql", Source: "stg.sql", Layer: "staging", Grain: "order_id", DefaultChecks: &dc},
		{Name: "ent", Type: "sql", Source: "ent.sql", Layer: "entity"},
	}

	g, err := BuildGraph(assets, nil)
	if err != nil {
		t.Fatal(err)
	}

	stg := g.Assets["stg"]
	if stg.Layer != "staging" {
		t.Errorf("expected layer=staging, got %q", stg.Layer)
	}
	if stg.Grain != "order_id" {
		t.Errorf("expected grain=order_id, got %q", stg.Grain)
	}
	if stg.DefaultChecks == nil || *stg.DefaultChecks != true {
		t.Errorf("expected default_checks=true, got %v", stg.DefaultChecks)
	}

	ent := g.Assets["ent"]
	if ent.Layer != "entity" {
		t.Errorf("expected layer=entity, got %q", ent.Layer)
	}
	if ent.DefaultChecks != nil {
		t.Errorf("expected default_checks=nil, got %v", ent.DefaultChecks)
	}
}

func TestTopologicalSort(t *testing.T) {
	assets := []AssetInput{
		{Name: "A", Type: "shell", Source: "a.sh"},
		{Name: "B", Type: "shell", Source: "b.sh"},
		{Name: "C", Type: "shell", Source: "c.sh"},
	}
	deps := map[string][]string{
		"B": {"A"},
		"C": {"B"},
	}

	g, err := BuildGraph(assets, deps)
	if err != nil {
		t.Fatal(err)
	}

	order := g.TopologicalSort()
	if len(order) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(order))
	}

	// A must come before B, B before C
	pos := make(map[string]int)
	for i, n := range order {
		pos[n] = i
	}
	if pos["A"] > pos["B"] || pos["B"] > pos["C"] {
		t.Errorf("invalid order: %v", order)
	}
}
