package rerun

import (
	"sort"
	"testing"

	"github.com/analytehealth/granicus/internal/graph"
	"github.com/analytehealth/granicus/internal/logging"
)

func TestComputeRerunSet_Basic(t *testing.T) {
	dir := t.TempDir()
	store := logging.NewStore(dir)
	runID := "run_20260225_120000_test"

	// Write node results: A success, B failed, C skipped (depends on B), D success
	for _, entry := range []logging.NodeEntry{
		{Asset: "A", Status: "success"},
		{Asset: "B", Status: "failed", Error: "exit 1"},
		{Asset: "C", Status: "skipped", Error: "skipped: B failed"},
		{Asset: "D", Status: "success"},
	} {
		store.WriteNodeResult(runID, entry)
	}

	// Build graph: A -> B -> C, D independent
	g, err := graph.BuildGraph(
		[]graph.AssetInput{
			{Name: "A", Type: "shell", Source: "a.sh"},
			{Name: "B", Type: "shell", Source: "b.sh"},
			{Name: "C", Type: "shell", Source: "c.sh"},
			{Name: "D", Type: "shell", Source: "d.sh"},
		},
		map[string][]string{"B": {"A"}, "C": {"B"}},
	)
	if err != nil {
		t.Fatal(err)
	}

	rerun, warnings, err := ComputeRerunSet(store, runID, g)
	if err != nil {
		t.Fatal(err)
	}
	if len(warnings) != 0 {
		t.Errorf("unexpected warnings: %v", warnings)
	}

	sort.Strings(rerun)
	// Should include B (failed) and C (descendant of B), but NOT A or D
	if len(rerun) != 2 || rerun[0] != "B" || rerun[1] != "C" {
		t.Errorf("expected [B C], got %v", rerun)
	}
}

func TestComputeRerunSet_MissingNode(t *testing.T) {
	dir := t.TempDir()
	store := logging.NewStore(dir)
	runID := "run_20260225_120000_gone"

	store.WriteNodeResult(runID, logging.NodeEntry{Asset: "removed_node", Status: "failed"})

	g, _ := graph.BuildGraph(
		[]graph.AssetInput{{Name: "A", Type: "shell", Source: "a.sh"}},
		nil,
	)

	_, warnings, err := ComputeRerunSet(store, runID, g)
	if err != nil {
		t.Fatal(err)
	}
	if len(warnings) != 1 {
		t.Errorf("expected 1 warning, got %d: %v", len(warnings), warnings)
	}
}

func TestComputeRerunSet_NoFailures(t *testing.T) {
	dir := t.TempDir()
	store := logging.NewStore(dir)
	runID := "run_20260225_120000_ok"

	store.WriteNodeResult(runID, logging.NodeEntry{Asset: "A", Status: "success"})

	g, _ := graph.BuildGraph(
		[]graph.AssetInput{{Name: "A", Type: "shell", Source: "a.sh"}},
		nil,
	)

	_, _, err := ComputeRerunSet(store, runID, g)
	if err == nil {
		t.Error("expected error for no failures")
	}
}
