package rerun

import (
	"path/filepath"
	"sort"
	"testing"

	"github.com/Andrew-DataDecode/Granicus/internal/events"
	"github.com/Andrew-DataDecode/Granicus/internal/graph"
)

func newTestEventStore(t *testing.T) *events.Store {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "events.db")
	s, err := events.New(dbPath)
	if err != nil {
		t.Fatalf("creating store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestComputeRerunSet_Basic(t *testing.T) {
	store := newTestEventStore(t)
	runID := "run_20260225_120000_test"

	// Emit node events: A success, B failed, C skipped, D success
	store.Emit(events.Event{RunID: runID, Pipeline: "p", Asset: "A", EventType: "node_succeeded"})
	store.Emit(events.Event{RunID: runID, Pipeline: "p", Asset: "B", EventType: "node_failed"})
	store.Emit(events.Event{RunID: runID, Pipeline: "p", Asset: "C", EventType: "node_skipped"})
	store.Emit(events.Event{RunID: runID, Pipeline: "p", Asset: "D", EventType: "node_succeeded"})

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
	store := newTestEventStore(t)
	runID := "run_20260225_120000_gone"

	store.Emit(events.Event{RunID: runID, Pipeline: "p", Asset: "removed_node", EventType: "node_failed"})

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
	store := newTestEventStore(t)
	runID := "run_20260225_120000_ok"

	store.Emit(events.Event{RunID: runID, Pipeline: "p", Asset: "A", EventType: "node_succeeded"})

	g, _ := graph.BuildGraph(
		[]graph.AssetInput{{Name: "A", Type: "shell", Source: "a.sh"}},
		nil,
	)

	_, _, err := ComputeRerunSet(store, runID, g)
	if err == nil {
		t.Error("expected error for no failures")
	}
}
