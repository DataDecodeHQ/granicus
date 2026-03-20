package rerun

import (
	"fmt"

	"github.com/DataDecodeHQ/granicus/internal/events"
	"github.com/DataDecodeHQ/granicus/internal/graph"
)

// ComputeRerunSet determines which nodes to re-execute by collecting failed nodes and all their descendants.
func ComputeRerunSet(store *events.Store, runID string, g *graph.Graph) ([]string, []string, error) {
	failedNames, err := store.GetFailedNodes(runID)
	if err != nil {
		return nil, nil, fmt.Errorf("reading run %s: %w", runID, err)
	}

	if len(failedNames) == 0 {
		return nil, nil, fmt.Errorf("run %s has no failed nodes", runID)
	}

	// Build rerun set: failed nodes + all their descendants
	rerunSet := make(map[string]bool)
	var warnings []string

	for _, name := range failedNames {
		if _, ok := g.Assets[name]; !ok {
			warnings = append(warnings, fmt.Sprintf("node %q from failed run no longer exists in config", name))
			continue
		}
		rerunSet[name] = true
		for _, desc := range g.Descendants(name) {
			rerunSet[desc] = true
		}
	}

	var result []string
	for name := range rerunSet {
		result = append(result, name)
	}

	return result, warnings, nil
}
