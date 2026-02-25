package rerun

import (
	"fmt"

	"github.com/analytehealth/granicus/internal/graph"
	"github.com/analytehealth/granicus/internal/logging"
)

func ComputeRerunSet(store *logging.Store, runID string, g *graph.Graph) ([]string, []string, error) {
	nodes, err := store.ReadNodeResults(runID)
	if err != nil {
		return nil, nil, fmt.Errorf("reading run %s: %w", runID, err)
	}

	var failedNames []string
	for _, n := range nodes {
		if n.Status == "failed" {
			failedNames = append(failedNames, n.Asset)
		}
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
