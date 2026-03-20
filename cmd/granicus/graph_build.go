package main

import (
	"fmt"

	"github.com/DataDecodeHQ/granicus/internal/checker"
	"github.com/DataDecodeHQ/granicus/internal/config"
	"github.com/DataDecodeHQ/granicus/internal/graph"
)

// buildPipelineGraph parses directives, generates check nodes, wires source checks,
// and builds the execution graph from a loaded pipeline config.
// parseRoot is the directory used to resolve relative SQL/source file paths.
// Returns the graph and the raw deps map (used by callers that need it post-build).
func buildPipelineGraph(cfg *config.PipelineConfig, parseRoot string) (*graph.Graph, map[string][]string, error) {
	deps, directives, err := graph.ParseAllDirectives(cfg, parseRoot)
	if err != nil {
		return nil, nil, fmt.Errorf("dependencies: %w", err)
	}

	inputs := graph.ConfigToAssetInputs(cfg)

	// Apply directives (time_column, interval_unit, etc.) to asset inputs
	for i := range inputs {
		if d, ok := directives[inputs[i].Name]; ok {
			inputs[i].TimeColumn = d.TimeColumn
			inputs[i].IntervalUnit = d.IntervalUnit
			inputs[i].Lookback = d.Lookback
			inputs[i].StartDate = d.StartDate
			inputs[i].BatchSize = d.BatchSize
			if d.Layer != "" {
				inputs[i].Layer = d.Layer
			}
			if d.Grain != "" {
				inputs[i].Grain = d.Grain
			}
			if d.DefaultChecks != nil {
				inputs[i].DefaultChecks = d.DefaultChecks
			}
		}
	}

	// Add source phantom nodes
	sourceNodes := graph.SourcePhantomNodes(cfg)
	inputs = append(inputs, sourceNodes...)

	// Generate check nodes and merge into graph
	checkNodes, checkDeps := checker.GenerateCheckNodes(cfg)
	inputs = append(inputs, checkNodes...)
	for k, v := range checkDeps {
		deps[k] = v
	}

	// Generate default checks based on layer/grain
	defaultNodes, defaultDeps := checker.GenerateDefaultCheckNodes(cfg)
	inputs = append(inputs, defaultNodes...)
	for k, v := range defaultDeps {
		deps[k] = v
	}

	// Generate source check nodes
	sourceCheckNodes, sourceCheckDeps := checker.GenerateSourceCheckNodes(cfg)
	inputs = append(inputs, sourceCheckNodes...)
	for k, v := range sourceCheckDeps {
		deps[k] = v
	}

	// Wire source checks to gate staging assets
	if len(sourceCheckNodes) > 0 {
		var sourceCheckNames []string
		for _, sc := range sourceCheckNodes {
			sourceCheckNames = append(sourceCheckNames, sc.Name)
		}
		for i := range inputs {
			if inputs[i].Layer == "staging" {
				if deps[inputs[i].Name] == nil {
					deps[inputs[i].Name] = sourceCheckNames
				} else {
					deps[inputs[i].Name] = append(deps[inputs[i].Name], sourceCheckNames...)
				}
			}
		}
	}

	g, err := graph.BuildGraph(inputs, deps)
	if err != nil {
		return nil, nil, fmt.Errorf("graph: %w", err)
	}

	return g, deps, nil
}
