package graph

// ExpandMultiOutput replaces assets that have a "produces" directive with one node per output name.
func ExpandMultiOutput(inputs []AssetInput, directives map[string]*Directives) []AssetInput {
	var result []AssetInput

	for _, input := range inputs {
		d := directives[input.Name]
		if d == nil || len(d.Produces) == 0 {
			result = append(result, input)
			continue
		}

		// Expand into one node per output
		for _, outputName := range d.Produces {
			result = append(result, AssetInput{
				Name:                  outputName,
				Type:                  input.Type,
				Source:                input.Source,
				DestinationResource: input.DestinationResource,
				SourceResource:      input.SourceResource,
				TimeColumn:            input.TimeColumn,
				IntervalUnit:          input.IntervalUnit,
				Lookback:              input.Lookback,
				StartDate:             input.StartDate,
				BatchSize:             input.BatchSize,
				SourceAsset:           input.Name,
				MaxAttempts:           input.MaxAttempts,
				BackoffBase:           input.BackoffBase,
				RetryableErrors:       input.RetryableErrors,
			})
		}
	}

	return result
}

// ExpandMultiOutputWithDeps wraps ExpandMultiOutput and also rewrites the dependency map:
// - Parent's deps are copied to each output node
// - Any dep referencing a parent name is replaced with all its output names
func ExpandMultiOutputWithDeps(inputs []AssetInput, directives map[string]*Directives, deps map[string][]string) ([]AssetInput, map[string][]string) {
	expanded := ExpandMultiOutput(inputs, directives)

	// Build parent -> outputs map
	parentOutputs := make(map[string][]string)
	for _, input := range inputs {
		d := directives[input.Name]
		if d != nil && len(d.Produces) > 0 {
			parentOutputs[input.Name] = d.Produces
		}
	}

	if len(parentOutputs) == 0 {
		return expanded, deps
	}

	newDeps := make(map[string][]string, len(deps))

	// Copy and rewrite existing deps
	for name, depList := range deps {
		// If this node is a parent that was expanded, copy its deps to each output
		if outputs, ok := parentOutputs[name]; ok {
			for _, out := range outputs {
				newDeps[out] = append(newDeps[out], rewriteDepList(depList, parentOutputs)...)
			}
			continue
		}

		newDeps[name] = rewriteDepList(depList, parentOutputs)
	}

	return expanded, newDeps
}

// rewriteDepList replaces any parent name in a dep list with its output names.
func rewriteDepList(depList []string, parentOutputs map[string][]string) []string {
	var result []string
	for _, dep := range depList {
		if outputs, ok := parentOutputs[dep]; ok {
			result = append(result, outputs...)
		} else {
			result = append(result, dep)
		}
	}
	return result
}
