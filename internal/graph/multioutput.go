package graph

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
				DestinationConnection: input.DestinationConnection,
				SourceConnection:      input.SourceConnection,
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
