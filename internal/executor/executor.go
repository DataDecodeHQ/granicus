package executor

import (
	"strconv"
	"sync"
	"time"

	"github.com/analytehealth/granicus/internal/graph"
	"github.com/analytehealth/granicus/internal/state"
)

type NodeResult struct {
	AssetName string
	Status    string // "success", "failed", "skipped"
	StartTime time.Time
	EndTime   time.Time
	Duration  time.Duration
	Error     string
	Stdout    string
	Stderr    string
	ExitCode  int
	Metadata  map[string]string
}

type RunConfig struct {
	MaxParallel int
	Assets      []string
	ProjectRoot string
	RunID       string
	FromDate    string
	ToDate      string
	FullRefresh bool
	StateStore  *state.Store
}

type RunnerFunc func(asset *graph.Asset, projectRoot string, runID string) NodeResult

type RunResult struct {
	Results   []NodeResult
	StartTime time.Time
	EndTime   time.Time
}

func Execute(g *graph.Graph, cfg RunConfig, runner RunnerFunc) *RunResult {
	runResult := &RunResult{
		StartTime: time.Now(),
	}

	if len(g.Assets) == 0 {
		runResult.EndTime = time.Now()
		return runResult
	}

	// Determine which nodes to run
	nodesToRun := make(map[string]bool)
	if len(cfg.Assets) > 0 {
		subgraph := g.Subgraph(cfg.Assets)
		for _, n := range subgraph {
			nodesToRun[n] = true
		}
	} else {
		for name := range g.Assets {
			nodesToRun[name] = true
		}
	}

	// Full refresh: invalidate state for targeted incremental assets
	if cfg.FullRefresh && cfg.StateStore != nil {
		for name := range nodesToRun {
			asset := g.Assets[name]
			if asset.TimeColumn != "" {
				cfg.StateStore.InvalidateAll(name)
			}
		}
	}

	maxP := cfg.MaxParallel
	if maxP <= 0 {
		maxP = 10
	}

	var mu sync.Mutex
	results := make(map[string]*NodeResult)
	resolved := make(map[string]bool)

	// Track unresolved dependency counts
	unresolved := make(map[string]int)
	for name := range nodesToRun {
		count := 0
		for _, dep := range g.Assets[name].DependsOn {
			if nodesToRun[dep] {
				count++
			}
		}
		unresolved[name] = count
	}

	sem := make(chan struct{}, maxP)
	done := make(chan NodeResult, len(nodesToRun))
	pending := len(nodesToRun)

	// Multi-output dedup: track which source+interval combos have been executed
	var dedupMu sync.Mutex
	dedupResults := make(map[string]*NodeResult) // key: "source:intervalStart"

	dispatch := func(name string) {
		sem <- struct{}{}
		go func() {
			defer func() { <-sem }()
			asset := g.Assets[name]

			if asset.TimeColumn != "" && cfg.StateStore != nil {
				result := executeIncremental(asset, cfg, runner, &dedupMu, dedupResults)
				done <- result
			} else {
				// Full refresh or no state store — run once
				result := executeWithDedup(asset, cfg, runner, &dedupMu, dedupResults, "", "")
				done <- result
			}
		}()
	}

	// Find and dispatch root nodes
	for name := range nodesToRun {
		if unresolved[name] == 0 {
			resolved[name] = true
			dispatch(name)
		}
	}

	for pending > 0 {
		result := <-done
		mu.Lock()
		results[result.AssetName] = &result
		pending--

		if result.Status == "success" {
			for _, downstream := range g.Assets[result.AssetName].DependedOnBy {
				if !nodesToRun[downstream] || resolved[downstream] {
					continue
				}
				unresolved[downstream]--
				if unresolved[downstream] == 0 {
					allOK := true
					for _, dep := range g.Assets[downstream].DependsOn {
						if !nodesToRun[dep] {
							continue
						}
						if r, ok := results[dep]; !ok || r.Status != "success" {
							allOK = false
							break
						}
					}
					if allOK {
						resolved[downstream] = true
						mu.Unlock()
						dispatch(downstream)
						mu.Lock()
					}
				}
			}
		} else {
			descendants := g.Descendants(result.AssetName)
			for _, desc := range descendants {
				if !nodesToRun[desc] || resolved[desc] {
					continue
				}
				resolved[desc] = true
				results[desc] = &NodeResult{
					AssetName: desc,
					Status:    "skipped",
					Error:     "skipped: dependency " + result.AssetName + " failed",
					ExitCode:  -1,
				}
				pending--
			}
		}
		mu.Unlock()
	}

	runResult.EndTime = time.Now()

	for name := range nodesToRun {
		if r, ok := results[name]; ok {
			runResult.Results = append(runResult.Results, *r)
		}
	}

	return runResult
}

func executeIncremental(asset *graph.Asset, cfg RunConfig, runner RunnerFunc, dedupMu *sync.Mutex, dedupResults map[string]*NodeResult) NodeResult {
	startDate := asset.StartDate
	if cfg.FromDate != "" {
		startDate = cfg.FromDate
	}
	if startDate == "" {
		return NodeResult{
			AssetName: asset.Name,
			Status:    "failed",
			Error:     "incremental asset has no start_date",
			ExitCode:  -1,
		}
	}

	endDate := cfg.ToDate
	if endDate == "" {
		endDate = time.Now().UTC().Format("2006-01-02")
	}

	unit := asset.IntervalUnit
	if unit == "" {
		unit = "day"
	}

	allIntervals, err := state.GenerateIntervals(startDate, endDate, unit)
	if err != nil {
		return NodeResult{
			AssetName: asset.Name,
			Status:    "failed",
			Error:     err.Error(),
			ExitCode:  -1,
		}
	}

	if len(allIntervals) == 0 {
		return NodeResult{
			AssetName: asset.Name,
			Status:    "success",
			Metadata:  map[string]string{"intervals_processed": "0"},
		}
	}

	completed, _ := cfg.StateStore.GetIntervals(asset.Name)
	missing := state.ComputeMissing(allIntervals, completed, asset.Lookback)
	missing = state.ApplyBatchSize(missing, asset.BatchSize)

	if len(missing) == 0 {
		return NodeResult{
			AssetName: asset.Name,
			Status:    "success",
			Metadata:  map[string]string{"intervals_processed": "0", "intervals_up_to_date": "true"},
		}
	}

	// Execute intervals sequentially, stop on first failure
	var lastResult NodeResult
	processed := 0

	for _, iv := range missing {
		cfg.StateStore.MarkInProgress(asset.Name, iv.Start, iv.End, cfg.RunID)

		result := executeWithDedup(asset, cfg, runner, dedupMu, dedupResults, iv.Start, iv.End)

		if result.Status == "success" {
			cfg.StateStore.MarkComplete(asset.Name, iv.Start, iv.End)
			processed++
			lastResult = result
		} else {
			cfg.StateStore.MarkFailed(asset.Name, iv.Start, iv.End)
			result.Metadata = mergeMetadata(result.Metadata, map[string]string{
				"intervals_processed":  itoa(processed),
				"interval_failed_at":   iv.Start,
			})
			return result
		}
	}

	lastResult.Metadata = mergeMetadata(lastResult.Metadata, map[string]string{
		"intervals_processed": itoa(processed),
	})
	return lastResult
}

func executeWithDedup(asset *graph.Asset, cfg RunConfig, runner RunnerFunc, dedupMu *sync.Mutex, dedupResults map[string]*NodeResult, intervalStart, intervalEnd string) NodeResult {
	// For multi-output: check if another output already executed this source+interval
	if asset.SourceAsset != "" {
		dedupKey := asset.Source + ":" + intervalStart
		dedupMu.Lock()
		if existing, ok := dedupResults[dedupKey]; ok {
			dedupMu.Unlock()
			// Copy result with this asset's name
			r := *existing
			r.AssetName = asset.Name
			return r
		}
		dedupMu.Unlock()
	}

	// Set interval on asset for runner
	modified := *asset
	modified.IntervalStart = intervalStart
	modified.IntervalEnd = intervalEnd

	result := runner(&modified, cfg.ProjectRoot, cfg.RunID)

	// Store for dedup
	if asset.SourceAsset != "" {
		dedupKey := asset.Source + ":" + intervalStart
		dedupMu.Lock()
		dedupResults[dedupKey] = &result
		dedupMu.Unlock()
	}

	return result
}

func mergeMetadata(base, extra map[string]string) map[string]string {
	if base == nil {
		base = make(map[string]string)
	}
	for k, v := range extra {
		base[k] = v
	}
	return base
}

func itoa(i int) string {
	return strconv.Itoa(i)
}
