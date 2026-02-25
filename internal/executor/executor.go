package executor

import (
	"sync"
	"time"

	"github.com/analytehealth/granicus/internal/graph"
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

	maxP := cfg.MaxParallel
	if maxP <= 0 {
		maxP = 10
	}

	var mu sync.Mutex
	results := make(map[string]*NodeResult)
	resolved := make(map[string]bool) // dispatched or skipped

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

	// dispatch launches a goroutine for a node
	dispatch := func(name string) {
		sem <- struct{}{} // acquire semaphore (outside mutex!)
		go func() {
			defer func() { <-sem }()
			result := runner(g.Assets[name], cfg.ProjectRoot, cfg.RunID)
			done <- result
		}()
	}

	// Find and dispatch root nodes
	for name := range nodesToRun {
		if unresolved[name] == 0 {
			resolved[name] = true
			dispatch(name)
		}
	}

	// Process completions
	for pending > 0 {
		result := <-done
		mu.Lock()
		results[result.AssetName] = &result
		pending--

		if result.Status == "success" {
			// Check dependents
			for _, downstream := range g.Assets[result.AssetName].DependedOnBy {
				if !nodesToRun[downstream] || resolved[downstream] {
					continue
				}
				unresolved[downstream]--
				if unresolved[downstream] == 0 {
					// Verify all deps succeeded
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
			// Mark all descendants as skipped
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
