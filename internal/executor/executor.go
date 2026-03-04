package executor

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/analytehealth/granicus/internal/graph"
	"github.com/analytehealth/granicus/internal/pool"
	"github.com/analytehealth/granicus/internal/state"
)

const DefaultShutdownTimeout = 5 * time.Minute

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
	MaxParallel  int
	Assets       []string
	ProjectRoot  string
	RunID        string
	FromDate     string
	ToDate       string
	FullRefresh  bool
	StateStore   *state.Store
	TestMode     bool
	TestStart    string
	TestEnd      string
	KeepTestData    bool
	TestDataset     string // set by test mode setup (the created dataset name)
	DownstreamOnly  bool
	PoolManager     *pool.PoolManager
	AssetPools      map[string]string // asset name -> pool name
	Ctx             context.Context   // cancelled on SIGTERM/SIGINT for graceful shutdown
	ShutdownTimeout time.Duration     // max wait for in-progress nodes; 0 = DefaultShutdownTimeout
}

type RunnerFunc func(asset *graph.Asset, projectRoot string, runID string) NodeResult

type RunResult struct {
	Results     []NodeResult
	StartTime   time.Time
	EndTime     time.Time
	Interrupted bool // true if run was stopped by a shutdown signal
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
		var subgraph []string
		if cfg.DownstreamOnly {
			subgraph = g.DownstreamSubgraph(cfg.Assets)
		} else {
			subgraph = g.Subgraph(cfg.Assets)
		}
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
				if err := cfg.StateStore.InvalidateAll(name); err != nil {
					log.Printf("executor: state store InvalidateAll failed for %s: %v", name, err)
				}
			}
		}
	}

	maxP := cfg.MaxParallel
	if maxP <= 0 {
		maxP = 10
	}

	shutdownTimeout := cfg.ShutdownTimeout
	if shutdownTimeout <= 0 {
		shutdownTimeout = DefaultShutdownTimeout
	}

	// shutdownCh is closed when a shutdown signal is received.
	shutdownCh := make(chan struct{})
	var shutdownOnce sync.Once
	triggerShutdown := func() {
		shutdownOnce.Do(func() { close(shutdownCh) })
	}
	defer triggerShutdown() // clean up watcher goroutine on return

	if cfg.Ctx != nil {
		go func() {
			select {
			case <-cfg.Ctx.Done():
				triggerShutdown()
			case <-shutdownCh:
			}
		}()
	}

	isShuttingDown := func() bool {
		select {
		case <-shutdownCh:
			return true
		default:
			return false
		}
	}

	// Pre-compute blocking checks per asset: asset -> list of blocking check node names
	blockingChecks := make(map[string][]string)
	for name := range nodesToRun {
		asset := g.Assets[name]
		if asset.Blocking && strings.HasPrefix(name, "check:") {
			for _, parent := range asset.DependsOn {
				blockingChecks[parent] = append(blockingChecks[parent], name)
			}
		}
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

			// Source phantom nodes represent external data — succeed immediately (no-op)
			// so that downstream check nodes are not skipped.
			if asset.Type == graph.AssetTypeSource {
				now := time.Now()
				done <- NodeResult{
					AssetName: name,
					Status:    "success",
					StartTime: now,
					EndTime:   now,
				}
				return
			}

			// Acquire pool slot if configured
			if cfg.PoolManager != nil && cfg.AssetPools != nil {
				if poolName, ok := cfg.AssetPools[name]; ok && poolName != "" {
					if err := cfg.PoolManager.Acquire(context.Background(), poolName); err != nil {
						log.Printf("pool acquire failed for %s: %v", name, err)
						done <- NodeResult{
							AssetName: name,
							Status:    "failed",
							StartTime: time.Now(),
							EndTime:   time.Now(),
							Error:     fmt.Sprintf("pool slot acquisition failed: %v", err),
							ExitCode:  -1,
						}
						return
					}
					defer cfg.PoolManager.Release(poolName)
				}
			}

			if asset.TimeColumn != "" && cfg.StateStore != nil {
				result := executeIncremental(asset, cfg, runner, &dedupMu, dedupResults)
				done <- result
			} else {
				// Full refresh or no state store — run once, with retry
				result := runWithRetry(asset, cfg, runner, &dedupMu, dedupResults, "", "")
				done <- result
			}
		}()
	}

	// dispatchOrSkip dispatches a node, or immediately emits a skipped result
	// if a shutdown signal has been received.
	dispatchOrSkip := func(name string) {
		if isShuttingDown() {
			done <- NodeResult{
				AssetName: name,
				Status:    "skipped",
				Error:     "skipped: run interrupted",
				ExitCode:  -1,
			}
			return
		}
		dispatch(name)
	}

	// allBlockingChecksPassed returns true if every blocking check for the given
	// asset has completed successfully. Must be called with mu held.
	allBlockingChecksPassed := func(assetName string) bool {
		checks, has := blockingChecks[assetName]
		if !has {
			return true
		}
		for _, chk := range checks {
			r, ok := results[chk]
			if !ok || r.Status != "success" {
				return false
			}
		}
		return true
	}

	// tryDispatchDownstream attempts to dispatch a downstream node if all its
	// dependencies have succeeded AND all blocking checks on each dependency
	// have passed. Must be called with mu held; temporarily releases mu to
	// dispatch. Returns true if mu was temporarily released.
	tryDispatchDownstream := func(downstream string) bool {
		if !nodesToRun[downstream] || resolved[downstream] {
			return false
		}
		if unresolved[downstream] != 0 {
			return false
		}
		// All graph deps resolved — verify each dep succeeded
		for _, dep := range g.Assets[downstream].DependsOn {
			if !nodesToRun[dep] {
				continue
			}
			r, ok := results[dep]
			if !ok || r.Status != "success" {
				return false
			}
		}
		// Gate on blocking checks: for each dependency that has blocking
		// checks, all must have passed before we dispatch this downstream.
		// (Check nodes themselves are not gated — they run as soon as their
		// parent asset succeeds.)
		if !strings.HasPrefix(downstream, "check:") {
			for _, dep := range g.Assets[downstream].DependsOn {
				if !nodesToRun[dep] {
					continue
				}
				if !allBlockingChecksPassed(dep) {
					return false
				}
			}
		}
		resolved[downstream] = true
		mu.Unlock()
		dispatchOrSkip(downstream)
		mu.Lock()
		return true
	}

	// Find and dispatch root nodes
	for name := range nodesToRun {
		if unresolved[name] == 0 {
			resolved[name] = true
			dispatchOrSkip(name)
		}
	}

	// activeShutdownCh starts as shutdownCh; set to nil once we've started the
	// shutdown timer so the select doesn't spin on the closed channel.
	activeShutdownCh := shutdownCh
	var shutdownTimer <-chan time.Time // nil = never fires

	for pending > 0 {
		select {
		case result := <-done:
			mu.Lock()
			results[result.AssetName] = &result
			pending--

			if result.Status == "success" {
				// Decrement unresolved counts and try dispatching downstream nodes
				for _, downstream := range g.Assets[result.AssetName].DependedOnBy {
					if !nodesToRun[downstream] || resolved[downstream] {
						continue
					}
					unresolved[downstream]--
					tryDispatchDownstream(downstream)
				}

				// If this is a successful blocking check, its parent's other
				// downstream nodes may now be unblocked. Re-evaluate them.
				asset := g.Assets[result.AssetName]
				if asset.Blocking && strings.HasPrefix(result.AssetName, "check:") {
					for _, parentName := range asset.DependsOn {
						if !allBlockingChecksPassed(parentName) {
							continue
						}
						for _, downstream := range g.Assets[parentName].DependedOnBy {
							tryDispatchDownstream(downstream)
						}
					}
				}
			} else {
				// Skip all direct descendants of the failed/skipped node
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

				// Blocking check failure: skip descendants of the parent asset
				asset := g.Assets[result.AssetName]
				if asset.Blocking && strings.HasPrefix(result.AssetName, "check:") {
					for _, parentName := range asset.DependsOn {
						parentDescendants := g.Descendants(parentName)
						for _, desc := range parentDescendants {
							if desc == result.AssetName {
								continue
							}
							if !nodesToRun[desc] || resolved[desc] {
								continue
							}
							resolved[desc] = true
							results[desc] = &NodeResult{
								AssetName: desc,
								Status:    "skipped",
								Error:     "skipped: blocked_by_check:" + result.AssetName,
								ExitCode:  -1,
								Metadata:  map[string]string{"blocked_by_check": result.AssetName},
							}
							pending--
						}
					}
				}
			}
			mu.Unlock()

		case <-activeShutdownCh:
			// Shutdown signal received: start timeout and stop listening on
			// this channel (closed channels would spin the select).
			log.Printf("executor: shutdown signal received, waiting up to %v for in-progress nodes", shutdownTimeout)
			shutdownTimer = time.After(shutdownTimeout)
			activeShutdownCh = nil

		case <-shutdownTimer:
			// Timeout expired: force-resolve all nodes not yet in results.
			mu.Lock()
			for name := range nodesToRun {
				if _, ok := results[name]; !ok {
					results[name] = &NodeResult{
						AssetName: name,
						Status:    "skipped",
						Error:     "skipped: shutdown timeout exceeded",
						ExitCode:  -1,
					}
					pending--
				}
			}
			mu.Unlock()
			shutdownTimer = nil // prevent re-firing
		}
	}

	runResult.EndTime = time.Now()
	runResult.Interrupted = isShuttingDown()

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

	completed, err := cfg.StateStore.GetIntervals(asset.Name)
	if err != nil {
		log.Printf("executor: state store GetIntervals failed for %s: %v", asset.Name, err)
		return NodeResult{
			AssetName: asset.Name,
			Status:    "failed",
			Error:     fmt.Sprintf("state store GetIntervals: %v", err),
			ExitCode:  -1,
		}
	}
	missing := state.ComputeMissing(allIntervals, completed, asset.Lookback)
	missing = state.ApplyBatchSize(missing, asset.BatchSize)

	// In test mode, limit intervals to reduce BQ usage
	if cfg.TestMode && len(missing) > 0 {
		maxIntervals := asset.Lookback + 1
		if maxIntervals < 3 {
			maxIntervals = 3
		}
		if len(missing) > maxIntervals {
			missing = missing[len(missing)-maxIntervals:]
		}
	}

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

	maxAttempts := asset.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	backoffBase := asset.BackoffBase
	if backoffBase <= 0 {
		backoffBase = 10 * time.Second
	}
	maxRetries := maxAttempts - 1

	for _, iv := range missing {
		if err := cfg.StateStore.MarkInProgress(asset.Name, iv.Start, iv.End, cfg.RunID); err != nil {
			log.Printf("executor: state store MarkInProgress failed for %s interval %s: %v", asset.Name, iv.Start, err)
		}

		var result NodeResult
		for attempt := 0; attempt <= maxRetries; attempt++ {
			result = executeWithDedup(asset, cfg, runner, dedupMu, dedupResults, iv.Start, iv.End)
			if result.Status == "success" || !isRetryableForPolicy(result.Error, asset.RetryableErrors) || attempt == maxRetries {
				break
			}
			backoff := time.Duration(1<<uint(attempt)) * backoffBase
			log.Printf("executor: retrying %s interval %s after %v (attempt %d/%d): %s",
				asset.Name, iv.Start, backoff, attempt+1, maxAttempts, result.Error)
			time.Sleep(backoff)
		}

		if result.Status == "success" {
			if err := cfg.StateStore.MarkComplete(asset.Name, iv.Start, iv.End); err != nil {
				log.Printf("executor: state store MarkComplete failed for %s interval %s: %v", asset.Name, iv.Start, err)
			}
			processed++
			lastResult = result
		} else {
			if err := cfg.StateStore.MarkFailed(asset.Name, iv.Start, iv.End); err != nil {
				log.Printf("executor: state store MarkFailed failed for %s interval %s: %v", asset.Name, iv.Start, err)
			}
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
	modified.TestStart = cfg.TestStart
	modified.TestEnd = cfg.TestEnd

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

// runWithRetry executes an asset once (non-incremental path) with per-asset retry policy.
func runWithRetry(asset *graph.Asset, cfg RunConfig, runner RunnerFunc, dedupMu *sync.Mutex, dedupResults map[string]*NodeResult, intervalStart, intervalEnd string) NodeResult {
	maxAttempts := asset.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	backoffBase := asset.BackoffBase
	if backoffBase <= 0 {
		backoffBase = 10 * time.Second
	}
	maxRetries := maxAttempts - 1

	var result NodeResult
	for attempt := 0; attempt <= maxRetries; attempt++ {
		result = executeWithDedup(asset, cfg, runner, dedupMu, dedupResults, intervalStart, intervalEnd)
		if result.Status == "success" || !isRetryableForPolicy(result.Error, asset.RetryableErrors) || attempt == maxRetries {
			break
		}
		backoff := time.Duration(1<<uint(attempt)) * backoffBase
		log.Printf("executor: retrying %s after %v (attempt %d/%d): %s",
			asset.Name, backoff, attempt+1, maxAttempts, result.Error)
		time.Sleep(backoff)
	}
	return result
}
