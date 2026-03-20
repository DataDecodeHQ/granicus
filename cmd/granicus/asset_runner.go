package main

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"time"

	"github.com/DataDecodeHQ/granicus/internal/config"
	"github.com/DataDecodeHQ/granicus/internal/events"
	"github.com/DataDecodeHQ/granicus/internal/executor"
	"github.com/DataDecodeHQ/granicus/internal/graph"
	"github.com/DataDecodeHQ/granicus/internal/runner"
	"github.com/DataDecodeHQ/granicus/internal/server"
	"github.com/DataDecodeHQ/granicus/internal/types"
)

// nodeRunnerOptions holds the caller-specific parameters for buildNodeRunner.
// Only the fields relevant to each caller need to be set.
type nodeRunnerOptions struct {
	// OutputJSON suppresses console output when true (main.go path).
	OutputJSON bool
	// Dispatch handles remote execution when non-nil (serve.go path).
	Dispatch runner.RunnerDispatch
	// DispatchCtx is the context passed to Dispatch.Execute (serve.go path).
	DispatchCtx context.Context
	// ConfigDir overrides project root for source-file resolution when non-empty (serve.go path).
	ConfigDir string
}

// buildNodeRunner returns the executor asset-runner closure shared by runRun (main.go)
// and executePipeline (serve.go). Callers populate opts with the fields they need.
func buildNodeRunner(
	cfg *config.PipelineConfig,
	runID string,
	eventStore *events.Store,
	registry *runner.RunnerRegistry,
	opts nodeRunnerOptions,
) func(asset *types.Asset, pr string, rid string) types.AssetResult {
	return func(asset *types.Asset, pr string, rid string) types.AssetResult {
		if !opts.OutputJSON && opts.Dispatch == nil {
			ts := time.Now().Format("15:04:05")
			fmt.Printf("[%s] %s %-24s started\n", ts, whiteBullet, asset.Name)
		}

		logEmit(eventStore, events.Event{
			RunID: runID, Pipeline: cfg.Pipeline, Asset: asset.Name,
			EventType: "asset_started", Severity: "info",
			Summary: fmt.Sprintf("Asset %s started", asset.Name),
		})

		// Model version tracking
		if asset.Source != "" {
			sourceBase := pr
			if opts.ConfigDir != "" {
				sourceBase = opts.ConfigDir
			}
			srcPath := filepath.Join(sourceBase, asset.Source)
			if hash, herr := events.HashFile(srcPath); herr == nil {
				if _, _, mvErr := eventStore.RecordModelVersion(asset.Name, srcPath, hash, runID); mvErr != nil {
					slog.Warn("failed to record model version", "asset", asset.Name, "error", mvErr)
				}
			}
		}

		resolvedDataset, resolvedDestConn, resolvedSourceConn := resolveAssetRuntime(cfg, asset.Name)
		ac := config.AssetConfig{Source: asset.Source}
		if ac2 := findAssetConfig(cfg, asset.Name); ac2 != nil {
			ac = *ac2
		}
		ra := *asset
		ra.Prefix = cfg.Prefix
		ra.Dataset = resolvedDataset
		ra.ResolvedDestConn = resolvedDestConn
		ra.ResolvedSourceConn = resolvedSourceConn

		runRoot := pr
		if opts.ConfigDir != "" {
			runRoot = opts.ConfigDir
		}
		var r types.AssetResult
		if opts.Dispatch != nil && opts.Dispatch.Supports(ra.Type) {
			var derr error
			r, derr = opts.Dispatch.Execute(opts.DispatchCtx, &ra, runRoot, rid)
			if derr != nil {
				r = types.AssetResult{
					AssetName: ra.Name,
					Status:    "failed",
					StartTime: time.Now(),
					EndTime:   time.Now(),
					Error:     fmt.Sprintf("dispatch error: %v", derr),
					ExitCode:  -1,
					Metadata:  map[string]string{"runner": "cloud_run_job"},
				}
			}
		} else {
			r = registry.Run(&ra, runRoot, rid)
		}
		emitNodeResult(eventStore, runID, cfg, ac, r)

		if !opts.OutputJSON && opts.Dispatch == nil {
			ts := time.Now().Format("15:04:05")
			switch r.Status {
			case "success":
				fmt.Printf("[%s] %s %-24s success (%.1fs)\n", ts, greenCheck, r.AssetName, r.Duration.Seconds())
			case "failed":
				fmt.Printf("[%s] %s %-24s failed (%.1fs) -- %s\n", ts, redCross, r.AssetName, r.Duration.Seconds(), r.Error)
				if r.Stderr != "" {
					for _, line := range strings.Split(strings.TrimSpace(r.Stderr), "\n") {
						fmt.Printf("         %s\n", line)
					}
				}
			}
		}

		return r
	}
}

// sendRunAlerts sends a run summary notification if alerts are configured.
func sendRunAlerts(cfg *config.PipelineConfig, runID string, eventStore *events.Store, rr *executor.RunResult, totalDuration time.Duration) {
	if cfg.Alerts == nil {
		return
	}
	alertMgr := server.NewAlertManager(cfg.Alerts, eventStore)
	var summaryResults []struct {
		AssetName string
		Status    string
		Error     string
		Duration  float64
		Cost      float64
	}
	for _, r := range rr.Results {
		cost := 0.0
		if r.Metadata != nil {
			if c, ok := r.Metadata["bq_total_bytes_billed"]; ok {
				if v, err := fmt.Sscanf(c, "%f", &cost); v == 1 && err == nil {
					cost = cost / 1e12 * 5.0
				}
			}
		}
		summaryResults = append(summaryResults, struct {
			AssetName string
			Status    string
			Error     string
			Duration  float64
			Cost      float64
		}{r.AssetName, r.Status, r.Error, r.Duration.Seconds(), cost})
	}
	alertData := server.BuildRunSummary(cfg.Pipeline, runID, summaryResults, totalDuration.Seconds())
	alertMgr.SendRunSummary(alertData)
}

// runPostRunHooks executes post-run hooks (context.db, monitor.db, DuckDB assembly).
func runPostRunHooks(cfg *config.PipelineConfig, g *graph.Graph, projectRoot string, rr *executor.RunResult) {
	bqClient := newBQClientForContext(cfg)
	if bqClient != nil {
		defer bqClient.Close()
	}
	hooks := []executor.PostRunHook{
		executor.WriteContextHook(bqClient),
		monitorHook(bqClient),
		executor.DuckDBAssemblyHook(),
	}
	hookFailures := executor.RunPostHooks(hooks, g, cfg, projectRoot, rr)
	if hookFailures > 0 {
		slog.Warn("post-run hooks failed", "failures", hookFailures)
	}
}

// findAssetConfig returns the AssetConfig for the given asset name, or nil if not found.
func findAssetConfig(cfg *config.PipelineConfig, name string) *config.AssetConfig {
	for i := range cfg.Assets {
		if cfg.Assets[i].Name == name {
			return &cfg.Assets[i]
		}
	}
	return nil
}

// resourceForAsset returns the destination resource config for an asset, or nil.
func resourceForAsset(cfg *config.PipelineConfig, asset *config.AssetConfig) *config.ResourceConfig {
	resName := asset.DestinationResource
	if resName == "" {
		return nil
	}
	if res, ok := cfg.Resources[resName]; ok {
		return res
	}
	return nil
}

// resolveAssetRuntime resolves the dataset, destination resource, and source resource
// for the named asset using layer routing and resource lookups from cfg.
func resolveAssetRuntime(cfg *config.PipelineConfig, assetName string) (dataset string, destConn, sourceConn *config.ResourceConfig) {
	assetCfg := findAssetConfig(cfg, assetName)
	if assetCfg == nil {
		return "", nil, nil
	}
	defaultDS := ""
	if res := resourceForAsset(cfg, assetCfg); res != nil {
		defaultDS = res.Properties["dataset"]
	}
	dataset = cfg.DatasetForAsset(*assetCfg, defaultDS)
	if assetCfg.DestinationResource != "" {
		if res, ok := cfg.Resources[assetCfg.DestinationResource]; ok {
			destConn = res
		}
	}
	if assetCfg.SourceResource != "" {
		if res, ok := cfg.Resources[assetCfg.SourceResource]; ok {
			sourceConn = res
		}
	}
	return dataset, destConn, sourceConn
}

// emitNodeResult emits an asset_succeeded or asset_failed event to eventStore.
// Stdout and stderr are truncated to 10 KiB before inclusion in failure details.
func emitNodeResult(eventStore *events.Store, runID string, cfg *config.PipelineConfig, asset config.AssetConfig, r types.AssetResult) {
	if r.Status == "success" {
		logEmit(eventStore, events.Event{
			RunID: runID, Pipeline: cfg.Pipeline, Asset: r.AssetName,
			EventType: "asset_succeeded", Severity: "info",
			DurationMs: r.Duration.Milliseconds(),
			Summary:    fmt.Sprintf("Asset %s succeeded (%.1fs)", r.AssetName, r.Duration.Seconds()),
			Details: map[string]any{
				"exit_code":    r.ExitCode,
				"metadata":     r.Metadata,
				"stdout_lines": events.CountLines(r.Stdout),
				"stderr_lines": events.CountLines(r.Stderr),
			},
		})
		return
	}
	stdout := r.Stdout
	if len(stdout) > 10*1024 {
		stdout = stdout[:10*1024] + "[truncated]"
	}
	stderr := r.Stderr
	if len(stderr) > 10*1024 {
		stderr = stderr[:10*1024] + "[truncated]"
	}
	logEmit(eventStore, events.Event{
		RunID: runID, Pipeline: cfg.Pipeline, Asset: r.AssetName,
		EventType: "asset_failed", Severity: "error",
		DurationMs: r.Duration.Milliseconds(),
		Summary:    fmt.Sprintf("Asset %s failed: %s", r.AssetName, r.Error),
		Details: map[string]any{
			"error_message": r.Error,
			"exit_code":     r.ExitCode,
			"source_file":   asset.Source,
			"metadata":      r.Metadata,
			"stdout":        stdout,
			"stderr":        stderr,
		},
	})
}

