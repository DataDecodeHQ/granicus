package runner

import (
	"context"
	"fmt"
)

// DispatchRegistry routes assets to the appropriate RunnerDispatch based on
// the asset's Runner field from pipeline.yaml.
type DispatchRegistry struct {
	dispatchers map[string]RunnerDispatch
}

// NewDispatchRegistry creates a registry with at least a local dispatcher.
func NewDispatchRegistry(local *LocalDispatch) *DispatchRegistry {
	return &DispatchRegistry{
		dispatchers: map[string]RunnerDispatch{
			"local": local,
		},
	}
}

// RegisterDispatcher adds a named dispatcher (e.g., "cloud_run_job").
func (r *DispatchRegistry) RegisterDispatcher(name string, d RunnerDispatch) {
	r.dispatchers[name] = d
}

// Dispatch routes an asset to the appropriate dispatcher and executes it.
func (r *DispatchRegistry) Dispatch(ctx context.Context, asset *Asset, projectRoot, runID, runnerName string) (NodeResult, error) {
	if runnerName == "" {
		runnerName = "local"
	}

	d, ok := r.dispatchers[runnerName]
	if !ok {
		return NodeResult{
			AssetName: asset.Name,
			Status:    "failed",
			Error:     fmt.Sprintf("no dispatcher registered for runner: %s", runnerName),
			ExitCode:  -1,
		}, nil
	}

	return d.Execute(ctx, asset, projectRoot, runID)
}
