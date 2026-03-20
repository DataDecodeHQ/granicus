package runner

import (
	"fmt"

	"github.com/DataDecodeHQ/granicus/internal/config"
)

type RunnerRegistry struct {
	runners     map[string]Runner
	resources map[string]*config.ResourceConfig
}

// NewRunnerRegistry creates a RunnerRegistry with the given resources.
func NewRunnerRegistry(resources map[string]*config.ResourceConfig) *RunnerRegistry {
	return &RunnerRegistry{
		runners:     make(map[string]Runner),
		resources: resources,
	}
}

// Register adds a runner for the given asset type name.
func (r *RunnerRegistry) Register(typeName string, runner Runner) {
	r.runners[typeName] = runner
}

// Resource returns the resource config for the given name, or nil if not found.
func (r *RunnerRegistry) Resource(name string) *config.ResourceConfig {
	if r.resources == nil {
		return nil
	}
	return r.resources[name]
}

// Run dispatches the asset to the appropriate registered runner based on asset type.
func (r *RunnerRegistry) Run(asset *Asset, projectRoot string, runID string) NodeResult {
	runner, ok := r.runners[asset.Type]
	if !ok {
		return NodeResult{
			AssetName: asset.Name,
			Status:    "failed",
			Error:     fmt.Sprintf("no runner registered for type: %s", asset.Type),
			ExitCode:  -1,
		}
	}
	return runner.Run(asset, projectRoot, runID)
}
