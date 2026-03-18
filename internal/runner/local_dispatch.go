package runner

import "context"

// LocalDispatch wraps the existing RunnerRegistry to implement RunnerDispatch.
// This is the default dispatcher — all runners execute in-process.
type LocalDispatch struct {
	registry *RunnerRegistry
}

// NewLocalDispatch creates a LocalDispatch backed by the given registry.
func NewLocalDispatch(registry *RunnerRegistry) *LocalDispatch {
	return &LocalDispatch{registry: registry}
}

// Execute runs the asset using the registry's runner for the asset type.
// The context is currently unused for local execution but reserved for
// future cancellation support.
func (d *LocalDispatch) Execute(_ context.Context, asset *Asset, projectRoot string, runID string) (NodeResult, error) {
	result := d.registry.Run(asset, projectRoot, runID)
	return result, nil
}

// Supports reports whether this dispatcher handles the given asset type.
func (d *LocalDispatch) Supports(assetType string) bool {
	_, ok := d.registry.runners[assetType]
	return ok
}

// Verify LocalDispatch implements RunnerDispatch at compile time.
var _ RunnerDispatch = (*LocalDispatch)(nil)
