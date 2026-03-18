package runner

import "context"

// RunnerDispatch abstracts how asset execution is dispatched. LocalDispatch
// runs everything in-process (current behavior). CloudRunJobDispatch sends
// work to Cloud Run Jobs (Phase 4).
type RunnerDispatch interface {
	// Execute runs the given asset and returns its result. The context carries
	// cancellation from graceful shutdown.
	Execute(ctx context.Context, asset *Asset, projectRoot string, runID string) (NodeResult, error)

	// Supports reports whether this dispatcher handles the given asset type.
	Supports(assetType string) bool
}
