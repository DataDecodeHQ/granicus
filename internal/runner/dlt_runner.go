package runner

import (
	"github.com/DataDecodeHQ/granicus/internal/config"
	"github.com/DataDecodeHQ/granicus/internal/events"
)

type DLTRunner struct {
	inner *PythonRunner
}

// NewDLTRunner creates a DLTRunner that delegates to a PythonRunner for dlt pipeline execution.
func NewDLTRunner(destConn, srcConn *config.ConnectionConfig, eventStore *events.Store, pipeline string) *DLTRunner {
	return &DLTRunner{
		inner: NewPythonRunner(destConn, srcConn, eventStore, pipeline),
	}
}

// SetRefFunc sets the asset reference resolution function on the inner runner.
func (r *DLTRunner) SetRefFunc(f func(string) (string, error)) {
	r.inner.RefFunc = f
}

// Run executes the dlt pipeline script and returns the result with any dlt metadata.
func (r *DLTRunner) Run(asset *Asset, projectRoot string, runID string) NodeResult {
	result := r.inner.Run(asset, projectRoot, runID)

	// Extract dlt-specific metadata fields if present
	if result.Metadata != nil {
		// dlt scripts write rows_loaded, tables_created, load_duration via GRANICUS_METADATA_PATH
		// These are already captured by PythonRunner's metadata file reading.
		// No additional extraction needed — dlt scripts write the same JSON format.
	}

	return result
}
