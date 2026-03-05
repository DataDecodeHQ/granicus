package runner

import (
	"github.com/analytehealth/granicus/internal/config"
	"github.com/analytehealth/granicus/internal/events"
)

type DLTRunner struct {
	inner *PythonRunner
}

func NewDLTRunner(destConn, srcConn *config.ConnectionConfig, eventStore *events.Store, pipeline string) *DLTRunner {
	return &DLTRunner{
		inner: NewPythonRunner(destConn, srcConn, eventStore, pipeline),
	}
}

func (r *DLTRunner) SetRefFunc(f func(string) (string, error)) {
	r.inner.RefFunc = f
}

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
