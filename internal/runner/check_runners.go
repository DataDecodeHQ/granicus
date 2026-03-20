package runner

import (
	"github.com/DataDecodeHQ/granicus/internal/config"
	"github.com/DataDecodeHQ/granicus/internal/events"
)

// PythonCheckRunner runs a Python script as a check.
// exit 0 = pass (success), non-zero = fail.
type PythonCheckRunner struct {
	inner *PythonRunner
}

// NewPythonCheckRunner creates a PythonCheckRunner that delegates to a PythonRunner.
func NewPythonCheckRunner(destConn, srcConn *config.ResourceConfig, eventStore *events.Store, pipeline string) *PythonCheckRunner {
	return &PythonCheckRunner{
		inner: NewPythonRunner(destConn, srcConn, eventStore, pipeline),
	}
}

// SetRefFunc sets the asset reference resolution function on the inner runner.
func (r *PythonCheckRunner) SetRefFunc(f func(string) (string, error)) {
	r.inner.RefFunc = f
}

// Run executes the Python check script and returns the result.
func (r *PythonCheckRunner) Run(asset *Asset, projectRoot string, runID string) NodeResult {
	return r.inner.Run(asset, projectRoot, runID)
}
