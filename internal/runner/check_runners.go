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

func NewPythonCheckRunner(destConn, srcConn *config.ConnectionConfig, eventStore *events.Store, pipeline string) *PythonCheckRunner {
	return &PythonCheckRunner{
		inner: NewPythonRunner(destConn, srcConn, eventStore, pipeline),
	}
}

func (r *PythonCheckRunner) SetRefFunc(f func(string) (string, error)) {
	r.inner.RefFunc = f
}

func (r *PythonCheckRunner) Run(asset *Asset, projectRoot string, runID string) NodeResult {
	return r.inner.Run(asset, projectRoot, runID)
}
