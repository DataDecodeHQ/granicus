package runner

import (
	"github.com/analytehealth/granicus/internal/config"
)

// PythonCheckRunner runs a Python script as a check.
// exit 0 = pass (success), non-zero = fail.
type PythonCheckRunner struct {
	inner *PythonRunner
}

func NewPythonCheckRunner(destConn, srcConn *config.ConnectionConfig) *PythonCheckRunner {
	return &PythonCheckRunner{
		inner: NewPythonRunner(destConn, srcConn),
	}
}

func (r *PythonCheckRunner) Run(asset *Asset, projectRoot string, runID string) NodeResult {
	return r.inner.Run(asset, projectRoot, runID)
}
