package runner

import (
	"fmt"

	"github.com/analytehealth/granicus/internal/config"
)

type RunnerRegistry struct {
	runners     map[string]Runner
	connections map[string]*config.ConnectionConfig
}

func NewRunnerRegistry(connections map[string]*config.ConnectionConfig) *RunnerRegistry {
	r := &RunnerRegistry{
		runners:     make(map[string]Runner),
		connections: connections,
	}
	r.Register("shell", NewShellRunner())
	return r
}

func (r *RunnerRegistry) Register(typeName string, runner Runner) {
	r.runners[typeName] = runner
}

func (r *RunnerRegistry) Connection(name string) *config.ConnectionConfig {
	if r.connections == nil {
		return nil
	}
	return r.connections[name]
}

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
