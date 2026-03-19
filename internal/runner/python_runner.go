package runner

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/DataDecodeHQ/granicus/internal/config"
	"github.com/DataDecodeHQ/granicus/internal/events"
)

type PythonRunner struct {
	Timeout               time.Duration
	DestinationConnection *config.ConnectionConfig
	SourceConnection      *config.ConnectionConfig
	EventStore            *events.Store
	Pipeline              string
	RefFunc               func(string) (string, error)
}

// NewPythonRunner creates a PythonRunner with the given connections, event store, and pipeline name.
func NewPythonRunner(destConn, srcConn *config.ConnectionConfig, eventStore *events.Store, pipeline string) *PythonRunner {
	return &PythonRunner{
		Timeout:               DefaultTimeout,
		DestinationConnection: destConn,
		SourceConnection:      srcConn,
		EventStore:            eventStore,
		Pipeline:              pipeline,
	}
}

// Run executes a Python script as a subprocess with connection and ref environment variables.
func (r *PythonRunner) Run(asset *Asset, projectRoot string, runID string) NodeResult {
	start := time.Now()

	pythonBin := findPython(projectRoot)

	metadataFile, err := os.CreateTemp("", "granicus-metadata-*.json")
	if err != nil {
		return NodeResult{
			AssetName: asset.Name, Status: "failed", StartTime: start,
			EndTime: time.Now(), Duration: time.Since(start),
			Error: fmt.Sprintf("creating metadata file: %v", err), ExitCode: -1,
		}
	}
	metadataPath := metadataFile.Name()
	metadataFile.Close()
	defer os.Remove(metadataPath)

	env := []string{
		"GRANICUS_ASSET_NAME=" + asset.Name,
		"GRANICUS_RUN_ID=" + runID,
		"GRANICUS_PROJECT_ROOT=" + projectRoot,
		"GRANICUS_METADATA_PATH=" + metadataPath,
	}

	if asset.IntervalStart != "" {
		env = append(env, "GRANICUS_INTERVAL_START="+asset.IntervalStart)
		env = append(env, "GRANICUS_INTERVAL_END="+asset.IntervalEnd)
	}

	destConn := r.DestinationConnection
	if destConn == nil {
		destConn = asset.ResolvedDestConn
	}
	srcConn := r.SourceConnection
	if srcConn == nil {
		srcConn = asset.ResolvedSourceConn
	}
	if destConn != nil {
		connJSON, _ := json.Marshal(flattenConnection(destConn))
		env = append(env, "GRANICUS_DEST_CONNECTION="+string(connJSON))
	}
	if srcConn != nil {
		connJSON, _ := json.Marshal(flattenConnection(srcConn))
		env = append(env, "GRANICUS_SOURCE_CONNECTION="+string(connJSON))
	}

	if r.RefFunc != nil && len(asset.DependsOn) > 0 {
		refs := make(map[string]string, len(asset.DependsOn))
		for _, dep := range asset.DependsOn {
			resolved, err := r.RefFunc(dep)
			if err == nil {
				refs[dep] = strings.ReplaceAll(resolved, "`", "")
			}
		}
		if refsJSON, err := json.Marshal(refs); err == nil {
			env = append(env, "GRANICUS_REFS="+string(refsJSON))
		}
	}

	if destConn != nil && destConn.Properties["credentials"] != "" {
		slog.Info("credential_access", "event", "subprocess_credential_pass", "asset", asset.Name, "run_id", runID, "connection", destConn.Name, "credential_method", "file")
	}
	if srcConn != nil && srcConn.Properties["credentials"] != "" {
		slog.Info("credential_access", "event", "subprocess_credential_pass", "asset", asset.Name, "run_id", runID, "connection", srcConn.Name, "credential_method", "file")
	}

	validateEnv(env, asset.Name, runID)

	done := make(chan struct{})
	if r.EventStore != nil {
		go r.monitorProgress(metadataPath, asset.Name, runID, done)
	}

	sub := RunSubprocess(SubprocessConfig{
		Command: []string{pythonBin, asset.Source},
		Env:     env,
		WorkDir: projectRoot,
		Timeout: effectiveTimeout(asset.Timeout, r.Timeout),
	})
	close(done)
	end := time.Now()

	result := NodeResult{
		AssetName: asset.Name,
		StartTime: start,
		EndTime:   end,
		Duration:  sub.Duration,
		Stdout:    sub.Stdout,
		Stderr:    sub.Stderr,
		ExitCode:  sub.ExitCode,
	}

	if sub.Error != "" {
		result.Status = "failed"
		result.Error = sub.Error
	} else {
		result.Status = "success"
	}

	// Read metadata file if it exists and has content
	if data, err := os.ReadFile(metadataPath); err == nil && len(data) > 0 {
		var meta map[string]string
		if err := json.Unmarshal(data, &meta); err == nil {
			result.Metadata = meta
		}
	}

	return result
}

func (r *PythonRunner) monitorProgress(metadataPath, assetName, runID string, done <-chan struct{}) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			data, err := os.ReadFile(metadataPath)
			if err != nil || len(data) == 0 {
				continue
			}
			var meta map[string]string
			if json.Unmarshal(data, &meta) != nil {
				continue
			}
			details := make(map[string]any, len(meta))
			for k, v := range meta {
				details[k] = v
			}
			if err := r.EventStore.Emit(events.Event{
				RunID:     runID,
				Pipeline:  r.Pipeline,
				Asset:     assetName,
				EventType: "asset_progress",
				Summary:   meta["step"],
				Details:   details,
			}); err != nil {
				slog.Warn("failed to emit progress event", "asset", assetName, "error", err)
			}
		}
	}
}

func flattenConnection(conn *config.ConnectionConfig) map[string]string {
	flat := make(map[string]string, len(conn.Properties)+2)
	flat["name"] = conn.Name
	flat["type"] = conn.Type
	for k, v := range conn.Properties {
		flat[k] = v
	}
	return flat
}

// validateEnv checks the subprocess env slice against the runner contract.
// Logs warnings for issues but never fails the run (advisory only).
func validateEnv(env []string, assetName, runID string) {
	vars := make(map[string]string, len(env))
	for _, e := range env {
		if k, v, ok := strings.Cut(e, "="); ok {
			vars[k] = v
		}
	}

	required := []string{"GRANICUS_ASSET_NAME", "GRANICUS_RUN_ID", "GRANICUS_PROJECT_ROOT", "GRANICUS_METADATA_PATH"}
	for _, key := range required {
		if vars[key] == "" {
			slog.Warn("env_contract_violation", "issue", "missing_required", "var", key, "asset", assetName, "run_id", runID)
		}
	}

	for _, key := range []string{"GRANICUS_DEST_CONNECTION", "GRANICUS_SOURCE_CONNECTION"} {
		if val, ok := vars[key]; ok {
			var parsed map[string]string
			if err := json.Unmarshal([]byte(val), &parsed); err != nil {
				slog.Warn("env_contract_violation", "issue", "invalid_json", "var", key, "asset", assetName, "run_id", runID, "error", err)
			}
		}
	}
}

func findPython(projectRoot string) string {
	venvPython := filepath.Join(projectRoot, ".venv", "bin", "python3")
	if absPath, err := filepath.Abs(venvPython); err == nil {
		if _, err := os.Stat(absPath); err == nil {
			return absPath
		}
	}
	return "python3"
}
