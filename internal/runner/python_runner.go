package runner

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/analytehealth/granicus/internal/config"
)

type PythonRunner struct {
	Timeout              time.Duration
	DestinationConnection *config.ConnectionConfig
	SourceConnection      *config.ConnectionConfig
}

func NewPythonRunner(destConn, srcConn *config.ConnectionConfig) *PythonRunner {
	return &PythonRunner{
		Timeout:              DefaultTimeout,
		DestinationConnection: destConn,
		SourceConnection:      srcConn,
	}
}

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

	if r.DestinationConnection != nil {
		connJSON, _ := json.Marshal(r.DestinationConnection)
		env = append(env, "GRANICUS_DEST_CONNECTION="+string(connJSON))
	}
	if r.SourceConnection != nil {
		connJSON, _ := json.Marshal(r.SourceConnection)
		env = append(env, "GRANICUS_SOURCE_CONNECTION="+string(connJSON))
	}

	sub := RunSubprocess(SubprocessConfig{
		Command: []string{pythonBin, asset.Source},
		Env:     env,
		WorkDir: projectRoot,
		Timeout: r.Timeout,
	})
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

func findPython(projectRoot string) string {
	venvPython := filepath.Join(projectRoot, ".venv", "bin", "python3")
	if _, err := os.Stat(venvPython); err == nil {
		return venvPython
	}
	return "python3"
}
