package runner

import (
	"encoding/json"
	"errors"
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
	DestinationResource *config.ResourceConfig
	SourceResource      *config.ResourceConfig
	EventStore            *events.Store
	Pipeline              string
	RefFunc               func(string) (string, error)
}

// NewPythonRunner creates a PythonRunner with the given connections, event store, and pipeline name.
func NewPythonRunner(destConn, srcConn *config.ResourceConfig, eventStore *events.Store, pipeline string) *PythonRunner {
	return &PythonRunner{
		Timeout:               DefaultTimeout,
		DestinationResource: destConn,
		SourceResource:      srcConn,
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

	destConn := r.DestinationResource
	if destConn == nil {
		destConn = asset.ResolvedDestConn
	}
	srcConn := r.SourceResource
	if srcConn == nil {
		srcConn = asset.ResolvedSourceConn
	}

	var refs map[string]string
	if r.RefFunc != nil && len(asset.DependsOn) > 0 {
		refs = make(map[string]string, len(asset.DependsOn))
		for _, dep := range asset.DependsOn {
			resolved, err := r.RefFunc(dep)
			if err == nil {
				refs[dep] = strings.ReplaceAll(resolved, "`", "")
			}
		}
	}

	env := buildSubprocessEnv(SubprocessEnvConfig{
		Asset:        asset,
		ProjectRoot:  projectRoot,
		RunID:        runID,
		MetadataPath: metadataPath,
		DestConn:     destConn,
		SrcConn:      srcConn,
		Refs:         refs,
	})

	hasCredentials := (destConn != nil && destConn.Credentials != "") ||
		(srcConn != nil && srcConn.Credentials != "")

	if destConn != nil && destConn.Credentials != "" {
		slog.Info("credential_access", "event", "subprocess_credential_pass", "asset", asset.Name, "run_id", runID, "connection", destConn.Name, "credential_method", "file")
		LogCredentialCrossing("python_subprocess", destConn.Type, asset.Name, runID)
	}
	if srcConn != nil && srcConn.Credentials != "" {
		slog.Info("credential_access", "event", "subprocess_credential_pass", "asset", asset.Name, "run_id", runID, "connection", srcConn.Name, "credential_method", "file")
		LogCredentialCrossing("python_subprocess", srcConn.Type, asset.Name, runID)
	}
	LogSubprocessLaunch(asset.Name, "python", len(env), hasCredentials)

	if err := validateEnv(env, asset.Name, runID); err != nil {
		return NodeResult{
			AssetName: asset.Name, Status: "failed", StartTime: start,
			EndTime: time.Now(), Duration: time.Since(start),
			Error: fmt.Sprintf("env validation: %v", err), ExitCode: -1,
		}
	}

	done := make(chan struct{})
	if r.EventStore != nil {
		go r.monitorProgress(metadataPath, asset.Name, runID, done)
	}

	// Contract: Go owns this boundary. Schema: contracts/env_contract.json
	sub := RunSubprocess(SubprocessConfig{
		Command: []string{pythonBin, asset.Source},
		Env:     env,
		WorkDir: projectRoot,
		Timeout: effectiveTimeout(asset.Timeout, r.Timeout),
	})
	close(done)

	result := NodeResultFromSubprocess(asset.Name, start, sub)

	// Read metadata file if it exists and has content
	if data, err := os.ReadFile(metadataPath); err == nil && len(data) > 0 {
		var meta map[string]string
		if err := json.Unmarshal(data, &meta); err == nil {
			result.Metadata = meta
		}
	}
	LogSubprocessComplete(asset.Name, "python", result.ExitCode, result.Duration, result.Metadata != nil)

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

// validateEnv checks the subprocess env slice against the runner contract.
// Returns a combined error for any violations. Fail-fast: any contract
// violation prevents subprocess launch.
func validateEnv(env []string, assetName, runID string) error {
	vars := make(map[string]string, len(env))
	for _, e := range env {
		if k, v, ok := strings.Cut(e, "="); ok {
			vars[k] = v
		}
	}

	var errs []error

	required := []string{"GRANICUS_ASSET_NAME", "GRANICUS_RUN_ID", "GRANICUS_PROJECT_ROOT", "GRANICUS_METADATA_PATH"}
	for _, key := range required {
		if vars[key] == "" {
			errs = append(errs, fmt.Errorf("missing required env var %s", key))
		}
	}

	// Validate interval format: must be ISO 8601 datetime (YYYY-MM-DDTHH:MM:SSZ)
	for _, key := range []string{"GRANICUS_INTERVAL_START", "GRANICUS_INTERVAL_END"} {
		if val, ok := vars[key]; ok && val != "" {
			if _, err := time.Parse("2006-01-02T15:04:05Z", val); err != nil {
				errs = append(errs, fmt.Errorf("%s must be ISO 8601 datetime (YYYY-MM-DDTHH:MM:SSZ), got %q", key, val))
			}
		}
	}

	// Validate resource JSON and required fields (name, type)
	for _, key := range []string{"GRANICUS_DEST_RESOURCE", "GRANICUS_SOURCE_RESOURCE"} {
		if val, ok := vars[key]; ok {
			var parsed map[string]string
			if err := json.Unmarshal([]byte(val), &parsed); err != nil {
				errs = append(errs, fmt.Errorf("invalid JSON in %s: %w", key, err))
				continue
			}
			if parsed["name"] == "" {
				errs = append(errs, fmt.Errorf("%s missing required field 'name'", key))
			}
			if parsed["type"] == "" {
				errs = append(errs, fmt.Errorf("%s missing required field 'type'", key))
			}
		}
	}

	return errors.Join(errs...)
}

// findSDKPath locates the granicus SDK python directory.
// It checks relative to the running executable, then falls back to GRANICUS_SDK_PATH.
func findSDKPath() string {
	if envPath := os.Getenv("GRANICUS_SDK_PATH"); envPath != "" {
		return envPath
	}
	exe, err := os.Executable()
	if err != nil {
		return ""
	}
	// Walk up from the executable looking for sdk/python
	dir := filepath.Dir(exe)
	for i := 0; i < 5; i++ {
		candidate := filepath.Join(dir, "sdk", "python")
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
		dir = filepath.Dir(dir)
	}
	return ""
}

// appendPythonPath prepends sdkPath to any existing PYTHONPATH entry in env,
// or appends a new PYTHONPATH entry if none exists.
func appendPythonPath(env []string, sdkPath string) []string {
	for i, e := range env {
		if strings.HasPrefix(e, "PYTHONPATH=") {
			existing := strings.TrimPrefix(e, "PYTHONPATH=")
			env[i] = "PYTHONPATH=" + sdkPath + ":" + existing
			return env
		}
	}
	// No existing PYTHONPATH in env slice; also check the process environment.
	if existing := os.Getenv("PYTHONPATH"); existing != "" {
		return append(env, "PYTHONPATH="+sdkPath+":"+existing)
	}
	return append(env, "PYTHONPATH="+sdkPath)
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
