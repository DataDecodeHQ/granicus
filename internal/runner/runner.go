package runner

import (
	"bytes"
	"fmt"
	"os"
	"time"

	"github.com/DataDecodeHQ/granicus/internal/config"
)

const (
	DefaultTimeout   = 5 * time.Minute
	MaxCaptureBytes  = 1024 * 1024 // 1MB hard cap
	ResultTruncBytes = 10 * 1024   // 10KB in result
	TruncMarker      = "\n[truncated]"
)

type Asset struct {
	Name                  string
	Type                  string
	Source                string
	DestinationConnection string
	SourceConnection      string
	IntervalStart         string
	IntervalEnd           string
	Prefix                string
	InlineSQL             string
	TestStart             string
	TestEnd               string
	Dataset               string
	Layer                 string
	DependsOn             []string
	Timeout               time.Duration
	ResolvedDestConn      *config.ConnectionConfig
	ResolvedSourceConn    *config.ConnectionConfig
}

type NodeResult struct {
	AssetName string            `json:"asset"`
	Status    string            `json:"status"`
	StartTime time.Time         `json:"start_time"`
	EndTime   time.Time         `json:"end_time"`
	Duration  time.Duration     `json:"duration_ms"`
	Error     string            `json:"error"`
	Stdout    string            `json:"stdout"`
	Stderr    string            `json:"stderr"`
	ExitCode  int               `json:"exit_code"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

type Runner interface {
	Run(asset *Asset, projectRoot string, runID string) NodeResult
}

func NodeResultFromSubprocess(assetName string, start time.Time, sub SubprocessResult) NodeResult {
	end := time.Now()
	result := NodeResult{
		AssetName: assetName,
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

	return result
}

func effectiveTimeout(assetTimeout, runnerTimeout time.Duration) time.Duration {
	if assetTimeout > 0 {
		return assetTimeout
	}
	if runnerTimeout > 0 {
		return runnerTimeout
	}
	return DefaultTimeout
}

type ShellRunner struct {
	Timeout time.Duration
}

// NewShellRunner creates a ShellRunner with the default timeout.
func NewShellRunner() *ShellRunner {
	return &ShellRunner{Timeout: DefaultTimeout}
}

// Run executes a shell script asset as a bash subprocess.
func (r *ShellRunner) Run(asset *Asset, projectRoot string, runID string) NodeResult {
	if asset.Type != "shell" {
		return NodeResult{
			AssetName: asset.Name,
			Status:    "failed",
			Error:     fmt.Sprintf("runner not implemented for type: %s", asset.Type),
			ExitCode:  -1,
		}
	}

	scriptPath := asset.Source
	if projectRoot != "" {
		scriptPath = projectRoot + "/" + asset.Source
	}
	info, statErr := os.Stat(scriptPath)
	if statErr != nil {
		return NodeResult{
			AssetName: asset.Name,
			Status:    "failed",
			Error:     fmt.Sprintf("script not found: %s", scriptPath),
			ExitCode:  -1,
		}
	}
	if info.Mode()&0111 == 0 {
		return NodeResult{
			AssetName: asset.Name,
			Status:    "failed",
			Error:     fmt.Sprintf("script not executable: %s", scriptPath),
			ExitCode:  -1,
		}
	}

	if err := makeExecutable(asset.Source, projectRoot); err != nil {
		return NodeResult{
			AssetName: asset.Name,
			Status:    "failed",
			Error:     fmt.Sprintf("chmod: %v", err),
			ExitCode:  -1,
		}
	}

	metadataFile, err := os.CreateTemp("", "granicus-metadata-*.json")
	if err != nil {
		return NodeResult{
			AssetName: asset.Name,
			Status:    "failed",
			Error:     fmt.Sprintf("creating metadata file: %v", err),
			ExitCode:  -1,
		}
	}
	metadataPath := metadataFile.Name()
	metadataFile.Close()
	defer os.Remove(metadataPath)

	env := buildSubprocessEnv(SubprocessEnvConfig{
		Asset:        asset,
		ProjectRoot:  projectRoot,
		RunID:        runID,
		MetadataPath: metadataPath,
	})

	// Contract: Go owns this boundary. Base env vars per contracts/env_contract.json
	LogSubprocessLaunch(asset.Name, "shell", len(env), false)
	start := time.Now()
	sub := RunSubprocess(SubprocessConfig{
		Command: []string{"bash", asset.Source},
		Env:     env,
		WorkDir: projectRoot,
		Timeout: effectiveTimeout(asset.Timeout, r.Timeout),
	})

	result := NodeResultFromSubprocess(asset.Name, start, sub)

	if meta, err := readMetadata(metadataPath); err == nil && meta != nil {
		result.Metadata = meta
	}
	LogSubprocessComplete(asset.Name, "shell", result.ExitCode, result.Duration, result.Metadata != nil)

	return result
}

func makeExecutable(source, projectRoot string) error {
	path := source
	if projectRoot != "" {
		path = projectRoot + "/" + source
	}
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if info.Mode()&0111 == 0 {
		return os.Chmod(path, info.Mode()|0755)
	}
	return nil
}

func truncateOutput(s string) string {
	if len(s) <= ResultTruncBytes {
		return s
	}
	return s[:ResultTruncBytes] + TruncMarker
}

type limitedWriter struct {
	buf   *bytes.Buffer
	limit int
}

// Write appends bytes up to the configured limit, silently discarding overflow.
func (w *limitedWriter) Write(p []byte) (int, error) {
	remaining := w.limit - w.buf.Len()
	if remaining <= 0 {
		return len(p), nil
	}
	if len(p) > remaining {
		p = p[:remaining]
	}
	return w.buf.Write(p)
}
