package runner

import (
	"bytes"
	"fmt"
	"os"
	"time"
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

type ShellRunner struct {
	Timeout time.Duration
}

func NewShellRunner() *ShellRunner {
	return &ShellRunner{Timeout: DefaultTimeout}
}

func (r *ShellRunner) Run(asset *Asset, projectRoot string, runID string) NodeResult {
	if asset.Type != "shell" {
		return NodeResult{
			AssetName: asset.Name,
			Status:    "failed",
			Error:     fmt.Sprintf("runner not implemented for type: %s", asset.Type),
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

	env := []string{
		"GRANICUS_ASSET_NAME=" + asset.Name,
		"GRANICUS_RUN_ID=" + runID,
		"GRANICUS_PROJECT_ROOT=" + projectRoot,
	}

	start := time.Now()
	sub := RunSubprocess(SubprocessConfig{
		Command: []string{"bash", asset.Source},
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
