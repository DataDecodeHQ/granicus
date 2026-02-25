package runner

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"time"
)

const (
	DefaultTimeout   = 5 * time.Minute
	MaxCaptureBytes  = 1024 * 1024 // 1MB hard cap
	ResultTruncBytes = 10 * 1024   // 10KB in result
	TruncMarker      = "\n[truncated]"
)

type Asset struct {
	Name   string
	Type   string
	Source string
}

type NodeResult struct {
	AssetName string        `json:"asset"`
	Status    string        `json:"status"`
	StartTime time.Time     `json:"start_time"`
	EndTime   time.Time     `json:"end_time"`
	Duration  time.Duration `json:"duration_ms"`
	Error     string        `json:"error"`
	Stdout    string        `json:"stdout"`
	Stderr    string        `json:"stderr"`
	ExitCode  int           `json:"exit_code"`
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

	sourcePath := asset.Source
	if err := makeExecutable(sourcePath, projectRoot); err != nil {
		return NodeResult{
			AssetName: asset.Name,
			Status:    "failed",
			Error:     fmt.Sprintf("chmod: %v", err),
			ExitCode:  -1,
		}
	}

	timeout := r.Timeout
	if timeout == 0 {
		timeout = DefaultTimeout
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "bash", sourcePath)
	cmd.Dir = projectRoot
	cmd.Env = append(os.Environ(),
		"GRANICUS_ASSET_NAME="+asset.Name,
		"GRANICUS_RUN_ID="+runID,
		"GRANICUS_PROJECT_ROOT="+projectRoot,
	)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &limitedWriter{buf: &stdout, limit: MaxCaptureBytes}
	cmd.Stderr = &limitedWriter{buf: &stderr, limit: MaxCaptureBytes}

	start := time.Now()
	err := cmd.Run()
	end := time.Now()

	result := NodeResult{
		AssetName: asset.Name,
		StartTime: start,
		EndTime:   end,
		Duration:  end.Sub(start),
		Stdout:    truncateOutput(stdout.String()),
		Stderr:    truncateOutput(stderr.String()),
	}

	if err != nil {
		result.Status = "failed"
		if ctx.Err() == context.DeadlineExceeded {
			result.Error = "timeout"
		} else {
			result.Error = err.Error()
		}
		if exitErr, ok := err.(*exec.ExitError); ok {
			result.ExitCode = exitErr.ExitCode()
		} else {
			result.ExitCode = -1
		}
	} else {
		result.Status = "success"
		result.ExitCode = 0
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
		return len(p), nil // discard but report success
	}
	if len(p) > remaining {
		p = p[:remaining]
	}
	return w.buf.Write(p)
}
