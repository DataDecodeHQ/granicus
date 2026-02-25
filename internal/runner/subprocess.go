package runner

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"time"
)

type SubprocessResult struct {
	Stdout   string
	Stderr   string
	ExitCode int
	Error    string
	Duration time.Duration
}

type SubprocessConfig struct {
	Command []string
	Env     []string
	WorkDir string
	Timeout time.Duration
}

func RunSubprocess(cfg SubprocessConfig) SubprocessResult {
	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = DefaultTimeout
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, cfg.Command[0], cfg.Command[1:]...)
	cmd.Dir = cfg.WorkDir
	cmd.Env = append(os.Environ(), cfg.Env...)
	cmd.WaitDelay = 1 * time.Second

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &limitedWriter{buf: &stdout, limit: MaxCaptureBytes}
	cmd.Stderr = &limitedWriter{buf: &stderr, limit: MaxCaptureBytes}

	start := time.Now()
	err := cmd.Run()
	duration := time.Since(start)

	result := SubprocessResult{
		Stdout:   truncateOutput(stdout.String()),
		Stderr:   truncateOutput(stderr.String()),
		Duration: duration,
	}

	if err != nil {
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
	}

	return result
}
