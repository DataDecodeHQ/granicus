package runner

import (
	"bytes"
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
