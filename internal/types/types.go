package types

import (
	"time"

	"github.com/DataDecodeHQ/granicus/internal/config"
)

// Asset is the canonical runtime representation of a pipeline asset.
// It is a superset of graph-level and runner-level fields.
type Asset struct {
	// Identity
	Name string
	Type string

	// Source
	Source    string
	InlineSQL string

	// Graph relationships
	DependsOn    []string
	DependedOnBy []string
	SourceAsset  string // back-reference for multi-output assets

	// Resources
	DestinationResource string
	SourceResource      string
	ResolvedDestConn      *config.ResourceConfig
	ResolvedSourceConn    *config.ResourceConfig

	// Intervals
	TimeColumn   string
	IntervalUnit string
	Lookback     int
	StartDate    string
	BatchSize    int
	IntervalStart string // set at runtime
	IntervalEnd   string // set at runtime

	// Testing
	TestStart string
	TestEnd   string

	// Metadata
	Layer         string
	Grain         string
	Prefix        string
	Dataset       string
	DefaultChecks *bool
	Blocking      bool
	Severity      string // info, warning, error, critical

	// Execution
	Timeout         time.Duration
	MaxAttempts     int
	BackoffBase     time.Duration
	RetryableErrors []string
}

// AssetResult is the canonical execution result for a single asset run.
type AssetResult struct {
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
