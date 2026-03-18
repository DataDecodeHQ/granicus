package result

import "time"

// ResultEnvelope is the contract between runners and the control plane.
// Local runners populate it directly; remote runners serialize it as JSON.
type ResultEnvelope struct {
	Node       string            `json:"node"`
	RunID      string            `json:"run_id"`
	Pipeline   string            `json:"pipeline"`
	Status     string            `json:"status"` // success, failed, skipped
	StartedAt  time.Time         `json:"started_at"`
	EndedAt    time.Time         `json:"ended_at"`
	DurationMs int64             `json:"duration_ms"`
	Error      string            `json:"error,omitempty"`
	ExitCode   int               `json:"exit_code"`
	Telemetry  map[string]any    `json:"telemetry,omitempty"`
	Checks     []CheckResult     `json:"checks,omitempty"`
	Artifacts  []Artifact        `json:"artifacts,omitempty"`
	Metadata   map[string]string `json:"metadata,omitempty"`
}

// CheckResult captures the outcome of a single data quality check.
type CheckResult struct {
	Name         string `json:"name"`
	Status       string `json:"status"` // passed, failed, warning
	Severity     string `json:"severity,omitempty"`
	RowsReturned int64  `json:"rows_returned"`
	SQLHash      string `json:"sql_hash,omitempty"`
	Error        string `json:"error,omitempty"`
}

// Artifact references a side-effect produced by a runner (log, snapshot, etc).
type Artifact struct {
	Type string `json:"type"` // context_update, log, state_snapshot
	URI  string `json:"uri"`  // gs://..., file://..., pubsub://...
	Size int64  `json:"size_bytes,omitempty"`
}

// Telemetry key constants for BQ SQL runners.
const (
	TelBQBytesScanned = "bytes_scanned"
	TelBQBytesWritten = "bytes_written"
	TelBQRowCount     = "row_count"
	TelBQSlotMs       = "slot_ms"
	TelBQJobID        = "bq_job_id"
	TelBQCacheHit     = "cache_hit"
)

// Telemetry key constants for Cloud Run Job runners.
const (
	TelCRJPeakMemoryBytes = "peak_memory_bytes"
	TelCRJJobID           = "job_id"
)
