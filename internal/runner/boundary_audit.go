package runner

import (
	"log/slog"
	"time"
)

const boundaryAuditGroup = "boundary_audit"

// LogCredentialCrossing logs when credentials cross a boundary.
func LogCredentialCrossing(boundary, credentialType, asset, runID string) {
	slog.Info(boundaryAuditGroup+": credential_crossing",
		"boundary", boundary,
		"credential_type", credentialType,
		"asset", asset,
		"run_id", runID,
	)
}

// LogSubprocessLaunch logs when a subprocess is about to be launched.
func LogSubprocessLaunch(asset, runnerType string, envVarCount int, hasCredentials bool) {
	slog.Info(boundaryAuditGroup+": subprocess_launch",
		"asset", asset,
		"runner_type", runnerType,
		"env_var_count", envVarCount,
		"has_credentials", hasCredentials,
	)
}

// LogSubprocessComplete logs when a subprocess has finished.
func LogSubprocessComplete(asset, runnerType string, exitCode int, duration time.Duration, hasMetadata bool) {
	slog.Info(boundaryAuditGroup+": subprocess_complete",
		"asset", asset,
		"runner_type", runnerType,
		"exit_code", exitCode,
		"duration_ms", duration.Milliseconds(),
		"has_metadata", hasMetadata,
	)
}

// LogSQLExecution logs before a SQL query is executed.
func LogSQLExecution(asset, dataset string, dryRunPassed bool, estimatedBytes int64) {
	slog.Info(boundaryAuditGroup+": sql_execution",
		"asset", asset,
		"dataset", dataset,
		"dry_run_passed", dryRunPassed,
		"estimated_bytes", estimatedBytes,
	)
}

// LogCloudRunDispatch logs when a Cloud Run job is dispatched.
func LogCloudRunDispatch(asset, image, version, runID string) {
	slog.Info(boundaryAuditGroup+": cloud_run_dispatch",
		"asset", asset,
		"image", image,
		"version", version,
		"run_id", runID,
	)
}
