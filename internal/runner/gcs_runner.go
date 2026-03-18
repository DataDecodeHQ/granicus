package runner

import (
	"fmt"
	"os"
	"time"

	"github.com/DataDecodeHQ/granicus/internal/config"
)

// resolveGCSCredentials returns the credentials file path using the following
// priority: (1) explicit credentials property, (2) GCS_SERVICE_ACCOUNT env var,
// (3) empty string (ADC fallback).
func resolveGCSCredentials(conn *config.ConnectionConfig) string {
	if creds := conn.Properties["credentials"]; creds != "" {
		return creds
	}
	if envCreds := os.Getenv("GCS_SERVICE_ACCOUNT"); envCreds != "" {
		return envCreds
	}
	return ""
}

type GCSRunner struct {
	Connection *config.ConnectionConfig
	Timeout    time.Duration
}

func NewGCSRunner(conn *config.ConnectionConfig) *GCSRunner {
	return &GCSRunner{Connection: conn, Timeout: DefaultTimeout}
}

func (r *GCSRunner) Run(asset *Asset, projectRoot string, runID string) NodeResult {
	start := time.Now()

	bucket := r.Connection.Properties["bucket"]
	if bucket == "" {
		return NodeResult{
			AssetName: asset.Name, Status: "failed", StartTime: start,
			EndTime: time.Now(), Duration: time.Since(start),
			Error: "GCS connection missing 'bucket' property", ExitCode: -1,
		}
	}

	// GCS operations delegate to shell/python scripts that use gsutil or the GCS client
	// The runner sets up environment variables and delegates
	prefix := r.Connection.Properties["prefix"]
	env := []string{
		"GRANICUS_GCS_BUCKET=" + bucket,
		"GRANICUS_GCS_PREFIX=" + prefix,
		"GRANICUS_ASSET_NAME=" + asset.Name,
		"GRANICUS_RUN_ID=" + runID,
		"GRANICUS_PROJECT_ROOT=" + projectRoot,
	}
	if format := r.Connection.Properties["format"]; format != "" {
		env = append(env, "GRANICUS_GCS_FORMAT="+format)
	}
	if pp := r.Connection.Properties["partition_prefix"]; pp != "" {
		env = append(env, "GRANICUS_GCS_PARTITION_PREFIX="+pp)
	}
	if creds := resolveGCSCredentials(r.Connection); creds != "" {
		env = append(env, "GOOGLE_APPLICATION_CREDENTIALS="+creds)
	}
	if asset.IntervalStart != "" {
		env = append(env, "GRANICUS_INTERVAL_START="+asset.IntervalStart)
		env = append(env, "GRANICUS_INTERVAL_END="+asset.IntervalEnd)
	}

	sub := RunSubprocess(SubprocessConfig{
		Command: inferCommand(asset.Source, projectRoot),
		Env:     env,
		WorkDir: projectRoot,
		Timeout: effectiveTimeout(asset.Timeout, r.Timeout),
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

type S3Runner struct {
	Connection *config.ConnectionConfig
	Timeout    time.Duration
}

func NewS3Runner(conn *config.ConnectionConfig) *S3Runner {
	return &S3Runner{Connection: conn, Timeout: DefaultTimeout}
}

func (r *S3Runner) Run(asset *Asset, projectRoot string, runID string) NodeResult {
	start := time.Now()

	bucket := r.Connection.Properties["bucket"]
	endpoint := r.Connection.Properties["endpoint"]
	if bucket == "" {
		return NodeResult{
			AssetName: asset.Name, Status: "failed", StartTime: start,
			EndTime: time.Now(), Duration: time.Since(start),
			Error: "S3 connection missing 'bucket' property", ExitCode: -1,
		}
	}

	env := []string{
		"GRANICUS_S3_BUCKET=" + bucket,
		"GRANICUS_S3_ENDPOINT=" + endpoint,
		"GRANICUS_S3_PREFIX=" + r.Connection.Properties["prefix"],
		"GRANICUS_ASSET_NAME=" + asset.Name,
		"GRANICUS_RUN_ID=" + runID,
		"GRANICUS_PROJECT_ROOT=" + projectRoot,
	}
	if key := r.Connection.Properties["access_key"]; key != "" {
		env = append(env, "AWS_ACCESS_KEY_ID="+key)
	}
	if secret := r.Connection.Properties["secret_key"]; secret != "" {
		env = append(env, "AWS_SECRET_ACCESS_KEY="+secret)
	}
	if asset.IntervalStart != "" {
		env = append(env, "GRANICUS_INTERVAL_START="+asset.IntervalStart)
		env = append(env, "GRANICUS_INTERVAL_END="+asset.IntervalEnd)
	}

	sub := RunSubprocess(SubprocessConfig{
		Command: inferCommand(asset.Source, projectRoot),
		Env:     env,
		WorkDir: projectRoot,
		Timeout: effectiveTimeout(asset.Timeout, r.Timeout),
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

func inferCommand(source, projectRoot string) []string {
	ext := ""
	if len(source) > 3 {
		ext = source[len(source)-3:]
	}
	switch ext {
	case ".py":
		return []string{"python3", source}
	case ".sh":
		return []string{"bash", source}
	default:
		return []string{"bash", source}
	}
}

// Placeholder for Iceberg - would require BigLake/Spark integration
type IcebergRunner struct {
	Connection *config.ConnectionConfig
	Timeout    time.Duration
}

func NewIcebergRunner(conn *config.ConnectionConfig) *IcebergRunner {
	return &IcebergRunner{Connection: conn, Timeout: DefaultTimeout}
}

func (r *IcebergRunner) Run(asset *Asset, projectRoot string, runID string) NodeResult {
	return NodeResult{
		AssetName: asset.Name,
		Status:    "failed",
		Error:     fmt.Sprintf("iceberg connector not yet implemented (catalog=%s)", r.Connection.Properties["catalog"]),
		ExitCode:  -1,
	}
}
