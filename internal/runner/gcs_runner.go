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

// NewGCSRunner creates a GCSRunner for the given GCS connection.
func NewGCSRunner(conn *config.ConnectionConfig) *GCSRunner {
	return &GCSRunner{Connection: conn, Timeout: DefaultTimeout}
}

// Run executes a GCS operation by delegating to a subprocess with bucket and prefix environment variables.
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

	// GCS operations delegate to shell/python scripts that use gsutil or the GCS client
	// The runner sets up environment variables and delegates
	env := buildSubprocessEnv(SubprocessEnvConfig{
		Asset:        asset,
		ProjectRoot:  projectRoot,
		RunID:        runID,
		MetadataPath: metadataPath,
		DestConn:     r.Connection,
	})

	// Legacy runner-specific vars kept for backward compatibility
	prefix := r.Connection.Properties["prefix"]
	env = append(env,
		"GRANICUS_GCS_BUCKET="+bucket,
		"GRANICUS_GCS_PREFIX="+prefix,
	)
	if format := r.Connection.Properties["format"]; format != "" {
		env = append(env, "GRANICUS_GCS_FORMAT="+format)
	}
	if pp := r.Connection.Properties["partition_prefix"]; pp != "" {
		env = append(env, "GRANICUS_GCS_PARTITION_PREFIX="+pp)
	}
	if creds := resolveGCSCredentials(r.Connection); creds != "" {
		env = append(env, "GOOGLE_APPLICATION_CREDENTIALS="+creds)
	}

	// Contract: Go owns this boundary. Base env + runner-specific vars. Legacy env vars preserved for backward compat.
	sub := RunSubprocess(SubprocessConfig{
		Command: inferCommand(asset.Source, projectRoot),
		Env:     env,
		WorkDir: projectRoot,
		Timeout: effectiveTimeout(asset.Timeout, r.Timeout),
	})

	result := NodeResultFromSubprocess(asset.Name, start, sub)

	if meta, err := readMetadata(metadataPath); err == nil && meta != nil {
		result.Metadata = meta
	}

	return result
}

type S3Runner struct {
	Connection *config.ConnectionConfig
	Timeout    time.Duration
}

// NewS3Runner creates an S3Runner for the given S3 connection.
func NewS3Runner(conn *config.ConnectionConfig) *S3Runner {
	return &S3Runner{Connection: conn, Timeout: DefaultTimeout}
}

// Run executes an S3 operation by delegating to a subprocess with S3 environment variables.
func (r *S3Runner) Run(asset *Asset, projectRoot string, runID string) NodeResult {
	start := time.Now()

	bucket := r.Connection.Properties["bucket"]
	if bucket == "" {
		return NodeResult{
			AssetName: asset.Name, Status: "failed", StartTime: start,
			EndTime: time.Now(), Duration: time.Since(start),
			Error: "S3 connection missing 'bucket' property", ExitCode: -1,
		}
	}

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

	env := buildSubprocessEnv(SubprocessEnvConfig{
		Asset:        asset,
		ProjectRoot:  projectRoot,
		RunID:        runID,
		MetadataPath: metadataPath,
		DestConn:     r.Connection,
	})

	// Legacy runner-specific vars kept for backward compatibility
	endpoint := r.Connection.Properties["endpoint"]
	env = append(env,
		"GRANICUS_S3_BUCKET="+bucket,
		"GRANICUS_S3_ENDPOINT="+endpoint,
		"GRANICUS_S3_PREFIX="+r.Connection.Properties["prefix"],
	)
	if key := r.Connection.Properties["access_key"]; key != "" {
		env = append(env, "AWS_ACCESS_KEY_ID="+key)
	}
	if secret := r.Connection.Properties["secret_key"]; secret != "" {
		env = append(env, "AWS_SECRET_ACCESS_KEY="+secret)
	}

	sub := RunSubprocess(SubprocessConfig{
		Command: inferCommand(asset.Source, projectRoot),
		Env:     env,
		WorkDir: projectRoot,
		Timeout: effectiveTimeout(asset.Timeout, r.Timeout),
	})

	result := NodeResultFromSubprocess(asset.Name, start, sub)

	if meta, err := readMetadata(metadataPath); err == nil && meta != nil {
		result.Metadata = meta
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

// NewIcebergRunner creates an IcebergRunner for the given connection (not yet implemented).
func NewIcebergRunner(conn *config.ConnectionConfig) *IcebergRunner {
	return &IcebergRunner{Connection: conn, Timeout: DefaultTimeout}
}

// Run always returns a failure result because the Iceberg connector is not yet implemented.
func (r *IcebergRunner) Run(asset *Asset, projectRoot string, runID string) NodeResult {
	return NodeResult{
		AssetName: asset.Name,
		Status:    "failed",
		Error:     fmt.Sprintf("iceberg connector not yet implemented (catalog=%s)", r.Connection.Properties["catalog"]),
		ExitCode:  -1,
	}
}
