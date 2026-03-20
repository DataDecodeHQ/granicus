package runner

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/DataDecodeHQ/granicus/internal/config"
)

type GCSIngestRunner struct {
	SourceConnection *config.ConnectionConfig
	DestConnection   *config.ConnectionConfig
	Timeout          time.Duration
}

// NewGCSIngestRunner creates a GCSIngestRunner with the given source and destination connections.
func NewGCSIngestRunner(srcConn, destConn *config.ConnectionConfig) *GCSIngestRunner {
	return &GCSIngestRunner{
		SourceConnection: srcConn,
		DestConnection:   destConn,
		Timeout:          DefaultTimeout,
	}
}

// Run executes a GCS ingest operation by delegating to a subprocess with GCS environment variables.
func (r *GCSIngestRunner) Run(asset *Asset, projectRoot string, runID string) NodeResult {
	start := time.Now()

	srcConn := r.SourceConnection
	if srcConn == nil {
		srcConn = asset.ResolvedSourceConn
	}
	destConn := r.DestConnection
	if destConn == nil {
		destConn = asset.ResolvedDestConn
	}

	if srcConn == nil {
		return NodeResult{
			AssetName: asset.Name, Status: "failed", StartTime: start,
			EndTime: time.Now(), Duration: time.Since(start),
			Error: "gcs_ingest: source_connection not resolved", ExitCode: -1,
		}
	}

	bucket := srcConn.Properties["bucket"]
	if bucket == "" {
		return NodeResult{
			AssetName: asset.Name, Status: "failed", StartTime: start,
			EndTime: time.Now(), Duration: time.Since(start),
			Error: "gcs_ingest: source connection missing 'bucket' property", ExitCode: -1,
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
		DestConn:     destConn,
	})

	env = append(env,
		"GRANICUS_GCS_BUCKET="+bucket,
		"GRANICUS_GCS_PREFIX="+srcConn.Properties["prefix"],
		"GRANICUS_INGEST_MODE=true",
	)

	if fp := srcConn.Properties["file_pattern"]; fp != "" {
		env = append(env, "GRANICUS_GCS_FILE_PATTERN="+fp)
	}
	if format := srcConn.Properties["format"]; format != "" {
		env = append(env, "GRANICUS_GCS_FORMAT="+format)
	}
	if lm := srcConn.Properties["load_method"]; lm != "" {
		env = append(env, "GRANICUS_GCS_LOAD_METHOD="+lm)
	}
	if ap := srcConn.Properties["archive_prefix"]; ap != "" {
		env = append(env, "GRANICUS_GCS_ARCHIVE_PREFIX="+ap)
	}
	if destConn != nil {
		env = append(env, "GRANICUS_DEST_PROJECT="+destConn.Properties["project"])
		env = append(env, "GRANICUS_DEST_DATASET="+destConn.Properties["dataset"])
	}
	hasCredentials := resolveGCSCredentials(srcConn) != ""
	if hasCredentials {
		env = append(env, "GOOGLE_APPLICATION_CREDENTIALS="+resolveGCSCredentials(srcConn))
		LogCredentialCrossing("gcs_subprocess", "gcs", asset.Name, runID)
	}
	LogSubprocessLaunch(asset.Name, "gcs_ingest", len(env), hasCredentials)

	// Contract: Go owns this boundary. Base env + runner-specific vars. Legacy env vars preserved for backward compat.
	sub := RunSubprocess(SubprocessConfig{
		Command: inferCommand(asset.Source, projectRoot),
		Env:     env,
		WorkDir: projectRoot,
		Timeout: effectiveTimeout(asset.Timeout, r.Timeout),
	})

	result := NodeResultFromSubprocess(asset.Name, start, sub)

	// Prefer metadata file; fall back to stdout GRANICUS_META: line parsing.
	if meta, err := readMetadata(metadataPath); err == nil && meta != nil {
		result.Metadata = meta
	} else {
		result.Metadata = parseMetaLines(sub.Stdout)
	}
	LogSubprocessComplete(asset.Name, "gcs_ingest", result.ExitCode, result.Duration, result.Metadata != nil)

	return result
}

func parseMetaLines(stdout string) map[string]string {
	meta := make(map[string]string)
	for _, line := range strings.Split(stdout, "\n") {
		if strings.HasPrefix(line, "GRANICUS_META:") {
			kv := strings.TrimPrefix(line, "GRANICUS_META:")
			if k, v, ok := strings.Cut(kv, "="); ok {
				meta[k] = v
			}
		}
	}
	if len(meta) == 0 {
		return nil
	}
	return meta
}
