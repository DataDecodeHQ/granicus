package runner

import (
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

	env := []string{
		"GRANICUS_GCS_BUCKET=" + bucket,
		"GRANICUS_GCS_PREFIX=" + srcConn.Properties["prefix"],
		"GRANICUS_ASSET_NAME=" + asset.Name,
		"GRANICUS_RUN_ID=" + runID,
		"GRANICUS_PROJECT_ROOT=" + projectRoot,
		"GRANICUS_INGEST_MODE=true",
	}

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
	if creds := resolveGCSCredentials(srcConn); creds != "" {
		env = append(env, "GOOGLE_APPLICATION_CREDENTIALS="+creds)
	}

	if destConn != nil {
		env = append(env, "GRANICUS_DEST_PROJECT="+destConn.Properties["project"])
		env = append(env, "GRANICUS_DEST_DATASET="+destConn.Properties["dataset"])
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

	// Parse metadata from stdout lines like "GRANICUS_META:key=value"
	result.Metadata = parseMetaLines(sub.Stdout)

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

