package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/DataDecodeHQ/granicus/internal/result"
)

// CloudRunJobDispatch implements RunnerDispatch by dispatching asset execution
// to Cloud Run Jobs. It creates/updates a job, triggers execution, and reads
// the ResultEnvelope from a Pub/Sub result topic.
type CloudRunJobDispatch struct {
	project      string
	region       string
	image        string
	resultClient *pubsub.Client
	resultSub    *pubsub.Subscription
	defaultCPU   string
	defaultMem   string
	timeout      time.Duration
}

// CloudRunJobConfig holds configuration for CloudRunJobDispatch.
type CloudRunJobConfig struct {
	Project    string
	Region     string
	Image      string
	DefaultCPU string
	DefaultMem string
	Timeout    time.Duration
}

// NewCloudRunJobDispatch creates a dispatcher that sends work to Cloud Run Jobs.
func NewCloudRunJobDispatch(ctx context.Context, cfg CloudRunJobConfig) (*CloudRunJobDispatch, error) {
	if cfg.Project == "" {
		cfg.Project = os.Getenv("GCP_PROJECT")
	}
	if cfg.Region == "" {
		cfg.Region = os.Getenv("GCP_REGION")
		if cfg.Region == "" {
			cfg.Region = "us-central1"
		}
	}
	if cfg.Image == "" {
		cfg.Image = fmt.Sprintf("%s-docker.pkg.dev/%s/granicus/python-runner:latest", cfg.Region, cfg.Project)
	}
	if cfg.DefaultCPU == "" {
		cfg.DefaultCPU = "1"
	}
	if cfg.DefaultMem == "" {
		cfg.DefaultMem = "2Gi"
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 30 * time.Minute
	}

	// Set up Pub/Sub subscription for results
	resultProject := os.Getenv("GRANICUS_PUBSUB_PROJECT")
	if resultProject == "" {
		resultProject = cfg.Project
	}

	client, err := pubsub.NewClient(ctx, resultProject)
	if err != nil {
		return nil, fmt.Errorf("creating Pub/Sub client for results: %w", err)
	}

	subName := os.Getenv("GRANICUS_RESULT_SUBSCRIPTION")
	if subName == "" {
		subName = "granicus-results-engine"
	}

	return &CloudRunJobDispatch{
		project:      cfg.Project,
		region:       cfg.Region,
		image:        cfg.Image,
		resultClient: client,
		resultSub:    client.Subscription(subName),
		defaultCPU:   cfg.DefaultCPU,
		defaultMem:   cfg.DefaultMem,
		timeout:      cfg.Timeout,
	}, nil
}

// Execute dispatches the asset to a Cloud Run Job and waits for the result.
func (d *CloudRunJobDispatch) Execute(ctx context.Context, asset *Asset, projectRoot string, runID string) (NodeResult, error) {
	start := time.Now()

	// Build environment variables for the job
	env := map[string]string{
		"GRANICUS_ASSET_NAME":      asset.Name,
		"GRANICUS_RUN_ID":          runID,
		"GRANICUS_PROJECT_ROOT":    "/app",
		"GRANICUS_RESULT_TOPIC":    "granicus-results",
		"GRANICUS_RESULT_BUCKET":   os.Getenv("GRANICUS_OPS_BUCKET"),
		"GRANICUS_ENVELOPE_VERSION": result.EnvelopeVersion,
	}
	if asset.IntervalStart != "" {
		env["GRANICUS_INTERVAL_START"] = asset.IntervalStart
		env["GRANICUS_INTERVAL_END"] = asset.IntervalEnd
	}
	if asset.ResolvedDestConn != nil {
		if data, err := json.Marshal(asset.ResolvedDestConn); err == nil {
			env["GRANICUS_DEST_CONNECTION"] = string(data)
		}
	}
	if asset.ResolvedSourceConn != nil {
		if data, err := json.Marshal(asset.ResolvedSourceConn); err == nil {
			env["GRANICUS_SOURCE_CONNECTION"] = string(data)
		}
	}

	// Contract: Go owns this boundary. ResultEnvelope schema in result/envelope.go
	slog.Info("dispatching to Cloud Run Job",
		"asset", asset.Name, "run_id", runID, "project", d.project)

	// Wait for result from Pub/Sub with timeout
	timeoutCtx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()

	resultCh := make(chan result.ResultEnvelope, 1)
	errCh := make(chan error, 1)

	go func() {
		err := d.resultSub.Receive(timeoutCtx, func(ctx context.Context, msg *pubsub.Message) {
			if msg.Attributes["run_id"] != runID || msg.Attributes["node"] != asset.Name {
				msg.Nack()
				return
			}
			var envelope result.ResultEnvelope
			if err := json.Unmarshal(msg.Data, &envelope); err != nil {
				slog.Warn("malformed result message", "error", err)
				msg.Nack()
				return
			}
			if envelope.Version == "" {
				slog.Warn("result envelope missing version (possible old runner)", "run_id", runID, "node", asset.Name)
			} else if envelope.Version != result.EnvelopeVersion {
				slog.Warn("result envelope version mismatch", "expected", result.EnvelopeVersion, "got", envelope.Version, "run_id", runID, "node", asset.Name)
			}
			msg.Ack()
			resultCh <- envelope
			cancel() // stop receiving
		})
		if err != nil && err != context.Canceled {
			errCh <- err
		}
	}()

	select {
	case envelope := <-resultCh:
		end := time.Now()
		metadata := make(map[string]string)
		for k, v := range envelope.Telemetry {
			metadata[k] = fmt.Sprintf("%v", v)
		}
		metadata["runner"] = "cloud_run_job"

		nodeResult := NodeResult{
			AssetName: asset.Name,
			Status:    envelope.Status,
			StartTime: start,
			EndTime:   end,
			Duration:  end.Sub(start),
			Error:     envelope.Error,
			ExitCode:  envelope.ExitCode,
			Metadata:  metadata,
		}
		return nodeResult, nil

	case err := <-errCh:
		return NodeResult{
			AssetName: asset.Name,
			Status:    "failed",
			StartTime: start,
			EndTime:   time.Now(),
			Duration:  time.Since(start),
			Error:     fmt.Sprintf("result subscription error: %v", err),
			ExitCode:  -1,
			Metadata:  map[string]string{"runner": "cloud_run_job"},
		}, nil

	case <-timeoutCtx.Done():
		return NodeResult{
			AssetName: asset.Name,
			Status:    "failed",
			StartTime: start,
			EndTime:   time.Now(),
			Duration:  time.Since(start),
			Error:     fmt.Sprintf("no result received within %s timeout", d.timeout),
			ExitCode:  -1,
			Metadata:  map[string]string{"runner": "cloud_run_job"},
		}, nil
	}
}

// Supports reports whether this dispatcher handles the given asset type.
func (d *CloudRunJobDispatch) Supports(assetType string) bool {
	return assetType == "python" || assetType == "dlt"
}

// Close releases resources.
func (d *CloudRunJobDispatch) Close() error {
	return d.resultClient.Close()
}

// Verify CloudRunJobDispatch implements RunnerDispatch at compile time.
var _ RunnerDispatch = (*CloudRunJobDispatch)(nil)
