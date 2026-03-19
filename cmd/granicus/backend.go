package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/DataDecodeHQ/granicus/internal/runner"
	"github.com/DataDecodeHQ/granicus/internal/source"
	"github.com/DataDecodeHQ/granicus/internal/state"
)

// Backends holds all initialized backend components.
type Backends struct {
	State    state.StateBackend
	Source   source.PipelineSource
	Dispatch runner.RunnerDispatch
}

// initBackends initializes all backends and logs a structured startup line.
func initBackends(projectRoot, configDir, envName string) (*Backends, error) {
	stateBackend, err := initStateBackend(projectRoot, "", envName)
	if err != nil {
		return nil, fmt.Errorf("state backend: %w", err)
	}

	pipeSrc, err := initPipelineSource(configDir)
	if err != nil {
		stateBackend.Close()
		return nil, fmt.Errorf("pipeline source: %w", err)
	}

	dispatch, err := initRunnerDispatch()
	if err != nil {
		stateBackend.Close()
		return nil, fmt.Errorf("runner dispatch: %w", err)
	}

	stateLabel := os.Getenv("GRANICUS_STATE_BACKEND")
	if stateLabel == "" {
		stateLabel = "sqlite"
	}
	sourceLabel := os.Getenv("GRANICUS_PIPELINE_SOURCE")
	if sourceLabel == "" {
		sourceLabel = "local"
	}
	dispatchLabel := os.Getenv("GRANICUS_RUNNER_DISPATCH")
	if dispatchLabel == "" {
		dispatchLabel = "local"
	}

	slog.Info("backends initialized", "state", stateLabel, "source", sourceLabel, "dispatch", dispatchLabel)

	return &Backends{
		State:    stateBackend,
		Source:   pipeSrc,
		Dispatch: dispatch,
	}, nil
}

// initStateBackend creates the appropriate StateBackend based on env vars.
// GRANICUS_STATE_BACKEND=sqlite (default) or firestore.
func initStateBackend(projectRoot, pipeline, envName string) (state.StateBackend, error) {
	backend := os.Getenv("GRANICUS_STATE_BACKEND")

	switch backend {
	case "firestore":
		ctx := context.Background()
		return state.NewFirestoreStateBackend(ctx, "", pipeline)

	case "sqlite", "":
		stateDBPath := filepath.Join(projectRoot, ".granicus", "state.db")
		return state.New(stateDBPath)

	default:
		return nil, fmt.Errorf("unknown state backend: %s (use sqlite or firestore)", backend)
	}
}

// initRunnerDispatch creates the appropriate RunnerDispatch based on env vars.
// GRANICUS_RUNNER_DISPATCH=local (default) or cloud_run_job.
func initRunnerDispatch() (runner.RunnerDispatch, error) {
	mode := os.Getenv("GRANICUS_RUNNER_DISPATCH")

	switch mode {
	case "local", "":
		return nil, nil

	case "cloud_run_job":
		return runner.NewCloudRunJobDispatch(context.Background(), runner.CloudRunJobConfig{})

	default:
		return nil, fmt.Errorf("unknown runner dispatch: %s (use local or cloud_run_job)", mode)
	}
}

// initPipelineSource creates the appropriate PipelineSource based on env vars.
// GRANICUS_PIPELINE_SOURCE=local (default) or gcs.
func initPipelineSource(configDir string) (source.PipelineSource, error) {
	mode := os.Getenv("GRANICUS_PIPELINE_SOURCE")

	switch mode {
	case "local", "":
		if configDir == "" {
			return nil, fmt.Errorf("--config-dir is required when GRANICUS_PIPELINE_SOURCE is local or unset")
		}
		return source.NewLocalSource(configDir), nil

	case "gcs":
		bucket := os.Getenv("GRANICUS_GCS_BUCKET")
		if bucket == "" {
			return nil, fmt.Errorf("GRANICUS_GCS_BUCKET is required when GRANICUS_PIPELINE_SOURCE=gcs")
		}
		return source.NewGCSVersionedSource(context.Background(), "", bucket)

	default:
		return nil, fmt.Errorf("unknown pipeline source: %s (use local or gcs)", mode)
	}
}
