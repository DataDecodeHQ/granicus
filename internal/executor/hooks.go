package executor

import (
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"

	"cloud.google.com/go/bigquery"

	"github.com/DataDecodeHQ/granicus/internal/config"
	gctx "github.com/DataDecodeHQ/granicus/internal/context"
	"github.com/DataDecodeHQ/granicus/internal/graph"
)

type PostRunHook func(g *graph.Graph, cfg *config.PipelineConfig, projectRoot string, rr *RunResult) error

// WriteContextHook returns a post-run hook that syncs schemas, lineage, and assets to a local context database.
func WriteContextHook(bqClient *bigquery.Client) PostRunHook {
	return func(g *graph.Graph, cfg *config.PipelineConfig, projectRoot string, _ *RunResult) error {
		dbPath := filepath.Join(projectRoot, ".granicus", "context.db")

		datasets := cfg.OutputDatasets()
		schemas := gctx.SyncSchemas(bqClient, datasets)
		lineage := gctx.ExtractLineage(g, cfg)
		assets := gctx.ExtractAssets(g, cfg, projectRoot)

		return gctx.CreateOrReplace(dbPath, schemas, lineage, assets)
	}
}

// DuckDBAssemblyHook returns a post-run hook that builds a DuckDB file from dashboard parquet data.
func DuckDBAssemblyHook() PostRunHook {
	return func(_ *graph.Graph, cfg *config.PipelineConfig, projectRoot string, rr *RunResult) error {
		if !assetSucceeded(rr, "publish_dashboard_parquet") {
			slog.Info("skipping DuckDB assembly (publish_dashboard_parquet did not succeed)")
			return nil
		}

		scriptPath := filepath.Join(projectRoot, "python", "build_dashboard_duckdb.py")
		conn := cfg.Connections["gcs_dashboard"]
		if conn == nil {
			return fmt.Errorf("DuckDB assembly: gcs_dashboard connection not found")
		}

		pythonBin := "python3"
		if venvPy := filepath.Join(projectRoot, ".venv", "bin", "python3"); fileExists(venvPy) {
			pythonBin = venvPy
		}
		cmd := exec.Command(pythonBin, scriptPath)
		cmd.Env = append(os.Environ(),
			"GRANICUS_GCS_BUCKET="+conn.Properties["bucket"],
			"GRANICUS_GCS_PREFIX="+conn.Properties["prefix"],
		)
		credPath, _ := config.ResolveConnectionCredentials(conn)
		if credPath != "" {
			if !filepath.IsAbs(credPath) {
				credPath = filepath.Join(projectRoot, credPath)
			}
			cmd.Env = append(cmd.Env, "GOOGLE_APPLICATION_CREDENTIALS="+credPath)
		}
		cmd.Dir = projectRoot
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		if err := cmd.Run(); err != nil {
			return fmt.Errorf("DuckDB assembly failed: %w", err)
		}
		return nil
	}
}

func assetSucceeded(rr *RunResult, name string) bool {
	if rr == nil {
		return false
	}
	for _, r := range rr.Results {
		if r.AssetName == name && r.Status == "success" {
			return true
		}
	}
	return false
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// RunPostHooks executes all post-run hooks in order and returns the number of failures.
func RunPostHooks(hooks []PostRunHook, g *graph.Graph, cfg *config.PipelineConfig, projectRoot string, rr *RunResult) int {
	failures := 0
	for _, hook := range hooks {
		if err := hook(g, cfg, projectRoot, rr); err != nil {
			slog.Warn("post-run hook failed", "error", err)
			failures++
		}
	}
	return failures
}
