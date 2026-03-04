package executor

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"

	"cloud.google.com/go/bigquery"

	"github.com/Andrew-DataDecode/Granicus/internal/config"
	gctx "github.com/Andrew-DataDecode/Granicus/internal/context"
	"github.com/Andrew-DataDecode/Granicus/internal/graph"
)

type PostRunHook func(g *graph.Graph, cfg *config.PipelineConfig, projectRoot string, rr *RunResult) error

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

func DuckDBAssemblyHook() PostRunHook {
	return func(_ *graph.Graph, cfg *config.PipelineConfig, projectRoot string, rr *RunResult) error {
		if !assetSucceeded(rr, "publish_dashboard_parquet") {
			log.Printf("INFO: skipping DuckDB assembly (publish_dashboard_parquet did not succeed)")
			return nil
		}

		scriptPath := filepath.Join(projectRoot, "python", "build_dashboard_duckdb.py")
		conn := cfg.Connections["gcs_dashboard"]
		if conn == nil {
			return fmt.Errorf("DuckDB assembly: gcs_dashboard connection not found")
		}

		cmd := exec.Command("python3", scriptPath)
		cmd.Env = append(os.Environ(),
			"GRANICUS_GCS_BUCKET="+conn.Properties["bucket"],
			"GRANICUS_GCS_PREFIX="+conn.Properties["prefix"],
		)
		if creds, ok := conn.Properties["credentials"]; ok {
			cmd.Env = append(cmd.Env, "GOOGLE_APPLICATION_CREDENTIALS="+creds)
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

func RunPostHooks(hooks []PostRunHook, g *graph.Graph, cfg *config.PipelineConfig, projectRoot string, rr *RunResult) int {
	failures := 0
	for _, hook := range hooks {
		if err := hook(g, cfg, projectRoot, rr); err != nil {
			log.Printf("WARNING: post-run hook failed: %v", err)
			failures++
		}
	}
	return failures
}
