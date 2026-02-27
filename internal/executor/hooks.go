package executor

import (
	"log"
	"path/filepath"

	"cloud.google.com/go/bigquery"

	"github.com/analytehealth/granicus/internal/config"
	gctx "github.com/analytehealth/granicus/internal/context"
	"github.com/analytehealth/granicus/internal/graph"
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

func RunPostHooks(hooks []PostRunHook, g *graph.Graph, cfg *config.PipelineConfig, projectRoot string, rr *RunResult) {
	for _, hook := range hooks {
		if err := hook(g, cfg, projectRoot, rr); err != nil {
			log.Printf("WARNING: post-run hook failed: %v", err)
		}
	}
}
