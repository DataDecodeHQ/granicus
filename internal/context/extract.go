package context

import (
	"encoding/json"
	"path/filepath"
	"strings"

	"github.com/Andrew-DataDecode/Granicus/internal/config"
	"github.com/Andrew-DataDecode/Granicus/internal/graph"
)

func ExtractLineage(g *graph.Graph, cfg *config.PipelineConfig) []Lineage {
	var result []Lineage
	for _, asset := range g.Assets {
		if strings.HasPrefix(asset.Name, "check:") || asset.Type == graph.AssetTypeSource {
			continue
		}
		targetDS := datasetForGraphAsset(asset, cfg)
		targetTable := asset.Name

		for _, dep := range asset.DependsOn {
			parent := g.Assets[dep]
			if parent == nil || strings.HasPrefix(dep, "check:") {
				continue
			}
			sourceDS := datasetForGraphAsset(parent, cfg)
			sourceTable := parent.Name

			result = append(result, Lineage{
				SourceAsset:   dep,
				TargetAsset:   asset.Name,
				SourceDataset: sourceDS,
				SourceTable:   sourceTable,
				TargetDataset: targetDS,
				TargetTable:   targetTable,
			})
		}
	}
	return result
}

func ExtractAssets(g *graph.Graph, cfg *config.PipelineConfig, projectRoot string) []Asset {
	var result []Asset
	for _, asset := range g.Assets {
		if strings.HasPrefix(asset.Name, "check:") || asset.Type == graph.AssetTypeSource {
			continue
		}
		ds := datasetForGraphAsset(asset, cfg)

		dirJSON := "{}"
		if asset.Source != "" {
			srcPath := filepath.Join(projectRoot, asset.Source)
			directives, err := graph.ParseDirectives(srcPath)
			if err == nil {
				if b, merr := json.Marshal(directives); merr == nil {
					dirJSON = string(b)
				}
			}
		}

		result = append(result, Asset{
			AssetName:     asset.Name,
			Dataset:       ds,
			TableName:     asset.Name,
			Layer:         asset.Layer,
			Grain:         asset.Grain,
			Docstring:     "",
			DirectiveJSON: dirJSON,
		})
	}
	return result
}

func datasetForGraphAsset(asset *graph.Asset, cfg *config.PipelineConfig) string {
	ac := findAssetConfig(cfg, asset.Name)
	if ac == nil {
		if asset.Type == graph.AssetTypeSource {
			return sourceDataset(cfg, asset.Name)
		}
		return ""
	}
	defaultDS := ""
	if ac.DestinationConnection != "" {
		if conn, ok := cfg.Connections[ac.DestinationConnection]; ok {
			defaultDS = conn.Properties["dataset"]
		}
	}
	return cfg.DatasetForAsset(*ac, defaultDS)
}

func findAssetConfig(cfg *config.PipelineConfig, name string) *config.AssetConfig {
	for i := range cfg.Assets {
		if cfg.Assets[i].Name == name {
			return &cfg.Assets[i]
		}
	}
	return nil
}

func sourceDataset(cfg *config.PipelineConfig, name string) string {
	if src, ok := cfg.Sources[name]; ok {
		return src.Identifier
	}
	return ""
}
