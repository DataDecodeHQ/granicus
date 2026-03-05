package checker

import (
	"path/filepath"
	"strings"

	"github.com/Andrew-DataDecode/Granicus/internal/config"
	"github.com/Andrew-DataDecode/Granicus/internal/graph"
)

func GenerateCheckNodes(cfg *config.PipelineConfig) ([]graph.AssetInput, map[string][]string) {
	var nodes []graph.AssetInput
	deps := make(map[string][]string)

	for _, asset := range cfg.Assets {
		for _, check := range asset.Checks {
			name := checkNodeName(asset.Name, check)
			checkType := check.Type
			if checkType == "" {
				checkType = inferCheckType(check.Source)
			}

			nodes = append(nodes, graph.AssetInput{
				Name:                  name,
				Type:                  checkType,
				Source:                check.Source,
				DestinationConnection: asset.DestinationConnection,
				SourceConnection:      asset.SourceConnection,
				Blocking:              check.Blocking,
				Severity:              check.Severity,
			})
			deps[name] = []string{asset.Name}
		}
	}

	return nodes, deps
}

func checkNodeName(assetName string, check config.CheckConfig) string {
	name := check.Name
	if name == "" {
		base := filepath.Base(check.Source)
		name = strings.TrimSuffix(base, filepath.Ext(base))
	}
	return "check:" + assetName + ":" + name
}

func inferCheckType(source string) string {
	ext := filepath.Ext(source)
	switch ext {
	case ".sql":
		return "sql_check"
	case ".py":
		return "python_check"
	default:
		return "shell"
	}
}
