package checker

import (
	"fmt"

	"github.com/analytehealth/granicus/internal/config"
	"github.com/analytehealth/granicus/internal/graph"
)

func GenerateDefaultCheckNodes(cfg *config.PipelineConfig) ([]graph.AssetInput, map[string][]string) {
	var nodes []graph.AssetInput
	deps := make(map[string][]string)

	for _, asset := range cfg.Assets {
		if asset.DefaultChecks != nil && !*asset.DefaultChecks {
			continue
		}
		if asset.Layer == "" || asset.Grain == "" {
			continue
		}

		checks := defaultChecksForLayer(asset.Layer, asset.Name, asset.Grain, asset.DestinationConnection)
		for _, c := range checks {
			nodes = append(nodes, c)
			deps[c.Name] = []string{asset.Name}
		}
	}

	return nodes, deps
}

func defaultChecksForLayer(layer, assetName, grain, destConn string) []graph.AssetInput {
	var checks []graph.AssetInput

	switch layer {
	case "staging", "intermediate":
		checks = append(checks,
			defaultCheckNode(assetName, "unique_grain", grain, destConn, uniqueGrainSQL(assetName, grain)),
			defaultCheckNode(assetName, "not_null_grain", grain, destConn, notNullGrainSQL(assetName, grain)),
		)
	case "entity":
		checks = append(checks,
			defaultCheckNode(assetName, "unique_grain", grain, destConn, uniqueGrainSQL(assetName, grain)),
			defaultCheckNode(assetName, "not_null_grain", grain, destConn, notNullGrainSQL(assetName, grain)),
			defaultCheckNode(assetName, "row_count", grain, destConn, rowCountSQL(assetName)),
		)
	case "report":
		checks = append(checks,
			defaultCheckNode(assetName, "row_count", grain, destConn, rowCountSQL(assetName)),
			defaultCheckNode(assetName, "not_null_grain", grain, destConn, notNullGrainSQL(assetName, grain)),
		)
	}

	return checks
}

func defaultCheckNode(assetName, checkName, grain, destConn, sql string) graph.AssetInput {
	return graph.AssetInput{
		Name:                  fmt.Sprintf("check:%s:default:%s", assetName, checkName),
		Type:                  "sql_check",
		DestinationConnection: destConn,
		SourceAsset:           assetName,
		InlineSQL:             sql,
	}
}

func uniqueGrainSQL(assetName, grain string) string {
	return fmt.Sprintf(
		"SELECT %s, COUNT(*) AS cnt FROM `{{.Project}}.{{.Dataset}}.%s` GROUP BY %s HAVING cnt > 1 LIMIT 10",
		grain, assetName, grain,
	)
}

func notNullGrainSQL(assetName, grain string) string {
	return fmt.Sprintf(
		"SELECT * FROM `{{.Project}}.{{.Dataset}}.%s` WHERE %s IS NULL LIMIT 10",
		assetName, grain,
	)
}

func rowCountSQL(assetName string) string {
	return fmt.Sprintf(
		"SELECT 'empty' AS error FROM `{{.Project}}.{{.Dataset}}.%s` HAVING COUNT(*) = 0",
		assetName,
	)
}
