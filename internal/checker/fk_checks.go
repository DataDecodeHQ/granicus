package checker

import (
	"fmt"
	"strings"

	"github.com/analytehealth/granicus/internal/config"
	"github.com/analytehealth/granicus/internal/graph"
)

func GenerateFKCheckNodes(cfg *config.PipelineConfig) ([]graph.AssetInput, map[string][]string) {
	var nodes []graph.AssetInput
	deps := make(map[string][]string)

	for _, asset := range cfg.Assets {
		if len(asset.ForeignKeys) == 0 {
			continue
		}
		if asset.Grain == "" {
			continue
		}

		for _, fk := range asset.ForeignKeys {
			referencedTable, referencedColumn := splitFKReference(fk.References)

			if !fk.Nullable {
				checkName := fmt.Sprintf("fk_not_null_%s", fk.Column)
				nodeName := fmt.Sprintf("check:%s:default:%s", asset.Name, checkName)
				sql := fkNotNullSQL(asset.Name, asset.Grain, fk.Column)
				nodes = append(nodes, graph.AssetInput{
					Name:                  nodeName,
					Type:                  "sql_check",
					DestinationConnection: asset.DestinationConnection,
					SourceAsset:           asset.Name,
					InlineSQL:             sql,
				})
				deps[nodeName] = []string{asset.Name}
			}

			checkName := fmt.Sprintf("fk_integrity_%s", fk.Column)
			nodeName := fmt.Sprintf("check:%s:default:%s", asset.Name, checkName)
			sql := fkIntegritySQL(asset.Name, asset.Grain, fk.Column, referencedTable, referencedColumn)
			nodes = append(nodes, graph.AssetInput{
				Name:                  nodeName,
				Type:                  "sql_check",
				DestinationConnection: asset.DestinationConnection,
				SourceAsset:           asset.Name,
				InlineSQL:             sql,
			})
			deps[nodeName] = []string{asset.Name}
		}
	}

	return nodes, deps
}

func splitFKReference(references string) (table, column string) {
	idx := strings.LastIndex(references, ".")
	if idx < 0 {
		return references, ""
	}
	return references[:idx], references[idx+1:]
}

func fkNotNullSQL(assetName, grain, fkColumn string) string {
	return fmt.Sprintf(
		"SELECT %s, '%s' AS fk_column, 'FK_IS_NULL' AS issue_type FROM `{{.Project}}.{{.Dataset}}.%s` WHERE %s IS NULL LIMIT 10",
		grain, fkColumn, assetName, fkColumn,
	)
}

func fkIntegritySQL(assetName, grain, fkColumn, referencedTable, referencedColumn string) string {
	return fmt.Sprintf(
		"SELECT child.%s, child.%s, 'ORPHAN_FK' AS issue_type FROM `{{.Project}}.{{.Dataset}}.%s` child LEFT JOIN `{{.Project}}.{{.Dataset}}.%s` parent ON child.%s = parent.%s WHERE parent.%s IS NULL AND child.%s IS NOT NULL LIMIT 10",
		grain, fkColumn, assetName, referencedTable, fkColumn, referencedColumn, referencedColumn, fkColumn,
	)
}
