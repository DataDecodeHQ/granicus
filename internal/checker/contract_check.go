package checker

import (
	"fmt"
	"sort"
	"strings"

	"github.com/Andrew-DataDecode/Granicus/internal/config"
	"github.com/Andrew-DataDecode/Granicus/internal/graph"
)

// GenerateContractCheckNodes generates check nodes from contract declarations on assets.
// primary_key -> uniqueness check (returns duplicate rows)
// not_null    -> null check (returns rows where column IS NULL)
// accepted_values -> value set check (returns rows with disallowed values)
// All generated checks have severity "error".
func GenerateContractCheckNodes(cfg *config.PipelineConfig) ([]graph.AssetInput, map[string][]string) {
	var nodes []graph.AssetInput
	deps := make(map[string][]string)

	for _, asset := range cfg.Assets {
		if asset.Contract == nil {
			continue
		}
		c := asset.Contract

		if c.PrimaryKey != "" {
			nodeName := fmt.Sprintf("check:%s:default:contract_pk_%s", asset.Name, c.PrimaryKey)
			nodes = append(nodes, graph.AssetInput{
				Name:                  nodeName,
				Type:                  "sql_check",
				DestinationConnection: asset.DestinationConnection,
				SourceAsset:           asset.Name,
				InlineSQL:             contractPKSQL(asset.Name, c.PrimaryKey),
				Severity:              "error",
				Blocking:              true,
			})
			deps[nodeName] = []string{asset.Name}
		}

		for _, col := range c.NotNull {
			nodeName := fmt.Sprintf("check:%s:default:contract_not_null_%s", asset.Name, col)
			nodes = append(nodes, graph.AssetInput{
				Name:                  nodeName,
				Type:                  "sql_check",
				DestinationConnection: asset.DestinationConnection,
				SourceAsset:           asset.Name,
				InlineSQL:             contractNotNullSQL(asset.Name, col),
				Severity:              "error",
				Blocking:              true,
			})
			deps[nodeName] = []string{asset.Name}
		}

		// Sort accepted_values keys for deterministic output.
		avCols := make([]string, 0, len(c.AcceptedValues))
		for col := range c.AcceptedValues {
			avCols = append(avCols, col)
		}
		sort.Strings(avCols)

		for _, col := range avCols {
			vals := c.AcceptedValues[col]
			nodeName := fmt.Sprintf("check:%s:default:contract_accepted_values_%s", asset.Name, col)
			nodes = append(nodes, graph.AssetInput{
				Name:                  nodeName,
				Type:                  "sql_check",
				DestinationConnection: asset.DestinationConnection,
				SourceAsset:           asset.Name,
				InlineSQL:             contractAcceptedValuesSQL(asset.Name, col, vals),
				Severity:              "error",
				Blocking:              true,
			})
			deps[nodeName] = []string{asset.Name}
		}
	}

	return nodes, deps
}

func contractPKSQL(assetName, pkColumn string) string {
	return fmt.Sprintf(
		"SELECT %s, COUNT(*) AS duplicate_count, 'PK_NOT_UNIQUE' AS issue_type FROM {{ ref \"%s\" }} GROUP BY %s HAVING COUNT(*) > 1 LIMIT 10",
		pkColumn, assetName, pkColumn,
	)
}

func contractNotNullSQL(assetName, column string) string {
	return fmt.Sprintf(
		"SELECT %s, 'NULL_VALUE' AS issue_type FROM {{ ref \"%s\" }} WHERE %s IS NULL LIMIT 10",
		column, assetName, column,
	)
}

func contractAcceptedValuesSQL(assetName, column string, acceptedValues []string) string {
	quoted := make([]string, len(acceptedValues))
	for i, v := range acceptedValues {
		quoted[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''"))
	}
	valueList := strings.Join(quoted, ", ")
	return fmt.Sprintf(
		"SELECT %s, 'UNACCEPTED_VALUE' AS issue_type FROM {{ ref \"%s\" }} WHERE %s NOT IN (%s) OR %s IS NULL LIMIT 10",
		column, assetName, column, valueList, column,
	)
}
