package checker

import (
	"fmt"
	"strings"

	"github.com/Andrew-DataDecode/Granicus/internal/config"
	"github.com/Andrew-DataDecode/Granicus/internal/graph"
)

func GenerateCompletenessCheckNodes(cfg *config.PipelineConfig) ([]graph.AssetInput, map[string][]string) {
	var nodes []graph.AssetInput
	deps := make(map[string][]string)

	for _, asset := range cfg.Assets {
		if asset.Completeness == nil {
			continue
		}

		comp := asset.Completeness
		grain := asset.Grain
		if grain == "" {
			grain = comp.SourcePK
		}
		destConn := asset.DestinationConnection

		sql := completenessSQL(asset.Name, grain, comp)

		node := graph.AssetInput{
			Name:                  fmt.Sprintf("check:%s:default:completeness", asset.Name),
			Type:                  "sql_check",
			DestinationConnection: destConn,
			SourceAsset:           asset.Name,
			InlineSQL:             sql,
			Blocking:              asset.DefaultChecksBlocking,
		}

		nodes = append(nodes, node)
		deps[node.Name] = []string{asset.Name}
	}

	return nodes, deps
}

func completenessSQL(assetName, grain string, comp *config.CompletenessConfig) string {
	tolerance := 0.01
	if comp.Tolerance != nil {
		tolerance = *comp.Tolerance
	}

	var b strings.Builder

	b.WriteString(fmt.Sprintf("WITH source_pks AS (\n  SELECT DISTINCT %s AS pk\n  FROM {{ ref \"%s\" }}\n)",
		comp.SourcePK, comp.SourceTable))

	for i, excl := range comp.Exclusions {
		b.WriteString(fmt.Sprintf(",\nexclusion_%d AS (\n  SELECT DISTINCT %s AS pk\n  FROM {{ ref \"%s\" }}",
			i+1, excl.PK, excl.Table))
		if excl.Filter != "" {
			b.WriteString(fmt.Sprintf("\n  WHERE %s", excl.Filter))
		}
		b.WriteString("\n)")
	}

	for i, add := range comp.Additions {
		b.WriteString(fmt.Sprintf(",\naddition_%d AS (\n  SELECT DISTINCT %s AS pk\n  FROM {{ ref \"%s\" }}",
			i+1, add.PK, add.Table))
		if add.Filter != "" {
			b.WriteString(fmt.Sprintf("\n  WHERE %s", add.Filter))
		}
		b.WriteString("\n)")
	}

	b.WriteString(",\nexpected_pks AS (\n  SELECT pk FROM source_pks")
	for i := range comp.Exclusions {
		b.WriteString(fmt.Sprintf("\n  EXCEPT DISTINCT\n  SELECT pk FROM exclusion_%d", i+1))
	}
	for i := range comp.Additions {
		b.WriteString(fmt.Sprintf("\n  UNION DISTINCT\n  SELECT pk FROM addition_%d", i+1))
	}
	b.WriteString("\n)")

	b.WriteString(fmt.Sprintf(",\nentity_pks AS (\n  SELECT DISTINCT %s AS pk\n  FROM {{ ref \"%s\" }}\n)",
		grain, assetName))

	b.WriteString(",\nstats AS (\n  SELECT\n    (SELECT COUNT(*) FROM expected_pks) AS expected_count,\n    (SELECT COUNT(*) FROM entity_pks) AS entity_count,\n    (SELECT COUNT(*) FROM expected_pks WHERE pk NOT IN (SELECT pk FROM entity_pks)) AS missing_count,\n    (SELECT COUNT(*) FROM entity_pks WHERE pk NOT IN (SELECT pk FROM expected_pks)) AS unexpected_count\n)")

	b.WriteString(fmt.Sprintf("\nSELECT\n  expected_count,\n  entity_count,\n  missing_count,\n  unexpected_count,\n  SAFE_DIVIDE(missing_count + unexpected_count, expected_count) AS drift_ratio,\n  'COMPLETENESS_IMBALANCE' AS issue_type\nFROM stats\nWHERE SAFE_DIVIDE(missing_count + unexpected_count, expected_count) > %g",
		tolerance))

	return b.String()
}
