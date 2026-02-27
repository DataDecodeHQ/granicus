package checker

import (
	"fmt"

	"github.com/analytehealth/granicus/internal/config"
	"github.com/analytehealth/granicus/internal/graph"
)

func GenerateDefaultCheckNodes(cfg *config.PipelineConfig) ([]graph.AssetInput, map[string][]string) {
	return GenerateDefaultCheckNodesWithDirectives(cfg, nil)
}

func GenerateDefaultCheckNodesWithDirectives(cfg *config.PipelineConfig, directives map[string]graph.Directives) ([]graph.AssetInput, map[string][]string) {
	var nodes []graph.AssetInput
	deps := make(map[string][]string)

	for _, asset := range cfg.Assets {
		if asset.DefaultChecks != nil && !*asset.DefaultChecks {
			continue
		}
		if asset.Layer == "" || asset.Grain == "" {
			continue
		}

		var d graph.Directives
		if directives != nil {
			d = directives[asset.Name]
		}

		checks := defaultChecksForLayer(asset, d)
		for _, c := range checks {
			nodes = append(nodes, c)
			deps[c.Name] = []string{asset.Name}
		}
	}

	// Wire FK checks
	fkNodes, fkDeps := GenerateFKCheckNodes(cfg)
	for _, n := range fkNodes {
		nodes = append(nodes, n)
	}
	for k, v := range fkDeps {
		deps[k] = v
	}

	// Wire completeness checks
	compNodes, compDeps := GenerateCompletenessCheckNodes(cfg)
	for _, n := range compNodes {
		nodes = append(nodes, n)
	}
	for k, v := range compDeps {
		deps[k] = v
	}

	// Wire standards checks
	stdNodes, stdDeps := GenerateStandardsCheckNodes(cfg)
	for _, n := range stdNodes {
		nodes = append(nodes, n)
	}
	for k, v := range stdDeps {
		deps[k] = v
	}

	return nodes, deps
}

func defaultChecksForLayer(asset config.AssetConfig, d graph.Directives) []graph.AssetInput {
	blocking := asset.DefaultChecksBlocking
	var checks []graph.AssetInput
	layer := asset.Layer
	assetName := asset.Name
	grain := asset.Grain
	destConn := asset.DestinationConnection

	switch layer {
	case "staging":
		checks = append(checks,
			defaultCheckNode(assetName, "unique_grain", grain, destConn, uniqueGrainSQL(assetName, grain), blocking),
			defaultCheckNode(assetName, "not_null_grain", grain, destConn, notNullGrainSQL(assetName, grain), blocking),
			defaultCheckNode(assetName, "not_empty", grain, destConn, notEmptySQL(assetName), blocking),
			defaultCheckNode(assetName, "no_future_timestamps", grain, destConn, noFutureTimestampsSQL(assetName, grain), blocking),
			defaultCheckNode(assetName, "updated_at_gte_created_at", grain, destConn, updatedAtGteCreatedAtSQL(assetName, grain), blocking),
		)
		sourceTable := d.SourceTable
		sourcePK := d.SourcePK
		if sourceTable != "" {
			if sourcePK == "" {
				sourcePK = grain
			}
			checks = append(checks,
				defaultCheckNode(assetName, "source_completeness", grain, destConn, sourceCompletenessSQL(assetName, grain, sourceTable, sourcePK), blocking),
			)
		}
	case "intermediate":
		checks = append(checks,
			defaultCheckNode(assetName, "unique_grain", grain, destConn, uniqueGrainSQL(assetName, grain), blocking),
		)
		primaryUpstream := resolvePrimaryUpstream(asset)
		if primaryUpstream != "" {
			if asset.FanOutCheck == nil || *asset.FanOutCheck {
				checks = append(checks,
					defaultCheckNode(assetName, "fan_out", grain, destConn, fanOutSQL(assetName, primaryUpstream), blocking),
				)
			}
			checks = append(checks,
				defaultCheckNode(assetName, "row_retention", grain, destConn, rowRetentionSQL(assetName, primaryUpstream, *asset.MinRetentionRatio), blocking),
			)
		}
	case "entity", "analytics":
		checks = append(checks,
			defaultCheckNode(assetName, "unique_grain", grain, destConn, uniqueGrainSQL(assetName, grain), blocking),
		)
	case "report":
		checks = append(checks,
			defaultCheckNode(assetName, "row_count", grain, destConn, rowCountSQL(assetName), blocking),
		)
	}

	return checks
}

func defaultCheckNode(assetName, checkName, grain, destConn, sql string, blocking bool) graph.AssetInput {
	return graph.AssetInput{
		Name:                  fmt.Sprintf("check:%s:default:%s", assetName, checkName),
		Type:                  "sql_check",
		DestinationConnection: destConn,
		SourceAsset:           assetName,
		Blocking:              blocking,
		InlineSQL:             sql,
	}
}

func uniqueGrainSQL(assetName, grain string) string {
	return fmt.Sprintf(
		`SELECT %s, COUNT(*) AS cnt FROM `+"`{{ ref \"%s\" }}`"+` GROUP BY %s HAVING cnt > 1 LIMIT 10`,
		grain, assetName, grain,
	)
}

func notNullGrainSQL(assetName, grain string) string {
	return fmt.Sprintf(
		`SELECT * FROM `+"`{{ ref \"%s\" }}`"+` WHERE %s IS NULL LIMIT 10`,
		assetName, grain,
	)
}

func rowCountSQL(assetName string) string {
	return fmt.Sprintf(
		`SELECT 'empty' AS error FROM `+"`{{ ref \"%s\" }}`"+` HAVING COUNT(*) = 0`,
		assetName,
	)
}

func notEmptySQL(assetName string) string {
	return fmt.Sprintf(
		`SELECT 'EMPTY_TABLE' AS issue_type, 0 AS row_count FROM (SELECT 1) WHERE NOT EXISTS (SELECT 1 FROM `+"`{{ ref \"%s\" }}`)",
		assetName,
	)
}

func noFutureTimestampsSQL(assetName, grain string) string {
	return fmt.Sprintf(
		`SELECT %s, created_at, updated_at, CASE WHEN created_at > CURRENT_TIMESTAMP() THEN 'FUTURE_CREATED_AT' WHEN updated_at > CURRENT_TIMESTAMP() THEN 'FUTURE_UPDATED_AT' END AS issue_type FROM `+"`{{ ref \"%s\" }}`"+` WHERE created_at > CURRENT_TIMESTAMP() OR updated_at > CURRENT_TIMESTAMP()`,
		grain, assetName,
	)
}

func updatedAtGteCreatedAtSQL(assetName, grain string) string {
	return fmt.Sprintf(
		`SELECT %s, created_at, updated_at FROM `+"`{{ ref \"%s\" }}`"+` WHERE updated_at < created_at AND created_at > '2024-01-01'`,
		grain, assetName,
	)
}

func resolvePrimaryUpstream(asset config.AssetConfig) string {
	if asset.PrimaryUpstream != "" {
		return asset.PrimaryUpstream
	}
	if len(asset.Upstream) > 0 {
		return asset.Upstream[0]
	}
	return ""
}

func fanOutSQL(assetName, primaryUpstream string) string {
	return fmt.Sprintf(
		"WITH current_count AS (\n  SELECT COUNT(*) AS cnt FROM `{{ ref \"%s\" }}`\n),\nupstream_count AS (\n  SELECT COUNT(*) AS cnt FROM `{{ ref \"%s\" }}`\n)\nSELECT c.cnt AS table_row_count, u.cnt AS upstream_row_count, 'FAN_OUT_DETECTED' AS issue_type\nFROM current_count c CROSS JOIN upstream_count u\nWHERE c.cnt > u.cnt",
		assetName, primaryUpstream,
	)
}

func rowRetentionSQL(assetName, primaryUpstream string, minRetentionRatio float64) string {
	return fmt.Sprintf(
		"WITH this_count AS (\n  SELECT COUNT(*) AS cnt FROM `{{ ref \"%s\" }}`\n),\nprimary_upstream_count AS (\n  SELECT COUNT(*) AS cnt FROM `{{ ref \"%s\" }}`\n)\nSELECT t.cnt AS table_row_count, u.cnt AS upstream_row_count, SAFE_DIVIDE(t.cnt, u.cnt) AS retention_ratio, 'SUSPICIOUS_ROW_LOSS' AS issue_type\nFROM this_count t CROSS JOIN primary_upstream_count u\nWHERE SAFE_DIVIDE(t.cnt, u.cnt) < %g",
		assetName, primaryUpstream, minRetentionRatio,
	)
}

func sourceCompletenessSQL(assetName, grain, sourceTable, sourcePK string) string {
	return fmt.Sprintf(
		"WITH source_pks AS (\n  SELECT DISTINCT %s AS pk FROM `{{.Project}}.%s` WHERE DATE(created_at) < CURRENT_DATE()\n),\nstaging_pks AS (\n  SELECT DISTINCT %s AS pk FROM `{{ ref \"%s\" }}` WHERE DATE(created_at) < CURRENT_DATE()\n),\nmissing_from_staging AS (\n  SELECT s.pk, 'MISSING_FROM_STAGING' AS issue_type FROM source_pks s LEFT JOIN staging_pks st ON s.pk = st.pk WHERE st.pk IS NULL\n),\nmissing_from_source AS (\n  SELECT st.pk, 'MISSING_FROM_SOURCE' AS issue_type FROM staging_pks st LEFT JOIN source_pks s ON st.pk = s.pk WHERE s.pk IS NULL\n)\nSELECT * FROM missing_from_staging UNION ALL SELECT * FROM missing_from_source",
		sourcePK, sourceTable, grain, assetName,
	)
}
