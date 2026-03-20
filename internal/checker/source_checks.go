package checker

import (
	"fmt"
	"strings"
	"time"

	"github.com/DataDecodeHQ/granicus/internal/config"
	"github.com/DataDecodeHQ/granicus/internal/graph"
)

// GenerateSourceCheckNodes creates check nodes for source existence, freshness, primary key, and expected columns.
func GenerateSourceCheckNodes(cfg *config.PipelineConfig) ([]graph.AssetInput, map[string][]string) {
	var nodes []graph.AssetInput
	deps := make(map[string][]string)

	for sourceName, src := range cfg.Sources {
		project := resolveSourceProject(cfg, src)

		var checks []graph.AssetInput
		if len(src.Tables) > 0 {
			checks = sourceChecksForTables(sourceName, src, project)
		} else if strings.Contains(src.Identifier, ".") {
			checks = sourceChecksForLegacy(sourceName, src, project)
		}

		for _, c := range checks {
			nodes = append(nodes, c)
			deps[c.Name] = []string{"source:" + sourceName}
		}
	}

	return nodes, deps
}

func resolveSourceProject(cfg *config.PipelineConfig, src config.SourceConfig) string {
	if src.Connection == "" {
		return ""
	}
	conn, ok := cfg.Connections[src.Connection]
	if !ok {
		return ""
	}
	return conn.Properties["project"]
}

func sourceChecksForTables(sourceName string, src config.SourceConfig, project string) []graph.AssetInput {
	var checks []graph.AssetInput
	destConn := src.Connection

	for _, table := range src.Tables {
		checks = append(checks, sourceCheckNodeForTable(sourceName, table, "exists_not_empty", destConn,
			sourceExistsNotEmptySQL(sourceName, table)))
	}

	if src.ExpectedFresh != "" {
		d, err := time.ParseDuration(src.ExpectedFresh)
		if err == nil {
			minutes := int(d.Minutes())
			checks = append(checks, sourceCheckNode(sourceName, "freshness", destConn,
				sourceFreshnessSQL(sourceName, src.Tables[0], minutes)))
		}
	}

	if src.PrimaryKey != "" && len(src.Tables) == 1 {
		table := src.Tables[0]
		checks = append(checks, sourceCheckNode(sourceName, "pk_not_null", destConn,
			sourcePKNotNullSQL(sourceName, table, src.PrimaryKey)))
		checks = append(checks, sourceCheckNode(sourceName, "pk_unique", destConn,
			sourcePKUniqueSQL(sourceName, table, src.PrimaryKey)))
	}

	if len(src.ExpectedColumns) > 0 && len(src.Tables) == 1 {
		checks = append(checks, sourceCheckNode(sourceName, "expected_columns", destConn,
			sourceExpectedColumnsSQL(project, src.Identifier, src.Tables[0], src.ExpectedColumns)))
	}

	return checks
}

func sourceChecksForLegacy(sourceName string, src config.SourceConfig, project string) []graph.AssetInput {
	var checks []graph.AssetInput
	destConn := src.Connection

	checks = append(checks, sourceCheckNode(sourceName, "exists_not_empty", destConn,
		legacyExistsNotEmptySQL(src.Identifier)))

	if src.ExpectedFresh != "" {
		d, err := time.ParseDuration(src.ExpectedFresh)
		if err == nil {
			minutes := int(d.Minutes())
			checks = append(checks, sourceCheckNode(sourceName, "freshness", destConn,
				legacyFreshnessSQL(src.Identifier, minutes)))
		}
	}

	if src.PrimaryKey != "" {
		checks = append(checks, sourceCheckNode(sourceName, "pk_not_null", destConn,
			legacyPKNotNullSQL(src.Identifier, src.PrimaryKey)))
		checks = append(checks, sourceCheckNode(sourceName, "pk_unique", destConn,
			legacyPKUniqueSQL(src.Identifier, src.PrimaryKey)))
	}

	if len(src.ExpectedColumns) > 0 {
		dataset, tableName := parseIdentifierDatasetTable(src.Identifier)
		checks = append(checks, sourceCheckNode(sourceName, "expected_columns", destConn,
			sourceExpectedColumnsSQL(project, dataset, tableName, src.ExpectedColumns)))
	}

	return checks
}

func sourceCheckNodeForTable(sourceName, tableName, checkName, destConn, sql string) graph.AssetInput {
	return graph.AssetInput{
		Name:                  fmt.Sprintf("check:source:%s:%s:%s", sourceName, tableName, checkName),
		Type:                  "sql_check",
		DestinationConnection: destConn,
		SourceAsset:           sourceName,
		InlineSQL:             sql,
	}
}

func sourceCheckNode(sourceName, checkName, destConn, sql string) graph.AssetInput {
	return graph.AssetInput{
		Name:                  fmt.Sprintf("check:source:%s:default:%s", sourceName, checkName),
		Type:                  "sql_check",
		DestinationConnection: destConn,
		SourceAsset:           sourceName,
		InlineSQL:             sql,
	}
}

func sourceExistsNotEmptySQL(sourceName, tableName string) string {
	return fmt.Sprintf(
		`SELECT 'EMPTY_OR_MISSING' AS issue_type, 0 AS row_count FROM (SELECT 1) WHERE NOT EXISTS (SELECT 1 FROM {{ source "%s" "%s" }})`,
		sourceName, tableName,
	)
}

func sourceFreshnessSQL(sourceName, tableName string, minutes int) string {
	return fmt.Sprintf(
		`SELECT MAX(datastream_metadata.source_timestamp) AS latest_source_timestamp, TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), TIMESTAMP_MICROS(MAX(datastream_metadata.source_timestamp)), MINUTE) AS staleness_minutes, 'STALE_SOURCE' AS issue_type FROM {{ source "%s" "%s" }} HAVING TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), TIMESTAMP_MICROS(MAX(datastream_metadata.source_timestamp)), MINUTE) > %d`,
		sourceName, tableName, minutes,
	)
}

func sourcePKNotNullSQL(sourceName, tableName, pk string) string {
	return fmt.Sprintf(
		`SELECT 'NULL_PK' AS issue_type, COUNT(*) AS null_count FROM {{ source "%s" "%s" }} WHERE %s IS NULL HAVING COUNT(*) > 0`,
		sourceName, tableName, pk,
	)
}

func sourcePKUniqueSQL(sourceName, tableName, pk string) string {
	return fmt.Sprintf(
		`SELECT %s, COUNT(*) AS dupe_count FROM {{ source "%s" "%s" }} GROUP BY %s HAVING COUNT(*) > 1`,
		pk, sourceName, tableName, pk,
	)
}

// Legacy SQL functions use {{.Project}}.identifier format for backward compat
func legacyExistsNotEmptySQL(identifier string) string {
	return fmt.Sprintf(
		"SELECT 'EMPTY_OR_MISSING' AS issue_type, 0 AS row_count FROM (SELECT 1) WHERE NOT EXISTS (SELECT 1 FROM `{{.Project}}.%s`)",
		identifier,
	)
}

func legacyFreshnessSQL(identifier string, minutes int) string {
	return fmt.Sprintf(
		"SELECT MAX(datastream_metadata.source_timestamp) AS latest_source_timestamp, TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), TIMESTAMP_MICROS(MAX(datastream_metadata.source_timestamp)), MINUTE) AS staleness_minutes, 'STALE_SOURCE' AS issue_type FROM `{{.Project}}.%s` HAVING TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), TIMESTAMP_MICROS(MAX(datastream_metadata.source_timestamp)), MINUTE) > %d",
		identifier, minutes,
	)
}

func legacyPKNotNullSQL(identifier, pk string) string {
	return fmt.Sprintf(
		"SELECT 'NULL_PK' AS issue_type, COUNT(*) AS null_count FROM `{{.Project}}.%s` WHERE %s IS NULL HAVING COUNT(*) > 0",
		identifier, pk,
	)
}

func legacyPKUniqueSQL(identifier, pk string) string {
	return fmt.Sprintf(
		"SELECT %s, COUNT(*) AS dupe_count FROM `{{.Project}}.%s` GROUP BY %s HAVING COUNT(*) > 1",
		pk, identifier, pk,
	)
}

func sourceExpectedColumnsSQL(project, dataset, tableName string, columns []string) string {
	quoted := make([]string, len(columns))
	for i, col := range columns {
		quoted[i] = fmt.Sprintf("'%s'", col)
	}
	columnList := strings.Join(quoted, ", ")
	return fmt.Sprintf(
		"SELECT column_name AS expected_column, 'COLUMN_MISSING' AS issue_type FROM UNNEST([%s]) AS column_name WHERE column_name NOT IN (SELECT column_name FROM `%s.%s.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = '%s')",
		columnList, project, dataset, tableName,
	)
}

func parseIdentifierDatasetTable(identifier string) (dataset, tableName string) {
	parts := strings.Split(identifier, ".")
	if len(parts) < 2 {
		return "", ""
	}
	tableName = parts[len(parts)-1]
	dataset = parts[len(parts)-2]
	return dataset, tableName
}
