package checker

import (
	"fmt"
	"strings"
	"time"

	"github.com/analytehealth/granicus/internal/config"
	"github.com/analytehealth/granicus/internal/graph"
)

func GenerateSourceCheckNodes(cfg *config.PipelineConfig) ([]graph.AssetInput, map[string][]string) {
	var nodes []graph.AssetInput
	deps := make(map[string][]string)

	for sourceName, src := range cfg.Sources {
		project := resolveSourceProject(cfg, src)

		checks := sourceChecksFor(sourceName, src, project)
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

func sourceChecksFor(sourceName string, src config.SourceConfig, project string) []graph.AssetInput {
	var checks []graph.AssetInput
	destConn := src.Connection

	// exists_not_empty (always)
	checks = append(checks, sourceCheckNode(sourceName, "exists_not_empty", destConn,
		sourceExistsNotEmptySQL(src.Identifier)))

	// freshness (when ExpectedFresh is set)
	if src.ExpectedFresh != "" {
		d, err := time.ParseDuration(src.ExpectedFresh)
		if err == nil {
			minutes := int(d.Minutes())
			checks = append(checks, sourceCheckNode(sourceName, "freshness", destConn,
				sourceFreshnessSQL(src.Identifier, minutes)))
		}
	}

	// pk_not_null (when PrimaryKey is set)
	if src.PrimaryKey != "" {
		checks = append(checks, sourceCheckNode(sourceName, "pk_not_null", destConn,
			sourcePKNotNullSQL(src.Identifier, src.PrimaryKey)))
	}

	// pk_unique (when PrimaryKey is set)
	if src.PrimaryKey != "" {
		checks = append(checks, sourceCheckNode(sourceName, "pk_unique", destConn,
			sourcePKUniqueSQL(src.Identifier, src.PrimaryKey)))
	}

	// expected_columns (when ExpectedColumns is set)
	if len(src.ExpectedColumns) > 0 {
		dataset, tableName := parseIdentifierDatasetTable(src.Identifier)
		checks = append(checks, sourceCheckNode(sourceName, "expected_columns", destConn,
			sourceExpectedColumnsSQL(project, dataset, tableName, src.ExpectedColumns)))
	}

	return checks
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

func sourceExistsNotEmptySQL(identifier string) string {
	return fmt.Sprintf(
		"SELECT 'EMPTY_OR_MISSING' AS issue_type, 0 AS row_count FROM (SELECT 1) WHERE NOT EXISTS (SELECT 1 FROM `{{.Project}}.%s`)",
		identifier,
	)
}

func sourceFreshnessSQL(identifier string, minutes int) string {
	return fmt.Sprintf(
		"SELECT MAX(datastream_metadata.source_timestamp) AS latest_source_timestamp, TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(datastream_metadata.source_timestamp), MINUTE) AS staleness_minutes, 'STALE_SOURCE' AS issue_type FROM `{{.Project}}.%s` HAVING TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(datastream_metadata.source_timestamp), MINUTE) > %d",
		identifier, minutes,
	)
}

func sourcePKNotNullSQL(identifier, pk string) string {
	return fmt.Sprintf(
		"SELECT 'NULL_PK' AS issue_type, COUNT(*) AS null_count FROM `{{.Project}}.%s` WHERE %s IS NULL HAVING COUNT(*) > 0",
		identifier, pk,
	)
}

func sourcePKUniqueSQL(identifier, pk string) string {
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

// parseIdentifierDatasetTable extracts the dataset and table_name from a
// project.dataset.table identifier string. Returns empty strings if the
// identifier does not contain at least two dot-separated parts.
func parseIdentifierDatasetTable(identifier string) (dataset, tableName string) {
	parts := strings.Split(identifier, ".")
	if len(parts) < 2 {
		return "", ""
	}
	tableName = parts[len(parts)-1]
	dataset = parts[len(parts)-2]
	return dataset, tableName
}
