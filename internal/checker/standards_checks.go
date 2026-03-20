package checker

import (
	"fmt"

	"github.com/DataDecodeHQ/granicus/internal/config"
	"github.com/DataDecodeHQ/granicus/internal/graph"
)

// standardsCheckType defines a parameterized check type for standards validation.
type standardsCheckType struct {
	name       string
	columns    []string
	sqlBuilder func(assetName, col string) string
}

// GenerateStandardsCheckNodes creates check nodes that validate email, phone, and currency format standards.
func GenerateStandardsCheckNodes(cfg *config.PipelineConfig) ([]graph.AssetInput, map[string][]string) {
	var nodes []graph.AssetInput
	deps := make(map[string][]string)

	for _, asset := range cfg.Assets {
		if asset.Standards == nil {
			continue
		}

		blocking := asset.StandardsBlocking

		checkTypes := []standardsCheckType{
			{
				name:       "email",
				columns:    asset.Standards.Email,
				sqlBuilder: emailStandardSQL,
			},
			{
				name:       "phone",
				columns:    asset.Standards.Phone,
				sqlBuilder: phoneStandardSQL,
			},
			{
				name:       "currency",
				columns:    asset.Standards.Currency,
				sqlBuilder: currencyStandardSQL,
			},
		}

		for _, checkType := range checkTypes {
			for _, col := range checkType.columns {
				checkName := fmt.Sprintf("standards_%s_%s", checkType.name, col)
				nodeName := fmt.Sprintf("check:%s:default:%s", asset.Name, checkName)
				sql := checkType.sqlBuilder(asset.Name, col)
				nodes = append(nodes, graph.AssetInput{
					Name:                  nodeName,
					Type:                  "sql_check",
					DestinationConnection: asset.DestinationConnection,
					SourceAsset:           asset.Name,
					InlineSQL:             sql,
					Blocking:              blocking,
				})
				deps[nodeName] = []string{asset.Name}
			}
		}
	}

	return nodes, deps
}

func emailStandardSQL(assetName, col string) string {
	return fmt.Sprintf(
		"SELECT %s, 'EMAIL_NOT_NORMALIZED' AS issue_type FROM {{ ref \"%s\" }} WHERE %s IS NOT NULL AND %s != LOWER(TRIM(%s)) LIMIT 10",
		col, assetName, col, col, col,
	)
}

func phoneStandardSQL(assetName, col string) string {
	return fmt.Sprintf(
		"SELECT %s, 'PHONE_FORMAT_INVALID' AS issue_type FROM {{ ref \"%s\" }} WHERE %s IS NOT NULL AND NOT REGEXP_CONTAINS(%s, r'^1-\\d{3}-\\d{3}-\\d{4}$') LIMIT 10",
		col, assetName, col, col,
	)
}

func currencyStandardSQL(assetName, col string) string {
	return fmt.Sprintf(
		"SELECT %s, 'CURRENCY_NOT_ROUNDED' AS issue_type FROM {{ ref \"%s\" }} WHERE %s IS NOT NULL AND %s != ROUND(%s, 2) LIMIT 10",
		col, assetName, col, col, col,
	)
}
