package checker

import (
	"fmt"

	"github.com/Andrew-DataDecode/Granicus/internal/config"
	"github.com/Andrew-DataDecode/Granicus/internal/graph"
)

func GenerateStandardsCheckNodes(cfg *config.PipelineConfig) ([]graph.AssetInput, map[string][]string) {
	var nodes []graph.AssetInput
	deps := make(map[string][]string)

	for _, asset := range cfg.Assets {
		if asset.Standards == nil {
			continue
		}

		blocking := asset.StandardsBlocking

		for _, col := range asset.Standards.Email {
			checkName := fmt.Sprintf("standards_email_%s", col)
			nodeName := fmt.Sprintf("check:%s:default:%s", asset.Name, checkName)
			sql := emailStandardSQL(asset.Name, col)
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

		for _, col := range asset.Standards.Phone {
			checkName := fmt.Sprintf("standards_phone_%s", col)
			nodeName := fmt.Sprintf("check:%s:default:%s", asset.Name, checkName)
			sql := phoneStandardSQL(asset.Name, col)
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

		for _, col := range asset.Standards.Currency {
			checkName := fmt.Sprintf("standards_currency_%s", col)
			nodeName := fmt.Sprintf("check:%s:default:%s", asset.Name, checkName)
			sql := currencyStandardSQL(asset.Name, col)
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
