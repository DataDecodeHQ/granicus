package monitor

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
)

const structuralQueryTimeout = 60 * time.Second

type BQQuerier interface {
	Query(sql string) *bigquery.Query
}

type tableMetricPlan struct {
	Table    string
	Dataset  string
	RowCount bool
	NullCols []string
}

func CollectStructuralMetrics(client BQQuerier, cfg *StructuralConfig, pipeline string, tables map[string]string, capturedAt time.Time) []MetricSnapshot {
	if cfg == nil {
		return nil
	}

	plans := buildPlans(cfg, tables)
	if len(plans) == 0 {
		return nil
	}

	ts := capturedAt.UTC().Format(time.RFC3339)
	var snapshots []MetricSnapshot

	for _, plan := range plans {
		results, err := executeTableQuery(client, plan)
		if err != nil {
			slog.Warn("structural query failed", "dataset", plan.Dataset, "table", plan.Table, "error", err)
			continue
		}

		if plan.RowCount {
			if v, ok := results["row_count"]; ok {
				snapshots = append(snapshots, MetricSnapshot{
					Pipeline:    pipeline,
					TableName:   plan.Table,
					ColumnName:  "",
					MetricName:  "row_count",
					MetricValue: v,
					CapturedAt:  ts,
				})
			}
		}

		for _, col := range plan.NullCols {
			key := col + "_null_rate"
			if v, ok := results[key]; ok {
				snapshots = append(snapshots, MetricSnapshot{
					Pipeline:    pipeline,
					TableName:   plan.Table,
					ColumnName:  col,
					MetricName:  "null_rate",
					MetricValue: v,
					CapturedAt:  ts,
				})
			}
		}
	}

	return snapshots
}

func buildPlans(cfg *StructuralConfig, tables map[string]string) []tableMetricPlan {
	planMap := make(map[string]*tableMetricPlan)

	if cfg.RowCounts {
		for table, dataset := range tables {
			planMap[table] = &tableMetricPlan{
				Table:    table,
				Dataset:  dataset,
				RowCount: true,
			}
		}
	}

	for _, nr := range cfg.NullRates {
		table, col, _ := strings.Cut(nr, ".")
		dataset, ok := tables[table]
		if !ok {
			slog.Warn("null_rate table not found in tables map", "table", table)
			continue
		}

		plan, exists := planMap[table]
		if !exists {
			plan = &tableMetricPlan{
				Table:   table,
				Dataset: dataset,
			}
			planMap[table] = plan
		}
		plan.NullCols = append(plan.NullCols, col)
	}

	plans := make([]tableMetricPlan, 0, len(planMap))
	for _, p := range planMap {
		plans = append(plans, *p)
	}
	return plans
}

func buildQuery(plan tableMetricPlan) string {
	var selects []string

	if plan.RowCount {
		selects = append(selects, "COUNT(*) AS row_count")
	}

	for _, col := range plan.NullCols {
		selects = append(selects, fmt.Sprintf("SAFE_DIVIDE(COUNTIF(`%s` IS NULL), COUNT(*)) AS %s_null_rate", col, col))
	}

	return fmt.Sprintf("SELECT %s FROM `%s.%s`", strings.Join(selects, ", "), plan.Dataset, plan.Table)
}

func executeTableQuery(client BQQuerier, plan tableMetricPlan) (map[string]float64, error) {
	sql := buildQuery(plan)

	ctx, cancel := context.WithTimeout(context.Background(), structuralQueryTimeout)
	defer cancel()

	q := client.Query(sql)
	it, err := q.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("running query: %w", err)
	}

	var row map[string]bigquery.Value
	err = it.Next(&row)
	if err == iterator.Done {
		return nil, fmt.Errorf("no rows returned")
	}
	if err != nil {
		return nil, fmt.Errorf("reading row: %w", err)
	}

	results := make(map[string]float64, len(row))
	for k, v := range row {
		switch n := v.(type) {
		case int64:
			results[k] = float64(n)
		case float64:
			results[k] = n
		case nil:
			results[k] = 0
		default:
			slog.Warn("unexpected column type", "type", fmt.Sprintf("%T", v), "column", k)
		}
	}

	return results, nil
}
