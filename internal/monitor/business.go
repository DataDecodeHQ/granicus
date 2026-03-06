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

const businessQueryTimeout = 120 * time.Second

func CollectBusinessMetrics(ctx context.Context, bq BQQuerier, cfg *MonitorConfig, pipeline, project string, tables map[string]string) []MetricSnapshot {
	now := time.Now().UTC().Format(time.RFC3339)
	var all []MetricSnapshot

	all = append(all, collectAggregates(ctx, bq, cfg, pipeline, project, tables, now)...)
	all = append(all, collectRates(ctx, bq, cfg, pipeline, project, tables, now)...)
	all = append(all, collectSegments(ctx, bq, cfg, pipeline, project, tables, now)...)

	return all
}

func collectAggregates(ctx context.Context, bq BQQuerier, cfg *MonitorConfig, pipeline, project string, tables map[string]string, now string) []MetricSnapshot {
	var snapshots []MetricSnapshot

	for _, mc := range cfg.Monitoring.Metrics {
		dataset, ok := tables[mc.Table]
		if !ok {
			slog.Warn("aggregate table not found in tables map", "table", mc.Table)
			continue
		}
		results, err := runAggregateQuery(ctx, bq, mc, project, dataset)
		if err != nil {
			slog.Warn("aggregate query failed", "table", mc.Table, "error", err)
			continue
		}

		for _, col := range mc.Columns {
			key := aggregateKey(col.Column, col.Aggregate)
			if val, ok := results[key]; ok {
				snapshots = append(snapshots, MetricSnapshot{
					Pipeline:    pipeline,
					TableName:   mc.Table,
					ColumnName:  col.Column,
					MetricName:  col.Aggregate,
					MetricValue: val,
					CapturedAt:  now,
				})
			}
		}
	}

	return snapshots
}

func runAggregateQuery(ctx context.Context, bq BQQuerier, mc MetricConfig, project, dataset string) (map[string]float64, error) {
	var selects []string
	for _, col := range mc.Columns {
		expr := aggregateExpr(col.Column, col.Aggregate)
		alias := aggregateKey(col.Column, col.Aggregate)
		selects = append(selects, fmt.Sprintf("%s AS `%s`", expr, alias))
	}

	sql := fmt.Sprintf("SELECT %s FROM `%s.%s.%s`",
		strings.Join(selects, ", "), project, dataset, mc.Table)

	qctx, cancel := context.WithTimeout(ctx, businessQueryTimeout)
	defer cancel()

	q := bq.Query(sql)
	it, err := q.Read(qctx)
	if err != nil {
		return nil, fmt.Errorf("executing query: %w", err)
	}

	var row []bigquery.Value
	if err := it.Next(&row); err != nil {
		return nil, fmt.Errorf("reading result: %w", err)
	}

	results := make(map[string]float64, len(mc.Columns))
	for i, col := range mc.Columns {
		if i < len(row) {
			results[aggregateKey(col.Column, col.Aggregate)] = toFloat64(row[i])
		}
	}

	return results, nil
}

func aggregateExpr(column, aggregate string) string {
	switch aggregate {
	case "sum":
		return fmt.Sprintf("SUM(`%s`)", column)
	case "avg":
		return fmt.Sprintf("AVG(`%s`)", column)
	case "count":
		return fmt.Sprintf("COUNT(`%s`)", column)
	case "min":
		return fmt.Sprintf("MIN(`%s`)", column)
	case "max":
		return fmt.Sprintf("MAX(`%s`)", column)
	case "median":
		return fmt.Sprintf("APPROX_QUANTILES(`%s`, 2)[OFFSET(1)]", column)
	default:
		return fmt.Sprintf("COUNT(`%s`)", column)
	}
}

func aggregateKey(column, aggregate string) string {
	return aggregate + "_" + column
}

func collectRates(ctx context.Context, bq BQQuerier, cfg *MonitorConfig, pipeline, project string, tables map[string]string, now string) []MetricSnapshot {
	var snapshots []MetricSnapshot

	for _, rc := range cfg.Monitoring.Rates {
		numDS, ok := tables[rc.Numerator.Table]
		if !ok {
			slog.Warn("rate numerator table not found in tables map", "rate", rc.Name, "table", rc.Numerator.Table)
			continue
		}
		numVal, err := runScalarAggregate(ctx, bq, rc.Numerator.Table, rc.Numerator.Aggregate, project, numDS)
		if err != nil {
			slog.Warn("rate numerator query failed", "rate", rc.Name, "error", err)
			continue
		}

		denDS, ok := tables[rc.Denominator.Table]
		if !ok {
			slog.Warn("rate denominator table not found in tables map", "rate", rc.Name, "table", rc.Denominator.Table)
			continue
		}
		denVal, err := runScalarAggregate(ctx, bq, rc.Denominator.Table, rc.Denominator.Aggregate, project, denDS)
		if err != nil {
			slog.Warn("rate denominator query failed", "rate", rc.Name, "error", err)
			continue
		}

		var ratio float64
		if denVal != 0 {
			ratio = numVal / denVal
		}

		snapshots = append(snapshots, MetricSnapshot{
			Pipeline:    pipeline,
			TableName:   rc.Numerator.Table,
			ColumnName:  rc.Name,
			MetricName:  "rate",
			MetricValue: ratio,
			CapturedAt:  now,
		})
	}

	return snapshots
}

func runScalarAggregate(ctx context.Context, bq BQQuerier, table, aggregate, project, dataset string) (float64, error) {
	expr := scalarAggregateExpr(aggregate)
	sql := fmt.Sprintf("SELECT %s AS val FROM `%s.%s.%s`", expr, project, dataset, table)

	qctx, cancel := context.WithTimeout(ctx, businessQueryTimeout)
	defer cancel()

	q := bq.Query(sql)
	it, err := q.Read(qctx)
	if err != nil {
		return 0, fmt.Errorf("executing query: %w", err)
	}

	var row []bigquery.Value
	if err := it.Next(&row); err != nil {
		return 0, fmt.Errorf("reading result: %w", err)
	}

	if len(row) == 0 {
		return 0, nil
	}

	return toFloat64(row[0]), nil
}

func scalarAggregateExpr(aggregate string) string {
	switch aggregate {
	case "count":
		return "COUNT(*)"
	default:
		return "COUNT(*)"
	}
}

func collectSegments(ctx context.Context, bq BQQuerier, cfg *MonitorConfig, pipeline, project string, tables map[string]string, now string) []MetricSnapshot {
	var snapshots []MetricSnapshot

	for _, sc := range cfg.Monitoring.Segments {
		dataset, ok := tables[sc.Table]
		if !ok {
			slog.Warn("segment table not found in tables map", "table", sc.Table)
			continue
		}
		rows, err := runSegmentQuery(ctx, bq, sc, project, dataset)
		if err != nil {
			slog.Warn("segment query failed", "table", sc.Table, "error", err)
			continue
		}

		for segValue, metricValue := range rows {
			snapshots = append(snapshots, MetricSnapshot{
				Pipeline:     pipeline,
				TableName:    sc.Table,
				ColumnName:   sc.SegmentColumn,
				MetricName:   sc.Metric,
				MetricValue:  metricValue,
				SegmentValue: fmt.Sprintf("%s=%s", sc.SegmentColumn, segValue),
				CapturedAt:   now,
			})
		}
	}

	return snapshots
}

func runSegmentQuery(ctx context.Context, bq BQQuerier, sc SegmentConfig, project, dataset string) (map[string]float64, error) {
	expr := segmentMetricExpr(sc.Metric)
	sql := fmt.Sprintf("SELECT CAST(`%s` AS STRING) AS seg, %s AS val FROM `%s.%s.%s` GROUP BY `%s`",
		sc.SegmentColumn, expr, project, dataset, sc.Table, sc.SegmentColumn)

	qctx, cancel := context.WithTimeout(ctx, businessQueryTimeout)
	defer cancel()

	q := bq.Query(sql)
	it, err := q.Read(qctx)
	if err != nil {
		return nil, fmt.Errorf("executing query: %w", err)
	}

	results := make(map[string]float64)
	for {
		var row []bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return results, fmt.Errorf("reading segment row: %w", err)
		}
		if len(row) >= 2 {
			segVal := fmt.Sprintf("%v", row[0])
			results[segVal] = toFloat64(row[1])
		}
	}

	return results, nil
}

func segmentMetricExpr(metric string) string {
	switch metric {
	case "count":
		return "COUNT(*)"
	default:
		return "COUNT(*)"
	}
}

func toFloat64(v bigquery.Value) float64 {
	switch val := v.(type) {
	case int64:
		return float64(val)
	case float64:
		return val
	case int:
		return float64(val)
	default:
		return 0
	}
}
