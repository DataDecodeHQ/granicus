package monitor

import (
	"testing"
)

func TestAggregateExpr_Sum(t *testing.T) {
	got := aggregateExpr("total", "sum")
	want := "SUM(`total`)"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestAggregateExpr_Avg(t *testing.T) {
	got := aggregateExpr("price", "avg")
	want := "AVG(`price`)"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestAggregateExpr_Count(t *testing.T) {
	got := aggregateExpr("id", "count")
	want := "COUNT(`id`)"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestAggregateExpr_Min(t *testing.T) {
	got := aggregateExpr("created_at", "min")
	want := "MIN(`created_at`)"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestAggregateExpr_Max(t *testing.T) {
	got := aggregateExpr("amount", "max")
	want := "MAX(`amount`)"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestAggregateExpr_Median(t *testing.T) {
	got := aggregateExpr("revenue", "median")
	want := "APPROX_QUANTILES(`revenue`, 2)[OFFSET(1)]"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestAggregateExpr_Default(t *testing.T) {
	got := aggregateExpr("col", "unknown")
	want := "COUNT(`col`)"
	if got != want {
		t.Errorf("default should fallback to COUNT, got %q", got)
	}
}

func TestAggregateKey(t *testing.T) {
	got := aggregateKey("total", "sum")
	want := "sum_total"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestScalarAggregateExpr_Count(t *testing.T) {
	got := scalarAggregateExpr("count")
	want := "COUNT(*)"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestScalarAggregateExpr_Default(t *testing.T) {
	got := scalarAggregateExpr("anything")
	want := "COUNT(*)"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestSegmentMetricExpr_Count(t *testing.T) {
	got := segmentMetricExpr("count")
	want := "COUNT(*)"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestSegmentMetricExpr_Default(t *testing.T) {
	got := segmentMetricExpr("anything")
	want := "COUNT(*)"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestToFloat64_Int64(t *testing.T) {
	got := toFloat64(int64(42))
	if got != 42.0 {
		t.Errorf("got %f, want 42.0", got)
	}
}

func TestToFloat64_Float64(t *testing.T) {
	got := toFloat64(float64(3.14))
	if got != 3.14 {
		t.Errorf("got %f, want 3.14", got)
	}
}

func TestToFloat64_Int(t *testing.T) {
	got := toFloat64(int(7))
	if got != 7.0 {
		t.Errorf("got %f, want 7.0", got)
	}
}

func TestToFloat64_Nil(t *testing.T) {
	got := toFloat64(nil)
	if got != 0 {
		t.Errorf("got %f, want 0 for nil", got)
	}
}

func TestToFloat64_String(t *testing.T) {
	got := toFloat64("not a number")
	if got != 0 {
		t.Errorf("got %f, want 0 for string", got)
	}
}

func TestCollectAggregates_EmptyConfig(t *testing.T) {
	cfg := &MonitorConfig{}
	snapshots := collectAggregates(nil, nil, cfg, "test_pipe", "proj", map[string]string{}, "2026-02-26T00:00:00Z")
	if len(snapshots) != 0 {
		t.Errorf("expected 0 snapshots for empty config, got %d", len(snapshots))
	}
}

func TestCollectRates_EmptyConfig(t *testing.T) {
	cfg := &MonitorConfig{}
	snapshots := collectRates(nil, nil, cfg, "test_pipe", "proj", map[string]string{}, "2026-02-26T00:00:00Z")
	if len(snapshots) != 0 {
		t.Errorf("expected 0 snapshots for empty config, got %d", len(snapshots))
	}
}

func TestCollectSegments_EmptyConfig(t *testing.T) {
	cfg := &MonitorConfig{}
	snapshots := collectSegments(nil, nil, cfg, "test_pipe", "proj", map[string]string{}, "2026-02-26T00:00:00Z")
	if len(snapshots) != 0 {
		t.Errorf("expected 0 snapshots for empty config, got %d", len(snapshots))
	}
}

func TestCollectBusinessMetrics_EmptyConfig(t *testing.T) {
	cfg := &MonitorConfig{}
	snapshots := CollectBusinessMetrics(nil, nil, cfg, "test_pipe", "proj", map[string]string{})
	if len(snapshots) != 0 {
		t.Errorf("expected 0 snapshots for empty config, got %d", len(snapshots))
	}
}

func TestRunAggregateQuery_BuildsCorrectSQL(t *testing.T) {
	mc := MetricConfig{
		Table: "orders",
		Columns: []ColumnMetric{
			{Column: "total", Aggregate: "sum"},
			{Column: "quantity", Aggregate: "median"},
		},
	}

	var selects []string
	for _, col := range mc.Columns {
		expr := aggregateExpr(col.Column, col.Aggregate)
		alias := aggregateKey(col.Column, col.Aggregate)
		selects = append(selects, expr+" AS `"+alias+"`")
	}

	if len(selects) != 2 {
		t.Fatalf("expected 2 selects, got %d", len(selects))
	}
	if selects[0] != "SUM(`total`) AS `sum_total`" {
		t.Errorf("select[0]: got %q", selects[0])
	}
	if selects[1] != "APPROX_QUANTILES(`quantity`, 2)[OFFSET(1)] AS `median_quantity`" {
		t.Errorf("select[1]: got %q", selects[1])
	}
}

func TestSegmentQueryFormat(t *testing.T) {
	sc := SegmentConfig{
		Table:         "orders",
		SegmentColumn: "state",
		Metric:        "count",
	}

	expr := segmentMetricExpr(sc.Metric)
	if expr != "COUNT(*)" {
		t.Errorf("expected COUNT(*), got %q", expr)
	}
}
