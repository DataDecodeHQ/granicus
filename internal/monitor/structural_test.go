package monitor

import (
	"sort"
	"testing"
	"time"
)

func TestBuildPlans_RowCountsOnly(t *testing.T) {
	cfg := &StructuralConfig{RowCounts: true}
	tables := map[string]string{
		"stg_orders": "dev_analytics",
		"stg_users":  "dev_analytics",
	}

	plans := buildPlans(cfg, tables)
	if len(plans) != 2 {
		t.Fatalf("expected 2 plans, got %d", len(plans))
	}

	for _, p := range plans {
		if !p.RowCount {
			t.Errorf("expected RowCount=true for %s", p.Table)
		}
		if len(p.NullCols) != 0 {
			t.Errorf("expected no NullCols for %s, got %v", p.Table, p.NullCols)
		}
	}
}

func TestBuildPlans_NullRatesOnly(t *testing.T) {
	cfg := &StructuralConfig{
		NullRates: []string{"stg_orders.email", "stg_orders.phone"},
	}
	tables := map[string]string{
		"stg_orders": "dev_analytics",
	}

	plans := buildPlans(cfg, tables)
	if len(plans) != 1 {
		t.Fatalf("expected 1 plan, got %d", len(plans))
	}

	p := plans[0]
	if p.Table != "stg_orders" {
		t.Errorf("expected table stg_orders, got %s", p.Table)
	}
	if p.RowCount {
		t.Error("expected RowCount=false")
	}
	sort.Strings(p.NullCols)
	if len(p.NullCols) != 2 || p.NullCols[0] != "email" || p.NullCols[1] != "phone" {
		t.Errorf("expected NullCols=[email, phone], got %v", p.NullCols)
	}
}

func TestBuildPlans_Combined(t *testing.T) {
	cfg := &StructuralConfig{
		RowCounts: true,
		NullRates: []string{"stg_orders.email"},
	}
	tables := map[string]string{
		"stg_orders": "dev_analytics",
		"stg_users":  "dev_analytics",
	}

	plans := buildPlans(cfg, tables)
	if len(plans) != 2 {
		t.Fatalf("expected 2 plans, got %d", len(plans))
	}

	planMap := make(map[string]tableMetricPlan)
	for _, p := range plans {
		planMap[p.Table] = p
	}

	op := planMap["stg_orders"]
	if !op.RowCount {
		t.Error("stg_orders should have RowCount=true")
	}
	if len(op.NullCols) != 1 || op.NullCols[0] != "email" {
		t.Errorf("stg_orders NullCols: expected [email], got %v", op.NullCols)
	}

	up := planMap["stg_users"]
	if !up.RowCount {
		t.Error("stg_users should have RowCount=true")
	}
	if len(up.NullCols) != 0 {
		t.Errorf("stg_users NullCols: expected [], got %v", up.NullCols)
	}
}

func TestBuildPlans_NullRateMissingTable(t *testing.T) {
	cfg := &StructuralConfig{
		NullRates: []string{"missing_table.col"},
	}
	tables := map[string]string{
		"stg_orders": "dev_analytics",
	}

	plans := buildPlans(cfg, tables)
	if len(plans) != 0 {
		t.Errorf("expected 0 plans for missing table, got %d", len(plans))
	}
}

func TestBuildPlans_Empty(t *testing.T) {
	cfg := &StructuralConfig{}
	tables := map[string]string{"t": "ds"}

	plans := buildPlans(cfg, tables)
	if len(plans) != 0 {
		t.Errorf("expected 0 plans, got %d", len(plans))
	}
}

func TestBuildQuery_RowCountOnly(t *testing.T) {
	plan := tableMetricPlan{
		Table:    "stg_orders",
		Dataset:  "dev_analytics",
		RowCount: true,
	}

	got := buildQuery(plan)
	expected := "SELECT COUNT(*) AS row_count FROM `dev_analytics.stg_orders`"
	if got != expected {
		t.Errorf("expected:\n  %s\ngot:\n  %s", expected, got)
	}
}

func TestBuildQuery_NullRateOnly(t *testing.T) {
	plan := tableMetricPlan{
		Table:    "stg_orders",
		Dataset:  "dev_analytics",
		NullCols: []string{"email"},
	}

	got := buildQuery(plan)
	expected := "SELECT SAFE_DIVIDE(COUNTIF(`email` IS NULL), COUNT(*)) AS email_null_rate FROM `dev_analytics.stg_orders`"
	if got != expected {
		t.Errorf("expected:\n  %s\ngot:\n  %s", expected, got)
	}
}

func TestBuildQuery_Combined(t *testing.T) {
	plan := tableMetricPlan{
		Table:    "stg_orders",
		Dataset:  "dev_analytics",
		RowCount: true,
		NullCols: []string{"email", "phone"},
	}

	got := buildQuery(plan)
	expected := "SELECT COUNT(*) AS row_count, SAFE_DIVIDE(COUNTIF(`email` IS NULL), COUNT(*)) AS email_null_rate, SAFE_DIVIDE(COUNTIF(`phone` IS NULL), COUNT(*)) AS phone_null_rate FROM `dev_analytics.stg_orders`"
	if got != expected {
		t.Errorf("expected:\n  %s\ngot:\n  %s", expected, got)
	}
}

func TestCollectStructuralMetrics_NilConfig(t *testing.T) {
	snapshots := CollectStructuralMetrics(nil, nil, "test", nil, time.Now())
	if snapshots != nil {
		t.Errorf("expected nil for nil config, got %v", snapshots)
	}
}

func TestCollectStructuralMetrics_EmptyConfig(t *testing.T) {
	cfg := &StructuralConfig{}
	snapshots := CollectStructuralMetrics(nil, cfg, "test", map[string]string{"t": "ds"}, time.Now())
	if snapshots != nil {
		t.Errorf("expected nil for empty config, got %v", snapshots)
	}
}
