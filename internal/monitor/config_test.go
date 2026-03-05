package monitor

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Andrew-DataDecode/Granicus/internal/config"
)

func writeTestMonitorConfig(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "monitoring.yaml")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadMonitorConfig_FullConfig(t *testing.T) {
	cfg, err := LoadMonitorConfig(writeTestMonitorConfig(t, `
monitoring:
  defaults:
    windows: [day, week, month, year]
    warning_threshold: 0.10
    error_threshold: 0.25

  structural:
    row_counts: true
    null_rates:
      - ent_payment.payment_amount
      - ent_order_line.order_line_price

  metrics:
    - table: ent_payment
      columns:
        - column: payment_amount
          aggregate: median
          warning_threshold: 0.15
        - column: payment_amount
          aggregate: sum

  rates:
    - name: refund_rate
      numerator: { table: ent_refund, aggregate: count }
      denominator: { table: ent_payment, aggregate: count }

  segments:
    - table: ent_order_line
      segment_column: brand_name
      metric: count
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil config")
	}

	m := cfg.Monitoring

	// Defaults
	if len(m.Defaults.Windows) != 4 {
		t.Errorf("expected 4 default windows, got %d", len(m.Defaults.Windows))
	}
	if *m.Defaults.WarningThreshold != 0.10 {
		t.Errorf("warning_threshold: %f", *m.Defaults.WarningThreshold)
	}
	if *m.Defaults.ErrorThreshold != 0.25 {
		t.Errorf("error_threshold: %f", *m.Defaults.ErrorThreshold)
	}

	// Structural
	if !m.Structural.RowCounts {
		t.Error("expected row_counts=true")
	}
	if len(m.Structural.NullRates) != 2 {
		t.Errorf("expected 2 null_rates, got %d", len(m.Structural.NullRates))
	}

	// Metrics
	if len(m.Metrics) != 1 {
		t.Fatalf("expected 1 metric group, got %d", len(m.Metrics))
	}
	if m.Metrics[0].Table != "ent_payment" {
		t.Errorf("metrics[0].table: %q", m.Metrics[0].Table)
	}
	if len(m.Metrics[0].Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(m.Metrics[0].Columns))
	}

	// Rates
	if len(m.Rates) != 1 {
		t.Fatalf("expected 1 rate, got %d", len(m.Rates))
	}
	if m.Rates[0].Name != "refund_rate" {
		t.Errorf("rates[0].name: %q", m.Rates[0].Name)
	}
	if m.Rates[0].Numerator.Table != "ent_refund" {
		t.Errorf("rates[0].numerator.table: %q", m.Rates[0].Numerator.Table)
	}

	// Segments
	if len(m.Segments) != 1 {
		t.Fatalf("expected 1 segment, got %d", len(m.Segments))
	}
	if m.Segments[0].SegmentColumn != "brand_name" {
		t.Errorf("segments[0].segment_column: %q", m.Segments[0].SegmentColumn)
	}
}

func TestLoadMonitorConfig_FileNotExists(t *testing.T) {
	cfg, err := LoadMonitorConfig("/nonexistent/path/monitoring.yaml")
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if cfg != nil {
		t.Error("expected nil config for missing file")
	}
}

func TestLoadMonitorConfig_DefaultThresholds(t *testing.T) {
	cfg, err := LoadMonitorConfig(writeTestMonitorConfig(t, `
monitoring:
  defaults: {}
  metrics:
    - table: orders
      columns:
        - column: total
          aggregate: sum
`))
	if err != nil {
		t.Fatal(err)
	}

	col := cfg.Monitoring.Metrics[0].Columns[0]
	if got := col.ResolvedWarningThreshold(cfg.Monitoring.Defaults); got != 0.10 {
		t.Errorf("expected default warning 0.10, got %f", got)
	}
	if got := col.ResolvedErrorThreshold(cfg.Monitoring.Defaults); got != 0.25 {
		t.Errorf("expected default error 0.25, got %f", got)
	}
}

func TestLoadMonitorConfig_ThresholdOverrides(t *testing.T) {
	cfg, err := LoadMonitorConfig(writeTestMonitorConfig(t, `
monitoring:
  defaults:
    warning_threshold: 0.10
    error_threshold: 0.25
  metrics:
    - table: orders
      columns:
        - column: total
          aggregate: sum
          warning_threshold: 0.05
          error_threshold: 0.50
`))
	if err != nil {
		t.Fatal(err)
	}

	col := cfg.Monitoring.Metrics[0].Columns[0]
	if got := col.ResolvedWarningThreshold(cfg.Monitoring.Defaults); got != 0.05 {
		t.Errorf("expected overridden warning 0.05, got %f", got)
	}
	if got := col.ResolvedErrorThreshold(cfg.Monitoring.Defaults); got != 0.50 {
		t.Errorf("expected overridden error 0.50, got %f", got)
	}
}

func TestLoadMonitorConfig_DefaultWindows(t *testing.T) {
	cfg, err := LoadMonitorConfig(writeTestMonitorConfig(t, `
monitoring:
  defaults: {}
  metrics:
    - table: orders
      columns:
        - column: total
          aggregate: sum
`))
	if err != nil {
		t.Fatal(err)
	}

	col := cfg.Monitoring.Metrics[0].Columns[0]
	windows := col.ResolvedWindows(cfg.Monitoring.Defaults)
	if len(windows) != 4 {
		t.Errorf("expected 4 default windows, got %v", windows)
	}
	expected := []string{"day", "week", "month", "year"}
	for i, w := range expected {
		if windows[i] != w {
			t.Errorf("window[%d]: expected %q, got %q", i, w, windows[i])
		}
	}
}

func TestLoadMonitorConfig_WindowOverrides(t *testing.T) {
	cfg, err := LoadMonitorConfig(writeTestMonitorConfig(t, `
monitoring:
  defaults:
    windows: [day, week, month, year]
  metrics:
    - table: orders
      columns:
        - column: total
          aggregate: sum
          windows: [day, week]
`))
	if err != nil {
		t.Fatal(err)
	}

	col := cfg.Monitoring.Metrics[0].Columns[0]
	windows := col.ResolvedWindows(cfg.Monitoring.Defaults)
	if len(windows) != 2 || windows[0] != "day" || windows[1] != "week" {
		t.Errorf("expected [day, week], got %v", windows)
	}
}

func TestLoadMonitorConfig_CustomDefaultWindows(t *testing.T) {
	cfg, err := LoadMonitorConfig(writeTestMonitorConfig(t, `
monitoring:
  defaults:
    windows: [day, month]
  metrics:
    - table: orders
      columns:
        - column: total
          aggregate: sum
`))
	if err != nil {
		t.Fatal(err)
	}

	col := cfg.Monitoring.Metrics[0].Columns[0]
	windows := col.ResolvedWindows(cfg.Monitoring.Defaults)
	if len(windows) != 2 || windows[0] != "day" || windows[1] != "month" {
		t.Errorf("expected [day, month], got %v", windows)
	}
}

func TestLoadMonitorConfig_InvalidWindow(t *testing.T) {
	_, err := LoadMonitorConfig(writeTestMonitorConfig(t, `
monitoring:
  defaults:
    windows: [day, hourly]
`))
	if err == nil {
		t.Error("expected error for invalid window")
	}
}

func TestLoadMonitorConfig_InvalidAggregate(t *testing.T) {
	_, err := LoadMonitorConfig(writeTestMonitorConfig(t, `
monitoring:
  metrics:
    - table: orders
      columns:
        - column: total
          aggregate: percentile
`))
	if err == nil {
		t.Error("expected error for invalid aggregate")
	}
}

func TestLoadMonitorConfig_MissingMetricTable(t *testing.T) {
	_, err := LoadMonitorConfig(writeTestMonitorConfig(t, `
monitoring:
  metrics:
    - columns:
        - column: total
          aggregate: sum
`))
	if err == nil {
		t.Error("expected error for missing metric table")
	}
}

func TestLoadMonitorConfig_MissingMetricColumns(t *testing.T) {
	_, err := LoadMonitorConfig(writeTestMonitorConfig(t, `
monitoring:
  metrics:
    - table: orders
      columns: []
`))
	if err == nil {
		t.Error("expected error for empty columns")
	}
}

func TestLoadMonitorConfig_MissingRateName(t *testing.T) {
	_, err := LoadMonitorConfig(writeTestMonitorConfig(t, `
monitoring:
  rates:
    - numerator: { table: refunds, aggregate: count }
      denominator: { table: payments, aggregate: count }
`))
	if err == nil {
		t.Error("expected error for missing rate name")
	}
}

func TestLoadMonitorConfig_MissingRateNumeratorTable(t *testing.T) {
	_, err := LoadMonitorConfig(writeTestMonitorConfig(t, `
monitoring:
  rates:
    - name: refund_rate
      numerator: { aggregate: count }
      denominator: { table: payments, aggregate: count }
`))
	if err == nil {
		t.Error("expected error for missing numerator table")
	}
}

func TestLoadMonitorConfig_MissingSegmentColumn(t *testing.T) {
	_, err := LoadMonitorConfig(writeTestMonitorConfig(t, `
monitoring:
  segments:
    - table: orders
      metric: count
`))
	if err == nil {
		t.Error("expected error for missing segment_column")
	}
}

func TestLoadMonitorConfig_MissingSegmentMetric(t *testing.T) {
	_, err := LoadMonitorConfig(writeTestMonitorConfig(t, `
monitoring:
  segments:
    - table: orders
      segment_column: brand
`))
	if err == nil {
		t.Error("expected error for missing segment metric")
	}
}

func TestLoadMonitorConfig_InvalidNullRateFormat(t *testing.T) {
	_, err := LoadMonitorConfig(writeTestMonitorConfig(t, `
monitoring:
  structural:
    null_rates:
      - payment_amount
`))
	if err == nil {
		t.Error("expected error for null_rate without table.column format")
	}
}

func TestLoadMonitorConfig_NegativeThreshold(t *testing.T) {
	_, err := LoadMonitorConfig(writeTestMonitorConfig(t, `
monitoring:
  defaults:
    warning_threshold: -0.1
`))
	if err == nil {
		t.Error("expected error for negative warning_threshold")
	}
}

func TestLoadMonitorConfig_InvalidYAML(t *testing.T) {
	_, err := LoadMonitorConfig(writeTestMonitorConfig(t, `
monitoring:
  defaults: [invalid
`))
	if err == nil {
		t.Error("expected error for invalid YAML")
	}
}

func TestLoadMonitorConfig_EmptyFile(t *testing.T) {
	cfg, err := LoadMonitorConfig(writeTestMonitorConfig(t, ``))
	if err != nil {
		t.Fatal(err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil config for empty file")
	}
	if cfg.Monitoring.Structural != nil {
		t.Error("expected nil structural for empty config")
	}
}

func TestLoadMonitorConfig_MinimalStructural(t *testing.T) {
	cfg, err := LoadMonitorConfig(writeTestMonitorConfig(t, `
monitoring:
  structural:
    row_counts: true
`))
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Monitoring.Structural.RowCounts {
		t.Error("expected row_counts=true")
	}
	if len(cfg.Monitoring.Structural.NullRates) != 0 {
		t.Error("expected empty null_rates")
	}
}

func TestMonitorConfig_ValidatePipeline(t *testing.T) {
	pipeline := &config.PipelineConfig{
		Pipeline: "test",
		Assets: []config.AssetConfig{
			{Name: "ent_payment"},
			{Name: "ent_order_line"},
			{Name: "ent_refund"},
		},
	}

	cfg, err := LoadMonitorConfig(writeTestMonitorConfig(t, `
monitoring:
  structural:
    null_rates:
      - ent_payment.payment_amount

  metrics:
    - table: ent_payment
      columns:
        - column: payment_amount
          aggregate: sum

  rates:
    - name: refund_rate
      numerator: { table: ent_refund, aggregate: count }
      denominator: { table: ent_payment, aggregate: count }

  segments:
    - table: ent_order_line
      segment_column: brand_name
      metric: count
`))
	if err != nil {
		t.Fatal(err)
	}

	errs := cfg.Validate(pipeline)
	if len(errs) != 0 {
		t.Errorf("expected no validation errors, got: %v", errs)
	}
}

func TestMonitorConfig_ValidatePipelineMissingTables(t *testing.T) {
	pipeline := &config.PipelineConfig{
		Pipeline: "test",
		Assets: []config.AssetConfig{
			{Name: "ent_payment"},
		},
	}

	cfg, err := LoadMonitorConfig(writeTestMonitorConfig(t, `
monitoring:
  structural:
    null_rates:
      - missing_table.col

  metrics:
    - table: nonexistent
      columns:
        - column: val
          aggregate: sum

  rates:
    - name: bad_rate
      numerator: { table: no_table, aggregate: count }
      denominator: { table: ent_payment, aggregate: count }

  segments:
    - table: gone_table
      segment_column: brand
      metric: count
`))
	if err != nil {
		t.Fatal(err)
	}

	errs := cfg.Validate(pipeline)
	if len(errs) != 4 {
		t.Errorf("expected 4 validation errors, got %d: %v", len(errs), errs)
	}
}

func TestMonitorConfig_ValidatePipelineWithSources(t *testing.T) {
	pipeline := &config.PipelineConfig{
		Pipeline: "test",
		Assets:   []config.AssetConfig{{Name: "orders"}},
		Sources: map[string]config.SourceConfig{
			"raw_orders": {Identifier: "project.dataset.raw_orders"},
		},
	}

	cfg, err := LoadMonitorConfig(writeTestMonitorConfig(t, `
monitoring:
  structural:
    null_rates:
      - raw_orders.order_id
`))
	if err != nil {
		t.Fatal(err)
	}

	errs := cfg.Validate(pipeline)
	if len(errs) != 0 {
		t.Errorf("sources should be valid table refs, got: %v", errs)
	}
}

func TestRateConfig_ResolvedThresholds(t *testing.T) {
	wt := 0.20
	et := 0.40
	rc := RateConfig{
		Name:             "test",
		WarningThreshold: &wt,
		ErrorThreshold:   &et,
	}
	defaults := DefaultsConfig{}

	if got := rc.ResolvedWarningThreshold(defaults); got != 0.20 {
		t.Errorf("expected 0.20, got %f", got)
	}
	if got := rc.ResolvedErrorThreshold(defaults); got != 0.40 {
		t.Errorf("expected 0.40, got %f", got)
	}
}

func TestRateConfig_ResolvedWindows(t *testing.T) {
	rc := RateConfig{
		Name:    "test",
		Windows: []string{"day", "year"},
	}
	defaults := DefaultsConfig{}

	windows := rc.ResolvedWindows(defaults)
	if len(windows) != 2 || windows[0] != "day" || windows[1] != "year" {
		t.Errorf("expected [day, year], got %v", windows)
	}
}

func TestSegmentConfig_ResolvedThresholds(t *testing.T) {
	wt := 0.30
	sc := SegmentConfig{
		Table:            "orders",
		SegmentColumn:    "brand",
		Metric:           "count",
		WarningThreshold: &wt,
	}
	defaults := DefaultsConfig{}

	if got := sc.ResolvedWarningThreshold(defaults); got != 0.30 {
		t.Errorf("expected 0.30, got %f", got)
	}
	if got := sc.ResolvedErrorThreshold(defaults); got != DefaultErrorThreshold {
		t.Errorf("expected default %f, got %f", DefaultErrorThreshold, got)
	}
}

func TestSegmentConfig_ResolvedWindows(t *testing.T) {
	sc := SegmentConfig{
		Table:         "orders",
		SegmentColumn: "brand",
		Metric:        "count",
	}
	dw := []string{"day", "week"}
	defaults := DefaultsConfig{Windows: dw}

	windows := sc.ResolvedWindows(defaults)
	if len(windows) != 2 || windows[0] != "day" || windows[1] != "week" {
		t.Errorf("expected [day, week], got %v", windows)
	}
}

func TestLoadMonitorConfig_InvalidRateAggregate(t *testing.T) {
	_, err := LoadMonitorConfig(writeTestMonitorConfig(t, `
monitoring:
  rates:
    - name: bad
      numerator: { table: a, aggregate: bogus }
      denominator: { table: b, aggregate: count }
`))
	if err == nil {
		t.Error("expected error for invalid rate numerator aggregate")
	}
}

func TestLoadMonitorConfig_InvalidSegmentMetric(t *testing.T) {
	_, err := LoadMonitorConfig(writeTestMonitorConfig(t, `
monitoring:
  segments:
    - table: orders
      segment_column: brand
      metric: bogus
`))
	if err == nil {
		t.Error("expected error for invalid segment metric")
	}
}

func TestLoadMonitorConfig_InvalidColumnWindow(t *testing.T) {
	_, err := LoadMonitorConfig(writeTestMonitorConfig(t, `
monitoring:
  metrics:
    - table: orders
      columns:
        - column: total
          aggregate: sum
          windows: [day, hourly]
`))
	if err == nil {
		t.Error("expected error for invalid column window")
	}
}
