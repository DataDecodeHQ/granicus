package monitor

import (
	"database/sql"
	"math"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func setupTestDB(t *testing.T, snapshots []MetricSnapshot) (*sql.DB, string) {
	t.Helper()
	dbPath := tempDBPath(t)
	if err := AppendSnapshots(dbPath, snapshots); err != nil {
		t.Fatalf("seeding snapshots: %v", err)
	}
	db, err := openDB(dbPath)
	if err != nil {
		t.Fatalf("opening test db: %v", err)
	}
	return db, dbPath
}

func ptrFloat(f float64) *float64 { return &f }

func TestCompareSnapshots_NoHistory(t *testing.T) {
	dbPath := tempDBPath(t)
	if err := AppendSnapshots(dbPath, nil); err != nil {
		t.Fatalf("init db: %v", err)
	}

	cfg := &MonitorConfig{
		Monitoring: MonitoringBlock{
			Metrics: []MetricConfig{
				{Table: "orders", Columns: []ColumnMetric{{Column: "total", Aggregate: "sum"}}},
			},
		},
	}

	current := []MetricSnapshot{
		{Pipeline: "p", TableName: "orders", ColumnName: "total", MetricName: "sum", MetricValue: 100, CapturedAt: "2026-02-26T12:00:00Z"},
	}

	flags, err := CompareSnapshots(dbPath, cfg, current)
	if err != nil {
		t.Fatalf("CompareSnapshots: %v", err)
	}
	if len(flags) != 0 {
		t.Errorf("expected no flags with no history, got %d", len(flags))
	}
}

func TestCompareSnapshots_SingleDataPoint_NoFlag(t *testing.T) {
	now := time.Date(2026, 2, 26, 12, 0, 0, 0, time.UTC)
	yesterday := now.AddDate(0, 0, -1)

	historical := []MetricSnapshot{
		{Pipeline: "p", TableName: "orders", ColumnName: "total", MetricName: "sum", MetricValue: 100, CapturedAt: yesterday.Format(time.RFC3339)},
	}

	db, _ := setupTestDB(t, historical)
	defer db.Close()

	cfg := &MonitorConfig{
		Monitoring: MonitoringBlock{
			Defaults: DefaultsConfig{Windows: []string{"day"}},
			Metrics: []MetricConfig{
				{Table: "orders", Columns: []ColumnMetric{{Column: "total", Aggregate: "sum"}}},
			},
		},
	}

	current := []MetricSnapshot{
		{Pipeline: "p", TableName: "orders", ColumnName: "total", MetricName: "sum", MetricValue: 200, CapturedAt: now.Format(time.RFC3339)},
	}

	flags, err := compareSnapshotsWithDB(db, cfg, current, now)
	if err != nil {
		t.Fatalf("compareSnapshotsWithDB: %v", err)
	}
	if len(flags) != 0 {
		t.Errorf("expected no flags with only 1 prior data point, got %d", len(flags))
	}
}

func TestCompareSnapshots_WarningFlag(t *testing.T) {
	now := time.Date(2026, 2, 26, 12, 0, 0, 0, time.UTC)
	d1 := now.AddDate(0, 0, -1).Add(-2 * time.Hour)
	d2 := now.AddDate(0, 0, -1).Add(-1 * time.Hour)

	historical := []MetricSnapshot{
		{Pipeline: "p", TableName: "orders", ColumnName: "total", MetricName: "sum", MetricValue: 100, CapturedAt: d1.Format(time.RFC3339)},
		{Pipeline: "p", TableName: "orders", ColumnName: "total", MetricName: "sum", MetricValue: 100, CapturedAt: d2.Format(time.RFC3339)},
	}

	db, _ := setupTestDB(t, historical)
	defer db.Close()

	cfg := &MonitorConfig{
		Monitoring: MonitoringBlock{
			Defaults: DefaultsConfig{Windows: []string{"day"}},
			Metrics: []MetricConfig{
				{Table: "orders", Columns: []ColumnMetric{{Column: "total", Aggregate: "sum"}}},
			},
		},
	}

	current := []MetricSnapshot{
		{Pipeline: "p", TableName: "orders", ColumnName: "total", MetricName: "sum", MetricValue: 115, CapturedAt: now.Format(time.RFC3339)},
	}

	flags, err := compareSnapshotsWithDB(db, cfg, current, now)
	if err != nil {
		t.Fatalf("compareSnapshotsWithDB: %v", err)
	}
	if len(flags) != 1 {
		t.Fatalf("expected 1 flag, got %d", len(flags))
	}
	if flags[0].Severity != "warning" {
		t.Errorf("expected warning, got %q", flags[0].Severity)
	}
	if flags[0].Window != "day" {
		t.Errorf("expected window=day, got %q", flags[0].Window)
	}
	expectedPct := 0.15
	if math.Abs(flags[0].PctChange-expectedPct) > 0.001 {
		t.Errorf("expected pct_change ~%.2f, got %.4f", expectedPct, flags[0].PctChange)
	}
}

func TestCompareSnapshots_ErrorFlag(t *testing.T) {
	now := time.Date(2026, 2, 26, 12, 0, 0, 0, time.UTC)
	d1 := now.AddDate(0, 0, -1).Add(-2 * time.Hour)
	d2 := now.AddDate(0, 0, -1).Add(-1 * time.Hour)

	historical := []MetricSnapshot{
		{Pipeline: "p", TableName: "orders", ColumnName: "total", MetricName: "sum", MetricValue: 100, CapturedAt: d1.Format(time.RFC3339)},
		{Pipeline: "p", TableName: "orders", ColumnName: "total", MetricName: "sum", MetricValue: 100, CapturedAt: d2.Format(time.RFC3339)},
	}

	db, _ := setupTestDB(t, historical)
	defer db.Close()

	cfg := &MonitorConfig{
		Monitoring: MonitoringBlock{
			Defaults: DefaultsConfig{Windows: []string{"day"}},
			Metrics: []MetricConfig{
				{Table: "orders", Columns: []ColumnMetric{{Column: "total", Aggregate: "sum"}}},
			},
		},
	}

	current := []MetricSnapshot{
		{Pipeline: "p", TableName: "orders", ColumnName: "total", MetricName: "sum", MetricValue: 130, CapturedAt: now.Format(time.RFC3339)},
	}

	flags, err := compareSnapshotsWithDB(db, cfg, current, now)
	if err != nil {
		t.Fatalf("compareSnapshotsWithDB: %v", err)
	}
	if len(flags) != 1 {
		t.Fatalf("expected 1 flag, got %d", len(flags))
	}
	if flags[0].Severity != "error" {
		t.Errorf("expected error severity, got %q", flags[0].Severity)
	}
}

func TestCompareSnapshots_BelowThreshold_NoFlag(t *testing.T) {
	now := time.Date(2026, 2, 26, 12, 0, 0, 0, time.UTC)
	d1 := now.AddDate(0, 0, -1).Add(-2 * time.Hour)
	d2 := now.AddDate(0, 0, -1).Add(-1 * time.Hour)

	historical := []MetricSnapshot{
		{Pipeline: "p", TableName: "orders", ColumnName: "total", MetricName: "sum", MetricValue: 100, CapturedAt: d1.Format(time.RFC3339)},
		{Pipeline: "p", TableName: "orders", ColumnName: "total", MetricName: "sum", MetricValue: 100, CapturedAt: d2.Format(time.RFC3339)},
	}

	db, _ := setupTestDB(t, historical)
	defer db.Close()

	cfg := &MonitorConfig{
		Monitoring: MonitoringBlock{
			Defaults: DefaultsConfig{Windows: []string{"day"}},
			Metrics: []MetricConfig{
				{Table: "orders", Columns: []ColumnMetric{{Column: "total", Aggregate: "sum"}}},
			},
		},
	}

	current := []MetricSnapshot{
		{Pipeline: "p", TableName: "orders", ColumnName: "total", MetricName: "sum", MetricValue: 105, CapturedAt: now.Format(time.RFC3339)},
	}

	flags, err := compareSnapshotsWithDB(db, cfg, current, now)
	if err != nil {
		t.Fatalf("compareSnapshotsWithDB: %v", err)
	}
	if len(flags) != 0 {
		t.Errorf("expected no flags for 5%% change, got %d", len(flags))
	}
}

func TestCompareSnapshots_NegativeChange(t *testing.T) {
	now := time.Date(2026, 2, 26, 12, 0, 0, 0, time.UTC)
	d1 := now.AddDate(0, 0, -1).Add(-2 * time.Hour)
	d2 := now.AddDate(0, 0, -1).Add(-1 * time.Hour)

	historical := []MetricSnapshot{
		{Pipeline: "p", TableName: "orders", ColumnName: "total", MetricName: "sum", MetricValue: 100, CapturedAt: d1.Format(time.RFC3339)},
		{Pipeline: "p", TableName: "orders", ColumnName: "total", MetricName: "sum", MetricValue: 100, CapturedAt: d2.Format(time.RFC3339)},
	}

	db, _ := setupTestDB(t, historical)
	defer db.Close()

	cfg := &MonitorConfig{
		Monitoring: MonitoringBlock{
			Defaults: DefaultsConfig{Windows: []string{"day"}},
			Metrics: []MetricConfig{
				{Table: "orders", Columns: []ColumnMetric{{Column: "total", Aggregate: "sum"}}},
			},
		},
	}

	current := []MetricSnapshot{
		{Pipeline: "p", TableName: "orders", ColumnName: "total", MetricName: "sum", MetricValue: 70, CapturedAt: now.Format(time.RFC3339)},
	}

	flags, err := compareSnapshotsWithDB(db, cfg, current, now)
	if err != nil {
		t.Fatalf("compareSnapshotsWithDB: %v", err)
	}
	if len(flags) != 1 {
		t.Fatalf("expected 1 flag for -30%% change, got %d", len(flags))
	}
	if flags[0].PctChange >= 0 {
		t.Errorf("expected negative pct_change, got %f", flags[0].PctChange)
	}
	if flags[0].Severity != "error" {
		t.Errorf("expected error for 30%% drop, got %q", flags[0].Severity)
	}
}

func TestCompareSnapshots_PerMetricThresholdOverride(t *testing.T) {
	now := time.Date(2026, 2, 26, 12, 0, 0, 0, time.UTC)
	d1 := now.AddDate(0, 0, -1).Add(-2 * time.Hour)
	d2 := now.AddDate(0, 0, -1).Add(-1 * time.Hour)

	historical := []MetricSnapshot{
		{Pipeline: "p", TableName: "orders", ColumnName: "total", MetricName: "sum", MetricValue: 100, CapturedAt: d1.Format(time.RFC3339)},
		{Pipeline: "p", TableName: "orders", ColumnName: "total", MetricName: "sum", MetricValue: 100, CapturedAt: d2.Format(time.RFC3339)},
	}

	db, _ := setupTestDB(t, historical)
	defer db.Close()

	cfg := &MonitorConfig{
		Monitoring: MonitoringBlock{
			Defaults: DefaultsConfig{Windows: []string{"day"}},
			Metrics: []MetricConfig{
				{Table: "orders", Columns: []ColumnMetric{
					{Column: "total", Aggregate: "sum", WarningThreshold: ptrFloat(0.50), ErrorThreshold: ptrFloat(0.75)},
				}},
			},
		},
	}

	current := []MetricSnapshot{
		{Pipeline: "p", TableName: "orders", ColumnName: "total", MetricName: "sum", MetricValue: 130, CapturedAt: now.Format(time.RFC3339)},
	}

	flags, err := compareSnapshotsWithDB(db, cfg, current, now)
	if err != nil {
		t.Fatalf("compareSnapshotsWithDB: %v", err)
	}
	if len(flags) != 0 {
		t.Errorf("expected no flags with high thresholds (50%%/75%%), got %d", len(flags))
	}
}

func TestCompareSnapshots_WeekWindow(t *testing.T) {
	now := time.Date(2026, 2, 26, 12, 0, 0, 0, time.UTC)

	// Prior week window: -14d to -7d from today
	d1 := now.AddDate(0, 0, -10)
	d2 := now.AddDate(0, 0, -9)

	historical := []MetricSnapshot{
		{Pipeline: "p", TableName: "orders", ColumnName: "total", MetricName: "sum", MetricValue: 100, CapturedAt: d1.Format(time.RFC3339)},
		{Pipeline: "p", TableName: "orders", ColumnName: "total", MetricName: "sum", MetricValue: 100, CapturedAt: d2.Format(time.RFC3339)},
	}

	db, _ := setupTestDB(t, historical)
	defer db.Close()

	cfg := &MonitorConfig{
		Monitoring: MonitoringBlock{
			Defaults: DefaultsConfig{Windows: []string{"week"}},
			Metrics: []MetricConfig{
				{Table: "orders", Columns: []ColumnMetric{{Column: "total", Aggregate: "sum"}}},
			},
		},
	}

	current := []MetricSnapshot{
		{Pipeline: "p", TableName: "orders", ColumnName: "total", MetricName: "sum", MetricValue: 115, CapturedAt: now.Format(time.RFC3339)},
	}

	flags, err := compareSnapshotsWithDB(db, cfg, current, now)
	if err != nil {
		t.Fatalf("compareSnapshotsWithDB: %v", err)
	}
	if len(flags) != 1 {
		t.Fatalf("expected 1 flag for week window, got %d", len(flags))
	}
	if flags[0].Window != "week" {
		t.Errorf("expected window=week, got %q", flags[0].Window)
	}
}

func TestCompareSnapshots_SegmentIndependent(t *testing.T) {
	now := time.Date(2026, 2, 26, 12, 0, 0, 0, time.UTC)
	d1 := now.AddDate(0, 0, -1).Add(-2 * time.Hour)
	d2 := now.AddDate(0, 0, -1).Add(-1 * time.Hour)

	historical := []MetricSnapshot{
		{Pipeline: "p", TableName: "orders", ColumnName: "state", MetricName: "count", MetricValue: 100, SegmentValue: "state=CA", CapturedAt: d1.Format(time.RFC3339)},
		{Pipeline: "p", TableName: "orders", ColumnName: "state", MetricName: "count", MetricValue: 100, SegmentValue: "state=CA", CapturedAt: d2.Format(time.RFC3339)},
		{Pipeline: "p", TableName: "orders", ColumnName: "state", MetricName: "count", MetricValue: 50, SegmentValue: "state=TX", CapturedAt: d1.Format(time.RFC3339)},
		{Pipeline: "p", TableName: "orders", ColumnName: "state", MetricName: "count", MetricValue: 50, SegmentValue: "state=TX", CapturedAt: d2.Format(time.RFC3339)},
	}

	db, _ := setupTestDB(t, historical)
	defer db.Close()

	cfg := &MonitorConfig{
		Monitoring: MonitoringBlock{
			Defaults: DefaultsConfig{Windows: []string{"day"}},
			Segments: []SegmentConfig{
				{Table: "orders", SegmentColumn: "state", Metric: "count"},
			},
		},
	}

	current := []MetricSnapshot{
		{Pipeline: "p", TableName: "orders", ColumnName: "state", MetricName: "count", MetricValue: 115, SegmentValue: "state=CA", CapturedAt: now.Format(time.RFC3339)},
		{Pipeline: "p", TableName: "orders", ColumnName: "state", MetricName: "count", MetricValue: 50, SegmentValue: "state=TX", CapturedAt: now.Format(time.RFC3339)},
	}

	flags, err := compareSnapshotsWithDB(db, cfg, current, now)
	if err != nil {
		t.Fatalf("compareSnapshotsWithDB: %v", err)
	}
	if len(flags) != 1 {
		t.Fatalf("expected 1 flag (CA only), got %d", len(flags))
	}
	if flags[0].CurrentValue != 115 {
		t.Errorf("expected flagged value 115, got %f", flags[0].CurrentValue)
	}
}

func TestCompareSnapshots_MultipleWindows(t *testing.T) {
	now := time.Date(2026, 2, 26, 12, 0, 0, 0, time.UTC)

	// Seed data for both day and week windows
	dayD1 := now.AddDate(0, 0, -1).Add(-2 * time.Hour)
	dayD2 := now.AddDate(0, 0, -1).Add(-1 * time.Hour)
	weekD1 := now.AddDate(0, 0, -10)
	weekD2 := now.AddDate(0, 0, -9)

	historical := []MetricSnapshot{
		{Pipeline: "p", TableName: "orders", ColumnName: "total", MetricName: "sum", MetricValue: 100, CapturedAt: dayD1.Format(time.RFC3339)},
		{Pipeline: "p", TableName: "orders", ColumnName: "total", MetricName: "sum", MetricValue: 100, CapturedAt: dayD2.Format(time.RFC3339)},
		{Pipeline: "p", TableName: "orders", ColumnName: "total", MetricName: "sum", MetricValue: 100, CapturedAt: weekD1.Format(time.RFC3339)},
		{Pipeline: "p", TableName: "orders", ColumnName: "total", MetricName: "sum", MetricValue: 100, CapturedAt: weekD2.Format(time.RFC3339)},
	}

	db, _ := setupTestDB(t, historical)
	defer db.Close()

	cfg := &MonitorConfig{
		Monitoring: MonitoringBlock{
			Defaults: DefaultsConfig{Windows: []string{"day", "week"}},
			Metrics: []MetricConfig{
				{Table: "orders", Columns: []ColumnMetric{{Column: "total", Aggregate: "sum"}}},
			},
		},
	}

	current := []MetricSnapshot{
		{Pipeline: "p", TableName: "orders", ColumnName: "total", MetricName: "sum", MetricValue: 115, CapturedAt: now.Format(time.RFC3339)},
	}

	flags, err := compareSnapshotsWithDB(db, cfg, current, now)
	if err != nil {
		t.Fatalf("compareSnapshotsWithDB: %v", err)
	}
	if len(flags) != 2 {
		t.Fatalf("expected 2 flags (day + week), got %d", len(flags))
	}

	windowsSeen := map[string]bool{}
	for _, f := range flags {
		windowsSeen[f.Window] = true
	}
	if !windowsSeen["day"] || !windowsSeen["week"] {
		t.Errorf("expected day and week windows, got %v", windowsSeen)
	}
}

func TestCompareSnapshots_NilInputs(t *testing.T) {
	flags, err := CompareSnapshots("/tmp/nonexistent.db", nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if flags != nil {
		t.Errorf("expected nil, got %v", flags)
	}

	cfg := &MonitorConfig{}
	flags, err = CompareSnapshots("/tmp/nonexistent.db", cfg, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if flags != nil {
		t.Errorf("expected nil, got %v", flags)
	}
}

func TestCompareSnapshots_PriorValueZero_NoFlag(t *testing.T) {
	now := time.Date(2026, 2, 26, 12, 0, 0, 0, time.UTC)
	d1 := now.AddDate(0, 0, -1).Add(-2 * time.Hour)
	d2 := now.AddDate(0, 0, -1).Add(-1 * time.Hour)

	historical := []MetricSnapshot{
		{Pipeline: "p", TableName: "orders", ColumnName: "total", MetricName: "sum", MetricValue: 0, CapturedAt: d1.Format(time.RFC3339)},
		{Pipeline: "p", TableName: "orders", ColumnName: "total", MetricName: "sum", MetricValue: 0, CapturedAt: d2.Format(time.RFC3339)},
	}

	db, _ := setupTestDB(t, historical)
	defer db.Close()

	cfg := &MonitorConfig{
		Monitoring: MonitoringBlock{
			Defaults: DefaultsConfig{Windows: []string{"day"}},
			Metrics: []MetricConfig{
				{Table: "orders", Columns: []ColumnMetric{{Column: "total", Aggregate: "sum"}}},
			},
		},
	}

	current := []MetricSnapshot{
		{Pipeline: "p", TableName: "orders", ColumnName: "total", MetricName: "sum", MetricValue: 100, CapturedAt: now.Format(time.RFC3339)},
	}

	flags, err := compareSnapshotsWithDB(db, cfg, current, now)
	if err != nil {
		t.Fatalf("compareSnapshotsWithDB: %v", err)
	}
	if len(flags) != 0 {
		t.Errorf("expected no flags when prior is zero, got %d", len(flags))
	}
}

func TestCompareSnapshots_RateMetric(t *testing.T) {
	now := time.Date(2026, 2, 26, 12, 0, 0, 0, time.UTC)
	d1 := now.AddDate(0, 0, -1).Add(-2 * time.Hour)
	d2 := now.AddDate(0, 0, -1).Add(-1 * time.Hour)

	historical := []MetricSnapshot{
		{Pipeline: "p", TableName: "orders", ColumnName: "conversion", MetricName: "rate", MetricValue: 0.50, CapturedAt: d1.Format(time.RFC3339)},
		{Pipeline: "p", TableName: "orders", ColumnName: "conversion", MetricName: "rate", MetricValue: 0.50, CapturedAt: d2.Format(time.RFC3339)},
	}

	db, _ := setupTestDB(t, historical)
	defer db.Close()

	cfg := &MonitorConfig{
		Monitoring: MonitoringBlock{
			Defaults: DefaultsConfig{Windows: []string{"day"}},
			Rates: []RateConfig{
				{Name: "conversion", Numerator: RateTableRef{Table: "orders", Aggregate: "count"}, Denominator: RateTableRef{Table: "visits", Aggregate: "count"}},
			},
		},
	}

	current := []MetricSnapshot{
		{Pipeline: "p", TableName: "orders", ColumnName: "conversion", MetricName: "rate", MetricValue: 0.35, CapturedAt: now.Format(time.RFC3339)},
	}

	flags, err := compareSnapshotsWithDB(db, cfg, current, now)
	if err != nil {
		t.Fatalf("compareSnapshotsWithDB: %v", err)
	}
	if len(flags) != 1 {
		t.Fatalf("expected 1 flag for rate change, got %d", len(flags))
	}
	if flags[0].Severity != "error" {
		t.Errorf("expected error for 30%% rate drop, got %q", flags[0].Severity)
	}
}

func TestComputeWindowRanges(t *testing.T) {
	now := time.Date(2026, 2, 26, 15, 30, 0, 0, time.UTC)
	wr := computeWindowRanges(now)

	if wr["day"].Start != "2026-02-25" || wr["day"].End != "2026-02-26" {
		t.Errorf("day: got %s to %s", wr["day"].Start, wr["day"].End)
	}
	if wr["week"].Start != "2026-02-12" || wr["week"].End != "2026-02-19" {
		t.Errorf("week: got %s to %s", wr["week"].Start, wr["week"].End)
	}
	if wr["month"].Start != "2026-01-01" || wr["month"].End != "2026-02-01" {
		t.Errorf("month: got %s to %s", wr["month"].Start, wr["month"].End)
	}
	if wr["year"].Start != "2025-02-26" || wr["year"].End != "2025-02-27" {
		t.Errorf("year: got %s to %s", wr["year"].Start, wr["year"].End)
	}
}

func TestCompareSnapshots_DefaultsApplyToStructuralMetrics(t *testing.T) {
	now := time.Date(2026, 2, 26, 12, 0, 0, 0, time.UTC)
	d1 := now.AddDate(0, 0, -1).Add(-2 * time.Hour)
	d2 := now.AddDate(0, 0, -1).Add(-1 * time.Hour)

	historical := []MetricSnapshot{
		{Pipeline: "p", TableName: "orders", ColumnName: "", MetricName: "row_count", MetricValue: 1000, CapturedAt: d1.Format(time.RFC3339)},
		{Pipeline: "p", TableName: "orders", ColumnName: "", MetricName: "row_count", MetricValue: 1000, CapturedAt: d2.Format(time.RFC3339)},
	}

	db, _ := setupTestDB(t, historical)
	defer db.Close()

	cfg := &MonitorConfig{
		Monitoring: MonitoringBlock{
			Defaults: DefaultsConfig{Windows: []string{"day"}},
		},
	}

	current := []MetricSnapshot{
		{Pipeline: "p", TableName: "orders", ColumnName: "", MetricName: "row_count", MetricValue: 1200, CapturedAt: now.Format(time.RFC3339)},
	}

	flags, err := compareSnapshotsWithDB(db, cfg, current, now)
	if err != nil {
		t.Fatalf("compareSnapshotsWithDB: %v", err)
	}
	if len(flags) != 1 {
		t.Fatalf("expected 1 flag for structural metric, got %d", len(flags))
	}
	if flags[0].Severity != "warning" {
		t.Errorf("expected warning for 20%% change, got %q", flags[0].Severity)
	}
}
