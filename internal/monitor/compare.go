package monitor

import (
	"database/sql"
	"fmt"
	"math"
	"time"
)

type snapshotKey struct {
	TableName    string
	ColumnName   string
	MetricName   string
	SegmentValue string
}

type windowRange struct {
	Name  string
	Start string
	End   string
}

// CompareSnapshots compares current metric values against historical values
// from monitor.db, returning distribution flags for any threshold violations.
// Current snapshots are matched against prior-period averages for each
// configured window (day, week, month, year). Missing historical data
// produces no flags (no false positives on bootstrap).
func CompareSnapshots(dbPath string, cfg *MonitorConfig, current []MetricSnapshot) ([]DistributionFlag, error) {
	if len(current) == 0 || cfg == nil {
		return nil, nil
	}

	db, err := openDB(dbPath)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	return compareSnapshotsWithDB(db, cfg, current, time.Now().UTC())
}

func compareSnapshotsWithDB(db *sql.DB, cfg *MonitorConfig, current []MetricSnapshot, now time.Time) ([]DistributionFlag, error) {
	thresholds := buildThresholdMap(cfg)
	windowsMap := buildWindowsMap(cfg)
	allWindows := computeWindowRanges(now)

	var flags []DistributionFlag

	for _, snap := range current {
		key := snapshotKey{
			TableName:    snap.TableName,
			ColumnName:   snap.ColumnName,
			MetricName:   snap.MetricName,
			SegmentValue: snap.SegmentValue,
		}

		windows := windowsMap[metricKeyStr(key)]
		if windows == nil {
			windows = resolvedDefaultWindows(cfg.Monitoring.Defaults)
		}

		warn, errThresh := lookupThresholds(thresholds, key, cfg.Monitoring.Defaults)

		for _, winName := range windows {
			wr, ok := allWindows[winName]
			if !ok {
				continue
			}

			priorAvg, count, err := queryPriorAverage(db, snap.Pipeline, key, wr)
			if err != nil {
				continue
			}
			if count < 2 {
				continue
			}
			if priorAvg == 0 {
				continue
			}

			pctChange := (snap.MetricValue - priorAvg) / priorAvg
			absPct := math.Abs(pctChange)

			var severity string
			if absPct >= errThresh {
				severity = "error"
			} else if absPct >= warn {
				severity = "warning"
			} else {
				continue
			}

			flags = append(flags, DistributionFlag{
				Pipeline:     snap.Pipeline,
				TableName:    snap.TableName,
				ColumnName:   snap.ColumnName,
				MetricName:   snap.MetricName,
				Window:       wr.Name,
				CurrentValue: snap.MetricValue,
				PriorValue:   priorAvg,
				PctChange:    pctChange,
				Severity:     severity,
				CapturedAt:   snap.CapturedAt,
			})
		}
	}

	return flags, nil
}

func computeWindowRanges(now time.Time) map[string]windowRange {
	today := now.Truncate(24 * time.Hour)

	yesterday := today.AddDate(0, 0, -1)

	weekStart := today.AddDate(0, 0, -14)
	weekEnd := today.AddDate(0, 0, -7)

	curYear, curMonth, _ := today.Date()
	priorMonthEnd := time.Date(curYear, curMonth, 1, 0, 0, 0, 0, time.UTC)
	priorMonthStart := priorMonthEnd.AddDate(0, -1, 0)

	yearStart := today.AddDate(-1, 0, 0)
	yearEnd := today.AddDate(-1, 0, 1)

	f := "2006-01-02"
	return map[string]windowRange{
		"day": {
			Name:  "day",
			Start: yesterday.Format(f),
			End:   today.Format(f),
		},
		"week": {
			Name:  "week",
			Start: weekStart.Format(f),
			End:   weekEnd.Format(f),
		},
		"month": {
			Name:  "month",
			Start: priorMonthStart.Format(f),
			End:   priorMonthEnd.Format(f),
		},
		"year": {
			Name:  "year",
			Start: yearStart.Format(f),
			End:   yearEnd.Format(f),
		},
	}
}

func queryPriorAverage(db *sql.DB, pipeline string, key snapshotKey, wr windowRange) (float64, int, error) {
	q := `SELECT AVG(metric_value), COUNT(*) FROM metric_snapshots
		WHERE pipeline = ?
		  AND table_name = ?
		  AND column_name = ?
		  AND metric_name = ?
		  AND segment_value = ?
		  AND date(captured_at) >= ?
		  AND date(captured_at) < ?`

	var avg sql.NullFloat64
	var count int
	err := db.QueryRow(q, pipeline, key.TableName, key.ColumnName, key.MetricName, key.SegmentValue, wr.Start, wr.End).Scan(&avg, &count)
	if err != nil {
		return 0, 0, fmt.Errorf("querying prior average: %w", err)
	}
	if !avg.Valid {
		return 0, 0, nil
	}
	return avg.Float64, count, nil
}

type thresholdPair struct {
	Warning float64
	Error   float64
}

func metricKeyStr(key snapshotKey) string {
	return key.TableName + "|" + key.ColumnName + "|" + key.MetricName
}

func buildThresholdMap(cfg *MonitorConfig) map[string]thresholdPair {
	m := make(map[string]thresholdPair)
	defaults := cfg.Monitoring.Defaults

	for _, mc := range cfg.Monitoring.Metrics {
		for _, col := range mc.Columns {
			k := mc.Table + "|" + col.Column + "|" + col.Aggregate
			m[k] = thresholdPair{
				Warning: col.ResolvedWarningThreshold(defaults),
				Error:   col.ResolvedErrorThreshold(defaults),
			}
		}
	}

	for _, rc := range cfg.Monitoring.Rates {
		k := rc.Numerator.Table + "|" + rc.Name + "|rate"
		m[k] = thresholdPair{
			Warning: rc.ResolvedWarningThreshold(defaults),
			Error:   rc.ResolvedErrorThreshold(defaults),
		}
	}

	for _, sc := range cfg.Monitoring.Segments {
		k := sc.Table + "|" + sc.SegmentColumn + "|" + sc.Metric
		m[k] = thresholdPair{
			Warning: sc.ResolvedWarningThreshold(defaults),
			Error:   sc.ResolvedErrorThreshold(defaults),
		}
	}

	return m
}

func buildWindowsMap(cfg *MonitorConfig) map[string][]string {
	m := make(map[string][]string)
	defaults := cfg.Monitoring.Defaults

	for _, mc := range cfg.Monitoring.Metrics {
		for _, col := range mc.Columns {
			k := mc.Table + "|" + col.Column + "|" + col.Aggregate
			m[k] = col.ResolvedWindows(defaults)
		}
	}

	for _, rc := range cfg.Monitoring.Rates {
		k := rc.Numerator.Table + "|" + rc.Name + "|rate"
		m[k] = rc.ResolvedWindows(defaults)
	}

	for _, sc := range cfg.Monitoring.Segments {
		k := sc.Table + "|" + sc.SegmentColumn + "|" + sc.Metric
		m[k] = sc.ResolvedWindows(defaults)
	}

	return m
}

func lookupThresholds(m map[string]thresholdPair, key snapshotKey, defaults DefaultsConfig) (float64, float64) {
	k := metricKeyStr(key)
	if pair, ok := m[k]; ok {
		return pair.Warning, pair.Error
	}
	warn := DefaultWarningThreshold
	if defaults.WarningThreshold != nil {
		warn = *defaults.WarningThreshold
	}
	errT := DefaultErrorThreshold
	if defaults.ErrorThreshold != nil {
		errT = *defaults.ErrorThreshold
	}
	return warn, errT
}
