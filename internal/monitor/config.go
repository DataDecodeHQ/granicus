package monitor

import (
	"fmt"
	"os"
	"strings"

	"github.com/analytehealth/granicus/internal/config"
	"gopkg.in/yaml.v3"
)

var defaultWindows = []string{"day", "week", "month", "year"}

const (
	DefaultWarningThreshold = 0.10
	DefaultErrorThreshold   = 0.25
)

var validWindows = map[string]bool{
	"day":   true,
	"week":  true,
	"month": true,
	"year":  true,
}

var validAggregates = map[string]bool{
	"count":  true,
	"sum":    true,
	"avg":    true,
	"median": true,
	"min":    true,
	"max":    true,
}

type MonitorConfig struct {
	Monitoring MonitoringBlock `yaml:"monitoring"`
}

type MonitoringBlock struct {
	Defaults   DefaultsConfig    `yaml:"defaults"`
	Structural *StructuralConfig `yaml:"structural,omitempty"`
	Metrics    []MetricConfig    `yaml:"metrics,omitempty"`
	Rates      []RateConfig      `yaml:"rates,omitempty"`
	Segments   []SegmentConfig   `yaml:"segments,omitempty"`
}

type DefaultsConfig struct {
	Windows          []string `yaml:"windows,omitempty"`
	WarningThreshold *float64 `yaml:"warning_threshold,omitempty"`
	ErrorThreshold   *float64 `yaml:"error_threshold,omitempty"`
}

type StructuralConfig struct {
	RowCounts bool     `yaml:"row_counts"`
	NullRates []string `yaml:"null_rates,omitempty"`
}

type ColumnMetric struct {
	Column           string   `yaml:"column"`
	Aggregate        string   `yaml:"aggregate"`
	WarningThreshold *float64 `yaml:"warning_threshold,omitempty"`
	ErrorThreshold   *float64 `yaml:"error_threshold,omitempty"`
	Windows          []string `yaml:"windows,omitempty"`
}

type MetricConfig struct {
	Table   string         `yaml:"table"`
	Columns []ColumnMetric `yaml:"columns"`
}

type RateTableRef struct {
	Table     string `yaml:"table"`
	Aggregate string `yaml:"aggregate"`
}

type RateConfig struct {
	Name             string       `yaml:"name"`
	Numerator        RateTableRef `yaml:"numerator"`
	Denominator      RateTableRef `yaml:"denominator"`
	WarningThreshold *float64     `yaml:"warning_threshold,omitempty"`
	ErrorThreshold   *float64     `yaml:"error_threshold,omitempty"`
	Windows          []string     `yaml:"windows,omitempty"`
}

type SegmentConfig struct {
	Table            string   `yaml:"table"`
	SegmentColumn    string   `yaml:"segment_column"`
	Metric           string   `yaml:"metric"`
	WarningThreshold *float64 `yaml:"warning_threshold,omitempty"`
	ErrorThreshold   *float64 `yaml:"error_threshold,omitempty"`
	Windows          []string `yaml:"windows,omitempty"`
}

func (cm *ColumnMetric) ResolvedWarningThreshold(defaults DefaultsConfig) float64 {
	if cm.WarningThreshold != nil {
		return *cm.WarningThreshold
	}
	if defaults.WarningThreshold != nil {
		return *defaults.WarningThreshold
	}
	return DefaultWarningThreshold
}

func (cm *ColumnMetric) ResolvedErrorThreshold(defaults DefaultsConfig) float64 {
	if cm.ErrorThreshold != nil {
		return *cm.ErrorThreshold
	}
	if defaults.ErrorThreshold != nil {
		return *defaults.ErrorThreshold
	}
	return DefaultErrorThreshold
}

func (cm *ColumnMetric) ResolvedWindows(defaults DefaultsConfig) []string {
	if len(cm.Windows) > 0 {
		return cm.Windows
	}
	return resolvedDefaultWindows(defaults)
}

func (rc *RateConfig) ResolvedWarningThreshold(defaults DefaultsConfig) float64 {
	if rc.WarningThreshold != nil {
		return *rc.WarningThreshold
	}
	if defaults.WarningThreshold != nil {
		return *defaults.WarningThreshold
	}
	return DefaultWarningThreshold
}

func (rc *RateConfig) ResolvedErrorThreshold(defaults DefaultsConfig) float64 {
	if rc.ErrorThreshold != nil {
		return *rc.ErrorThreshold
	}
	if defaults.ErrorThreshold != nil {
		return *defaults.ErrorThreshold
	}
	return DefaultErrorThreshold
}

func (rc *RateConfig) ResolvedWindows(defaults DefaultsConfig) []string {
	if len(rc.Windows) > 0 {
		return rc.Windows
	}
	return resolvedDefaultWindows(defaults)
}

func (sc *SegmentConfig) ResolvedWarningThreshold(defaults DefaultsConfig) float64 {
	if sc.WarningThreshold != nil {
		return *sc.WarningThreshold
	}
	if defaults.WarningThreshold != nil {
		return *defaults.WarningThreshold
	}
	return DefaultWarningThreshold
}

func (sc *SegmentConfig) ResolvedErrorThreshold(defaults DefaultsConfig) float64 {
	if sc.ErrorThreshold != nil {
		return *sc.ErrorThreshold
	}
	if defaults.ErrorThreshold != nil {
		return *defaults.ErrorThreshold
	}
	return DefaultErrorThreshold
}

func (sc *SegmentConfig) ResolvedWindows(defaults DefaultsConfig) []string {
	if len(sc.Windows) > 0 {
		return sc.Windows
	}
	return resolvedDefaultWindows(defaults)
}

func resolvedDefaultWindows(defaults DefaultsConfig) []string {
	if len(defaults.Windows) > 0 {
		return defaults.Windows
	}
	return defaultWindows
}

// LoadMonitorConfig loads monitoring.yaml from the given path.
// Returns nil, nil if the file does not exist (monitoring disabled).
func LoadMonitorConfig(path string) (*MonitorConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("reading monitoring config: %w", err)
	}

	var cfg MonitorConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing monitoring config: %w", err)
	}

	if err := validateConfig(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func validateConfig(cfg *MonitorConfig) error {
	m := &cfg.Monitoring

	for _, w := range m.Defaults.Windows {
		if !validWindows[w] {
			return fmt.Errorf("defaults: invalid window %q (must be day, week, month, or year)", w)
		}
	}

	if m.Defaults.WarningThreshold != nil && *m.Defaults.WarningThreshold < 0 {
		return fmt.Errorf("defaults: warning_threshold must be >= 0")
	}
	if m.Defaults.ErrorThreshold != nil && *m.Defaults.ErrorThreshold < 0 {
		return fmt.Errorf("defaults: error_threshold must be >= 0")
	}

	if m.Structural != nil {
		for _, nr := range m.Structural.NullRates {
			if !strings.Contains(nr, ".") {
				return fmt.Errorf("structural.null_rates: %q must be in table.column format", nr)
			}
		}
	}

	for i, mc := range m.Metrics {
		if mc.Table == "" {
			return fmt.Errorf("metrics[%d]: table is required", i)
		}
		if len(mc.Columns) == 0 {
			return fmt.Errorf("metrics[%d] (%s): at least one column is required", i, mc.Table)
		}
		for j, col := range mc.Columns {
			if col.Column == "" {
				return fmt.Errorf("metrics[%d].columns[%d]: column is required", i, j)
			}
			if col.Aggregate == "" {
				return fmt.Errorf("metrics[%d].columns[%d] (%s): aggregate is required", i, j, col.Column)
			}
			if !validAggregates[col.Aggregate] {
				return fmt.Errorf("metrics[%d].columns[%d] (%s): invalid aggregate %q", i, j, col.Column, col.Aggregate)
			}
			for _, w := range col.Windows {
				if !validWindows[w] {
					return fmt.Errorf("metrics[%d].columns[%d] (%s): invalid window %q", i, j, col.Column, w)
				}
			}
		}
	}

	for i, rc := range m.Rates {
		if rc.Name == "" {
			return fmt.Errorf("rates[%d]: name is required", i)
		}
		if rc.Numerator.Table == "" {
			return fmt.Errorf("rates[%d] (%s): numerator.table is required", i, rc.Name)
		}
		if rc.Numerator.Aggregate == "" {
			return fmt.Errorf("rates[%d] (%s): numerator.aggregate is required", i, rc.Name)
		}
		if !validAggregates[rc.Numerator.Aggregate] {
			return fmt.Errorf("rates[%d] (%s): invalid numerator aggregate %q", i, rc.Name, rc.Numerator.Aggregate)
		}
		if rc.Denominator.Table == "" {
			return fmt.Errorf("rates[%d] (%s): denominator.table is required", i, rc.Name)
		}
		if rc.Denominator.Aggregate == "" {
			return fmt.Errorf("rates[%d] (%s): denominator.aggregate is required", i, rc.Name)
		}
		if !validAggregates[rc.Denominator.Aggregate] {
			return fmt.Errorf("rates[%d] (%s): invalid denominator aggregate %q", i, rc.Name, rc.Denominator.Aggregate)
		}
		for _, w := range rc.Windows {
			if !validWindows[w] {
				return fmt.Errorf("rates[%d] (%s): invalid window %q", i, rc.Name, w)
			}
		}
	}

	for i, sc := range m.Segments {
		if sc.Table == "" {
			return fmt.Errorf("segments[%d]: table is required", i)
		}
		if sc.SegmentColumn == "" {
			return fmt.Errorf("segments[%d] (%s): segment_column is required", i, sc.Table)
		}
		if sc.Metric == "" {
			return fmt.Errorf("segments[%d] (%s): metric is required", i, sc.Table)
		}
		if !validAggregates[sc.Metric] {
			return fmt.Errorf("segments[%d] (%s): invalid metric %q", i, sc.Table, sc.Metric)
		}
		for _, w := range sc.Windows {
			if !validWindows[w] {
				return fmt.Errorf("segments[%d] (%s): invalid window %q", i, sc.Table, w)
			}
		}
	}

	return nil
}

// Validate cross-references monitoring config against a loaded pipeline config.
func (cfg *MonitorConfig) Validate(pipeline *config.PipelineConfig) []error {
	var errs []error
	assets := make(map[string]bool)
	for _, a := range pipeline.Assets {
		assets[a.Name] = true
	}
	for name := range pipeline.Sources {
		assets[name] = true
	}

	m := &cfg.Monitoring

	if m.Structural != nil {
		for _, nr := range m.Structural.NullRates {
			table, _, _ := strings.Cut(nr, ".")
			if !assets[table] {
				errs = append(errs, fmt.Errorf("structural.null_rates: table %q (from %q) not found in pipeline", table, nr))
			}
		}
	}

	for _, mc := range m.Metrics {
		if !assets[mc.Table] {
			errs = append(errs, fmt.Errorf("metrics: table %q not found in pipeline", mc.Table))
		}
	}

	for _, rc := range m.Rates {
		if !assets[rc.Numerator.Table] {
			errs = append(errs, fmt.Errorf("rates[%s]: numerator table %q not found in pipeline", rc.Name, rc.Numerator.Table))
		}
		if !assets[rc.Denominator.Table] {
			errs = append(errs, fmt.Errorf("rates[%s]: denominator table %q not found in pipeline", rc.Name, rc.Denominator.Table))
		}
	}

	for _, sc := range m.Segments {
		if !assets[sc.Table] {
			errs = append(errs, fmt.Errorf("segments: table %q not found in pipeline", sc.Table))
		}
	}

	return errs
}
