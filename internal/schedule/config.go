package schedule

import (
	"fmt"
	"os"
	"strings"

	"github.com/robfig/cron/v3"
	"gopkg.in/yaml.v3"
)

// ScheduleConfig is the top-level structure for schedule.yml.
type ScheduleConfig struct {
	Version   string                    `yaml:"version"`
	Schedules map[string]ScheduleEntry  `yaml:"schedules"`
}

// ScheduleEntry defines the schedule for a single pipeline.
type ScheduleEntry struct {
	Cron     string `yaml:"cron"`
	Timezone string `yaml:"timezone,omitempty"`
	Enabled  *bool  `yaml:"enabled,omitempty"`
	Mode     string `yaml:"mode,omitempty"`
}

// IsEnabled returns whether the schedule entry is enabled (defaults to true).
func (e ScheduleEntry) IsEnabled() bool {
	if e.Enabled == nil {
		return true
	}
	return *e.Enabled
}

// EffectiveTimezone returns the timezone, defaulting to UTC.
func (e ScheduleEntry) EffectiveTimezone() string {
	if e.Timezone == "" {
		return "UTC"
	}
	return e.Timezone
}

var validModes = map[string]bool{
	"":      true,
	"cloud": true,
	"local": true,
	"auto":  true,
}

// LoadScheduleConfig reads and validates a schedule.yml file.
func LoadScheduleConfig(path string) (*ScheduleConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading schedule config: %w", err)
	}

	var cfg ScheduleConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing schedule config: %w", err)
	}

	if err := validateScheduleConfig(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func validateScheduleConfig(cfg *ScheduleConfig) error {
	if len(cfg.Schedules) == 0 {
		return fmt.Errorf("schedule config: no schedules defined")
	}

	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

	for name, entry := range cfg.Schedules {
		if entry.Cron == "" {
			return fmt.Errorf("schedule %q: cron expression is required", name)
		}

		// Strip CRON_TZ prefix if present (robfig/cron handles it, but we validate the expression)
		cronExpr := entry.Cron
		if strings.HasPrefix(cronExpr, "CRON_TZ=") {
			parts := strings.SplitN(cronExpr, " ", 2)
			if len(parts) == 2 {
				cronExpr = parts[1]
			}
		}

		if _, err := parser.Parse(cronExpr); err != nil {
			return fmt.Errorf("schedule %q: invalid cron expression %q: %w", name, entry.Cron, err)
		}

		if !validModes[entry.Mode] {
			return fmt.Errorf("schedule %q: invalid mode %q (must be cloud, local, or auto)", name, entry.Mode)
		}
	}

	return nil
}
