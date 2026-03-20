package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/DataDecodeHQ/granicus/internal/config"
	"github.com/DataDecodeHQ/granicus/internal/schedule"
)

func newScheduleCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "schedule",
		Short: "Manage pipeline schedules",
	}

	listCmd := &cobra.Command{
		Use:   "list",
		Short: "Show all schedules from schedule.yml",
		RunE:  runScheduleList,
	}
	listCmd.Flags().String("project-root", ".", "Project root directory")
	listCmd.Flags().String("config-dir", "", "Config directory (defaults to project-root)")
	addJSONFlag(listCmd)

	showCmd := &cobra.Command{
		Use:   "show <pipeline>",
		Short: "Show detail for one pipeline schedule",
		Args:  cobra.ExactArgs(1),
		RunE:  runScheduleShow,
	}
	showCmd.Flags().String("project-root", ".", "Project root directory")
	showCmd.Flags().String("config-dir", "", "Config directory (defaults to project-root)")
	addJSONFlag(showCmd)

	setCmd := &cobra.Command{
		Use:   "set <pipeline> <cron>",
		Short: "Write or update cron expression in schedule.yml",
		Args:  cobra.ExactArgs(2),
		RunE:  runScheduleSet,
	}
	setCmd.Flags().String("project-root", ".", "Project root directory")
	setCmd.Flags().String("config-dir", "", "Config directory (defaults to project-root)")
	setCmd.Flags().String("timezone", "", "Timezone (e.g. America/Chicago)")
	setCmd.Flags().String("mode", "", "Mode: local, cloud, or auto")

	enableCmd := &cobra.Command{
		Use:   "enable <pipeline>",
		Short: "Enable a pipeline schedule",
		Args:  cobra.ExactArgs(1),
		RunE:  runScheduleEnable,
	}
	enableCmd.Flags().String("project-root", ".", "Project root directory")
	enableCmd.Flags().String("config-dir", "", "Config directory (defaults to project-root)")

	disableCmd := &cobra.Command{
		Use:   "disable <pipeline>",
		Short: "Disable a pipeline schedule",
		Args:  cobra.ExactArgs(1),
		RunE:  runScheduleDisable,
	}
	disableCmd.Flags().String("project-root", ".", "Project root directory")
	disableCmd.Flags().String("config-dir", "", "Config directory (defaults to project-root)")

	syncCmd := &cobra.Command{
		Use:   "sync",
		Short: "Sync schedules from pipeline configs",
		RunE:  runScheduleSync,
	}
	syncCmd.Flags().String("project-root", ".", "Project root directory")
	syncCmd.Flags().String("config-dir", "", "Config directory (defaults to project-root)")
	syncCmd.Flags().Bool("init", false, "Scan pipeline.yaml files and generate schedule.yml")

	cmd.AddCommand(listCmd, showCmd, setCmd, enableCmd, disableCmd, syncCmd)
	return cmd
}

func resolveSchedulePath(cmd *cobra.Command) string {
	projectRoot, _ := cmd.Flags().GetString("project-root")
	configDir, _ := cmd.Flags().GetString("config-dir")

	// Try project-root first
	p := filepath.Join(projectRoot, "schedule.yml")
	if _, err := os.Stat(p); err == nil {
		return p
	}

	// Then config-dir
	if configDir != "" {
		p = filepath.Join(configDir, "schedule.yml")
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}

	// Default to project-root (for creation)
	return filepath.Join(projectRoot, "schedule.yml")
}

func loadScheduleFile(cmd *cobra.Command) (*schedule.ScheduleConfig, string, error) {
	path := resolveSchedulePath(cmd)
	cfg, err := schedule.LoadScheduleConfig(path)
	if err != nil {
		return nil, path, err
	}
	return cfg, path, nil
}

func runScheduleList(cmd *cobra.Command, args []string) error {
	if isCloudMode() {
		data, err := cloudGet("/api/v1/schedules/list")
		if err != nil {
			return err
		}
		if wantJSON(cmd) {
			fmt.Println(string(data))
			return nil
		}
		var entries []struct {
			Pipeline string `json:"pipeline"`
			Cron     string `json:"cron"`
			Timezone string `json:"timezone"`
			Mode     string `json:"mode"`
			Enabled  bool   `json:"enabled"`
		}
		if err := json.Unmarshal(data, &entries); err != nil {
			return err
		}
		sort.Slice(entries, func(i, j int) bool { return entries[i].Pipeline < entries[j].Pipeline })
		fmt.Printf("%-24s %-20s %-18s %-8s %-8s\n", "Pipeline", "Cron", "Timezone", "Mode", "Enabled")
		fmt.Println(strings.Repeat("-", 82))
		for _, e := range entries {
			fmt.Printf("%-24s %-20s %-18s %-8s %-8v\n", e.Pipeline, e.Cron, e.Timezone, e.Mode, e.Enabled)
		}
		return nil
	}

	cfg, _, err := loadScheduleFile(cmd)
	if err != nil {
		return fmt.Errorf("loading schedule.yml: %w", err)
	}

	if wantJSON(cmd) {
		type entry struct {
			Pipeline string `json:"pipeline"`
			Cron     string `json:"cron"`
			Timezone string `json:"timezone"`
			Mode     string `json:"mode"`
			Enabled  bool   `json:"enabled"`
		}
		var entries []entry
		for name, e := range cfg.Schedules {
			mode := e.Mode
			if mode == "" {
				mode = "local"
			}
			entries = append(entries, entry{
				Pipeline: name,
				Cron:     e.Cron,
				Timezone: e.EffectiveTimezone(),
				Mode:     mode,
				Enabled:  e.IsEnabled(),
			})
		}
		sort.Slice(entries, func(i, j int) bool { return entries[i].Pipeline < entries[j].Pipeline })
		return outputJSON(entries)
	}

	names := make([]string, 0, len(cfg.Schedules))
	for name := range cfg.Schedules {
		names = append(names, name)
	}
	sort.Strings(names)

	fmt.Printf("%-24s %-20s %-18s %-8s %-8s\n", "Pipeline", "Cron", "Timezone", "Mode", "Enabled")
	fmt.Println(strings.Repeat("-", 82))
	for _, name := range names {
		e := cfg.Schedules[name]
		mode := e.Mode
		if mode == "" {
			mode = "local"
		}
		fmt.Printf("%-24s %-20s %-18s %-8s %-8v\n", name, e.Cron, e.EffectiveTimezone(), mode, e.IsEnabled())
	}
	return nil
}

func runScheduleShow(cmd *cobra.Command, args []string) error {
	pipeline := args[0]

	if isCloudMode() {
		data, err := cloudGet("/api/v1/schedules/" + pipeline)
		if err != nil {
			return err
		}
		if wantJSON(cmd) {
			fmt.Println(string(data))
			return nil
		}
		var entry struct {
			Pipeline string `json:"pipeline"`
			Cron     string `json:"cron"`
			Timezone string `json:"timezone"`
			Mode     string `json:"mode"`
			Enabled  bool   `json:"enabled"`
		}
		if err := json.Unmarshal(data, &entry); err != nil {
			return err
		}
		fmt.Printf("Pipeline: %s\n", entry.Pipeline)
		fmt.Printf("Cron:     %s\n", entry.Cron)
		fmt.Printf("Timezone: %s\n", entry.Timezone)
		fmt.Printf("Mode:     %s\n", entry.Mode)
		fmt.Printf("Enabled:  %v\n", entry.Enabled)
		return nil
	}

	cfg, _, err := loadScheduleFile(cmd)
	if err != nil {
		return fmt.Errorf("loading schedule.yml: %w", err)
	}

	entry, ok := cfg.Schedules[pipeline]
	if !ok {
		return fmt.Errorf("pipeline %q not found in schedule.yml", pipeline)
	}

	mode := entry.Mode
	if mode == "" {
		mode = "local"
	}

	if wantJSON(cmd) {
		out := map[string]any{
			"pipeline": pipeline,
			"cron":     entry.Cron,
			"timezone": entry.EffectiveTimezone(),
			"mode":     mode,
			"enabled":  entry.IsEnabled(),
		}
		return outputJSON(out)
	}

	fmt.Printf("Pipeline: %s\n", pipeline)
	fmt.Printf("Cron:     %s\n", entry.Cron)
	fmt.Printf("Timezone: %s\n", entry.EffectiveTimezone())
	fmt.Printf("Mode:     %s\n", mode)
	fmt.Printf("Enabled:  %v\n", entry.IsEnabled())
	return nil
}

func runScheduleSet(cmd *cobra.Command, args []string) error {
	pipeline := args[0]
	cronExpr := args[1]
	tz, _ := cmd.Flags().GetString("timezone")
	mode, _ := cmd.Flags().GetString("mode")

	if isCloudMode() {
		body := map[string]any{"cron": cronExpr}
		if tz != "" {
			body["timezone"] = tz
		}
		if mode != "" {
			body["mode"] = mode
		}
		_, err := cloudPost("/api/v1/schedules/"+pipeline, body)
		if err != nil {
			return err
		}
		fmt.Printf("Set schedule for %s: %s\n", pipeline, cronExpr)
		return nil
	}

	path := resolveSchedulePath(cmd)

	var cfg schedule.ScheduleConfig
	if data, err := os.ReadFile(path); err == nil {
		if err := yaml.Unmarshal(data, &cfg); err != nil {
			return fmt.Errorf("parsing existing schedule.yml: %w", err)
		}
	}

	if cfg.Version == "" {
		cfg.Version = "1"
	}
	if cfg.Schedules == nil {
		cfg.Schedules = make(map[string]schedule.ScheduleEntry)
	}

	entry := cfg.Schedules[pipeline]
	entry.Cron = cronExpr
	if tz != "" {
		entry.Timezone = tz
	}
	if mode != "" {
		entry.Mode = mode
	}
	cfg.Schedules[pipeline] = entry

	data, err := yaml.Marshal(&cfg)
	if err != nil {
		return fmt.Errorf("marshaling schedule.yml: %w", err)
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("writing schedule.yml: %w", err)
	}

	fmt.Printf("Set schedule for %s: %s\n", pipeline, cronExpr)
	return nil
}

func runScheduleEnable(cmd *cobra.Command, args []string) error {
	return setScheduleEnabled(cmd, args[0], true)
}

func runScheduleDisable(cmd *cobra.Command, args []string) error {
	return setScheduleEnabled(cmd, args[0], false)
}

func setScheduleEnabled(cmd *cobra.Command, pipeline string, enabled bool) error {
	if isCloudMode() {
		body := map[string]any{"enabled": enabled}
		_, err := cloudPost("/api/v1/schedules/"+pipeline, body)
		if err != nil {
			return err
		}
		action := "enabled"
		if !enabled {
			action = "disabled"
		}
		fmt.Printf("Schedule %s for %s\n", action, pipeline)
		return nil
	}

	path := resolveSchedulePath(cmd)

	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("reading schedule.yml: %w", err)
	}

	var cfg schedule.ScheduleConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return fmt.Errorf("parsing schedule.yml: %w", err)
	}

	entry, ok := cfg.Schedules[pipeline]
	if !ok {
		return fmt.Errorf("pipeline %q not found in schedule.yml", pipeline)
	}

	entry.Enabled = &enabled
	cfg.Schedules[pipeline] = entry

	out, err := yaml.Marshal(&cfg)
	if err != nil {
		return fmt.Errorf("marshaling schedule.yml: %w", err)
	}

	if err := os.WriteFile(path, out, 0644); err != nil {
		return fmt.Errorf("writing schedule.yml: %w", err)
	}

	action := "enabled"
	if !enabled {
		action = "disabled"
	}
	fmt.Printf("Schedule %s for %s\n", action, pipeline)
	return nil
}

func runScheduleSync(cmd *cobra.Command, args []string) error {
	initMode, _ := cmd.Flags().GetBool("init")
	if !initMode {
		return fmt.Errorf("--init flag is required (scans pipeline.yaml files to generate schedule.yml)")
	}

	projectRoot, _ := cmd.Flags().GetString("project-root")
	configDir, _ := cmd.Flags().GetString("config-dir")
	if configDir == "" {
		configDir = projectRoot
	}

	entries, err := os.ReadDir(configDir)
	if err != nil {
		return fmt.Errorf("reading config directory %s: %w", configDir, err)
	}

	schedules := make(map[string]schedule.ScheduleEntry)

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".yaml") && !strings.HasSuffix(name, ".yml") {
			continue
		}

		path := filepath.Join(configDir, name)
		cfg, err := config.LoadConfig(path)
		if err != nil {
			continue
		}

		if cfg.Schedule != "" && cfg.Pipeline != "" {
			schedules[cfg.Pipeline] = schedule.ScheduleEntry{
				Cron: cfg.Schedule,
				Mode: "auto",
			}
		}
	}

	if len(schedules) == 0 {
		fmt.Println("No pipeline schedules found in config directory")
		return nil
	}

	scheduleCfg := schedule.ScheduleConfig{
		Version:   "1",
		Schedules: schedules,
	}

	data, err := yaml.Marshal(&scheduleCfg)
	if err != nil {
		return fmt.Errorf("marshaling schedule.yml: %w", err)
	}

	outputPath := filepath.Join(projectRoot, "schedule.yml")
	if err := os.WriteFile(outputPath, data, 0644); err != nil {
		return fmt.Errorf("writing schedule.yml: %w", err)
	}

	fmt.Printf("Generated %s with %d schedule(s):\n", outputPath, len(schedules))
	names := make([]string, 0, len(schedules))
	for name := range schedules {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		e := schedules[name]
		fmt.Printf("  %s: %s (mode: %s)\n", name, e.Cron, e.Mode)
	}
	return nil
}
