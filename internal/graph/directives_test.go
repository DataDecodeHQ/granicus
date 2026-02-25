package graph

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseDirectives_SQL(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sql")
	os.WriteFile(path, []byte(`-- granicus:
--   depends_on: [raw_transactions, dim_customers]
--   time_column: created_at
--   interval_unit: day
--   lookback: 2
--   start_date: "2024-01-01"
--   batch_size: 30
SELECT * FROM foo WHERE created_at >= @start AND created_at < @end;
`), 0644)

	d, err := ParseDirectives(path)
	if err != nil {
		t.Fatal(err)
	}

	if len(d.DependsOn) != 2 || d.DependsOn[0] != "raw_transactions" || d.DependsOn[1] != "dim_customers" {
		t.Errorf("depends_on: %v", d.DependsOn)
	}
	if d.TimeColumn != "created_at" {
		t.Errorf("time_column: %q", d.TimeColumn)
	}
	if d.IntervalUnit != "day" {
		t.Errorf("interval_unit: %q", d.IntervalUnit)
	}
	if d.Lookback != 2 {
		t.Errorf("lookback: %d", d.Lookback)
	}
	if d.StartDate != "2024-01-01" {
		t.Errorf("start_date: %q", d.StartDate)
	}
	if d.BatchSize != 30 {
		t.Errorf("batch_size: %d", d.BatchSize)
	}
}

func TestParseDirectives_Python(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.py")
	os.WriteFile(path, []byte(`# granicus:
#   depends_on: [stg_orders]
#   time_column: order_date
#   interval_unit: hour
#   produces: [user_events, order_events]
import pandas as pd
`), 0644)

	d, err := ParseDirectives(path)
	if err != nil {
		t.Fatal(err)
	}

	if len(d.DependsOn) != 1 || d.DependsOn[0] != "stg_orders" {
		t.Errorf("depends_on: %v", d.DependsOn)
	}
	if d.TimeColumn != "order_date" {
		t.Errorf("time_column: %q", d.TimeColumn)
	}
	if d.IntervalUnit != "hour" {
		t.Errorf("interval_unit: %q", d.IntervalUnit)
	}
	if len(d.Produces) != 2 || d.Produces[0] != "user_events" || d.Produces[1] != "order_events" {
		t.Errorf("produces: %v", d.Produces)
	}
}

func TestParseDirectives_NoBlock(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sh")
	os.WriteFile(path, []byte("#!/bin/bash\necho hello\n"), 0644)

	d, err := ParseDirectives(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(d.DependsOn) != 0 {
		t.Errorf("expected empty depends_on, got %v", d.DependsOn)
	}
	if d.TimeColumn != "" {
		t.Errorf("expected empty time_column, got %q", d.TimeColumn)
	}
}

func TestParseDirectives_PartialDirectives(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sql")
	os.WriteFile(path, []byte(`-- granicus:
--   depends_on: [upstream]
SELECT 1;
`), 0644)

	d, err := ParseDirectives(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(d.DependsOn) != 1 || d.DependsOn[0] != "upstream" {
		t.Errorf("depends_on: %v", d.DependsOn)
	}
	if d.TimeColumn != "" {
		t.Errorf("expected empty time_column, got %q", d.TimeColumn)
	}
}

func TestParseDirectives_MalformedYAML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sql")
	os.WriteFile(path, []byte(`-- granicus:
--   depends_on: [unclosed
--   time_column: created_at
SELECT 1;
`), 0644)

	_, err := ParseDirectives(path)
	if err == nil {
		t.Error("expected error for malformed YAML")
	}
}

func TestParseDirectives_MissingFile(t *testing.T) {
	_, err := ParseDirectives("/nonexistent/file.sql")
	if err == nil {
		t.Error("expected error for missing file")
	}
}

func TestParseDirectives_BackwardsCompat_OldFormat(t *testing.T) {
	// Old format: -- depends_on: asset_name (without YAML block)
	// Should return empty since there's no "granicus:" marker
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sql")
	os.WriteFile(path, []byte(`-- depends_on: old_asset
SELECT 1;
`), 0644)

	d, err := ParseDirectives(path)
	if err != nil {
		t.Fatal(err)
	}
	// Old format not supported by ParseDirectives — callers use ParseDependencies for backwards compat
	if len(d.DependsOn) != 0 {
		t.Errorf("old format should not be parsed by ParseDirectives: %v", d.DependsOn)
	}
}

func TestParseDirectives_Beyond50Lines(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sql")
	content := ""
	for i := 0; i < 50; i++ {
		content += "-- some comment\n"
	}
	content += "-- granicus:\n--   depends_on: [should_not_find]\n"
	os.WriteFile(path, []byte(content), 0644)

	d, err := ParseDirectives(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(d.DependsOn) != 0 {
		t.Errorf("should not find directives after line 50: %v", d.DependsOn)
	}
}
