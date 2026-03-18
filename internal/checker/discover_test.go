package checker

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/DataDecodeHQ/granicus/internal/config"
)

func TestDiscoverChecks_MatchesFiles(t *testing.T) {
	dir := t.TempDir()
	checksDir := filepath.Join(dir, "checks")
	os.MkdirAll(checksDir, 0755)

	os.WriteFile(filepath.Join(checksDir, "check_stg_orders_no_dupes.sql"), []byte("SELECT 1"), 0644)
	os.WriteFile(filepath.Join(checksDir, "check_stg_orders_not_null.sql"), []byte("SELECT 1"), 0644)

	assets := []config.AssetConfig{
		{Name: "stg_orders", Type: "sql", Source: "sql/stg_orders.sql"},
		{Name: "stg_payments", Type: "sql", Source: "sql/stg_payments.sql"},
	}

	result := DiscoverChecks(dir, assets)

	if len(result[0].Checks) != 2 {
		t.Fatalf("expected 2 checks for stg_orders, got %d", len(result[0].Checks))
	}
	if len(result[1].Checks) != 0 {
		t.Errorf("expected 0 checks for stg_payments, got %d", len(result[1].Checks))
	}
}

func TestDiscoverChecks_NoDuplicates(t *testing.T) {
	dir := t.TempDir()
	checksDir := filepath.Join(dir, "checks")
	os.MkdirAll(checksDir, 0755)

	os.WriteFile(filepath.Join(checksDir, "check_stg_orders_no_dupes.sql"), []byte("SELECT 1"), 0644)

	assets := []config.AssetConfig{
		{
			Name: "stg_orders", Type: "sql", Source: "sql/stg_orders.sql",
			Checks: []config.CheckConfig{
				{Name: "check_stg_orders_no_dupes", Source: "checks/check_stg_orders_no_dupes.sql"},
			},
		},
	}

	result := DiscoverChecks(dir, assets)
	if len(result[0].Checks) != 1 {
		t.Errorf("expected 1 check (deduped), got %d", len(result[0].Checks))
	}
}

func TestDiscoverChecks_NoChecksDir(t *testing.T) {
	dir := t.TempDir()
	assets := []config.AssetConfig{
		{Name: "stg_orders", Type: "sql"},
	}

	result := DiscoverChecks(dir, assets)
	if len(result[0].Checks) != 0 {
		t.Errorf("expected 0 checks, got %d", len(result[0].Checks))
	}
}

func TestDiscoverChecks_PythonChecks(t *testing.T) {
	dir := t.TempDir()
	checksDir := filepath.Join(dir, "checks")
	os.MkdirAll(checksDir, 0755)

	os.WriteFile(filepath.Join(checksDir, "check_stg_orders_custom.py"), []byte("pass"), 0644)

	assets := []config.AssetConfig{
		{Name: "stg_orders", Type: "sql", Source: "sql/stg_orders.sql"},
	}

	result := DiscoverChecks(dir, assets)
	if len(result[0].Checks) != 1 {
		t.Fatalf("expected 1 check, got %d", len(result[0].Checks))
	}
}
