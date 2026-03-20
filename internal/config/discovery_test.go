package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDiscoverAssets_Basic(t *testing.T) {
	dir := t.TempDir()
	sqlDir := filepath.Join(dir, "sql", "staging")
	os.MkdirAll(sqlDir, 0755)
	os.WriteFile(filepath.Join(sqlDir, "stg_orders.sql"), []byte("SELECT 1"), 0644)
	os.WriteFile(filepath.Join(sqlDir, "stg_payments.sql"), []byte("SELECT 1"), 0644)
	os.WriteFile(filepath.Join(sqlDir, "check_stg_orders_dupes.sql"), []byte("SELECT 1"), 0644) // should be skipped

	paths := []DiscoveryPath{{Path: "sql", DefaultConnection: "bq"}}
	assets, err := DiscoverAssets(dir, paths)
	if err != nil {
		t.Fatal(err)
	}

	if len(assets) != 2 {
		t.Fatalf("expected 2 assets, got %d", len(assets))
	}

	for _, a := range assets {
		if a.Type != "sql" {
			t.Errorf("expected type sql, got %q", a.Type)
		}
		if a.DestinationResource != "bq" {
			t.Errorf("expected connection bq, got %q", a.DestinationResource)
		}
		if a.Layer != "staging" {
			t.Errorf("expected layer staging, got %q for %s", a.Layer, a.Name)
		}
	}
}

func TestDiscoverAssets_Excludes(t *testing.T) {
	dir := t.TempDir()
	sqlDir := filepath.Join(dir, "sql")
	os.MkdirAll(sqlDir, 0755)
	os.WriteFile(filepath.Join(sqlDir, "stg_orders.sql"), []byte("SELECT 1"), 0644)
	os.WriteFile(filepath.Join(sqlDir, "temp_debug.sql"), []byte("SELECT 1"), 0644)

	paths := []DiscoveryPath{{Path: "sql", Exclude: []string{"temp_*"}}}
	assets, err := DiscoverAssets(dir, paths)
	if err != nil {
		t.Fatal(err)
	}

	if len(assets) != 1 {
		t.Fatalf("expected 1 asset after exclude, got %d", len(assets))
	}
}

func TestDiscoverAssets_MissingDir(t *testing.T) {
	dir := t.TempDir()
	paths := []DiscoveryPath{{Path: "nonexistent"}}
	assets, err := DiscoverAssets(dir, paths)
	if err != nil {
		t.Fatal(err)
	}
	if len(assets) != 0 {
		t.Errorf("expected 0 assets, got %d", len(assets))
	}
}

func TestMergeDiscoveredAssets(t *testing.T) {
	explicit := []AssetConfig{
		{Name: "stg_orders", Type: "sql", Source: "sql/stg_orders.sql"},
	}
	discovered := []AssetConfig{
		{Name: "stg_orders", Type: "sql", Source: "discovered/stg_orders.sql"},
		{Name: "stg_payments", Type: "sql", Source: "discovered/stg_payments.sql"},
	}

	merged := MergeDiscoveredAssets(explicit, discovered)
	if len(merged) != 2 {
		t.Fatalf("expected 2 merged, got %d", len(merged))
	}
	// Explicit should take precedence
	if merged[0].Source != "sql/stg_orders.sql" {
		t.Errorf("explicit should win: %q", merged[0].Source)
	}
}
