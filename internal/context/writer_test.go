package context

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

func TestCreateOrReplace_Empty(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), ".granicus", "context.db")
	if err := CreateOrReplace(dbPath, nil, nil, nil); err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for _, table := range []string{"schemas", "lineage", "assets"} {
		var count int
		if err := db.QueryRow("SELECT count(*) FROM " + table).Scan(&count); err != nil {
			t.Fatalf("table %s: %v", table, err)
		}
		if count != 0 {
			t.Errorf("table %s: expected 0 rows, got %d", table, count)
		}
	}
}

func TestCreateOrReplace_WithData(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), ".granicus", "context.db")

	schemas := []Schema{
		{Dataset: "dev_analytics", TableName: "orders", ColumnName: "id", DataType: "INT64", Ordinal: 1, Description: "Primary key"},
		{Dataset: "dev_analytics", TableName: "orders", ColumnName: "total", DataType: "FLOAT64", Ordinal: 2},
	}
	lineage := []Lineage{
		{SourceAsset: "stg_orders", TargetAsset: "int_orders", SourceDataset: "dev_staging", SourceTable: "orders", TargetDataset: "dev_analytics", TargetTable: "int_orders"},
	}
	assets := []Asset{
		{AssetName: "int_orders", Dataset: "dev_analytics", TableName: "int_orders", Layer: "intermediate", Grain: "order_id", Docstring: "Cleaned orders", DirectiveJSON: `{"materialized":"table"}`},
	}

	if err := CreateOrReplace(dbPath, schemas, lineage, assets); err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var schemaCount int
	db.QueryRow("SELECT count(*) FROM schemas").Scan(&schemaCount)
	if schemaCount != 2 {
		t.Errorf("schemas: expected 2, got %d", schemaCount)
	}

	var lineageCount int
	db.QueryRow("SELECT count(*) FROM lineage").Scan(&lineageCount)
	if lineageCount != 1 {
		t.Errorf("lineage: expected 1, got %d", lineageCount)
	}

	var assetCount int
	db.QueryRow("SELECT count(*) FROM assets").Scan(&assetCount)
	if assetCount != 1 {
		t.Errorf("assets: expected 1, got %d", assetCount)
	}

	var desc string
	db.QueryRow("SELECT description FROM schemas WHERE column_name = 'id'").Scan(&desc)
	if desc != "Primary key" {
		t.Errorf("description: expected 'Primary key', got %q", desc)
	}

	var dj string
	db.QueryRow("SELECT directive_json FROM assets WHERE asset_name = 'int_orders'").Scan(&dj)
	if dj != `{"materialized":"table"}` {
		t.Errorf("directive_json: %q", dj)
	}
}

func TestCreateOrReplace_Replaces(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), ".granicus", "context.db")

	schemas1 := []Schema{
		{Dataset: "d", TableName: "t", ColumnName: "a", DataType: "STRING", Ordinal: 1},
		{Dataset: "d", TableName: "t", ColumnName: "b", DataType: "STRING", Ordinal: 2},
	}
	if err := CreateOrReplace(dbPath, schemas1, nil, nil); err != nil {
		t.Fatal(err)
	}

	schemas2 := []Schema{
		{Dataset: "d", TableName: "t", ColumnName: "x", DataType: "INT64", Ordinal: 1},
	}
	if err := CreateOrReplace(dbPath, schemas2, nil, nil); err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var count int
	db.QueryRow("SELECT count(*) FROM schemas").Scan(&count)
	if count != 1 {
		t.Errorf("after replace: expected 1, got %d", count)
	}

	var col string
	db.QueryRow("SELECT column_name FROM schemas").Scan(&col)
	if col != "x" {
		t.Errorf("expected column 'x', got %q", col)
	}
}
