package context

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

const ddl = `
CREATE TABLE schemas (
	dataset     TEXT NOT NULL,
	table_name  TEXT NOT NULL,
	column_name TEXT NOT NULL,
	data_type   TEXT NOT NULL,
	ordinal     INTEGER NOT NULL,
	description TEXT NOT NULL DEFAULT ''
);

CREATE TABLE lineage (
	source_asset   TEXT NOT NULL,
	target_asset   TEXT NOT NULL,
	source_dataset TEXT NOT NULL,
	source_table   TEXT NOT NULL,
	target_dataset TEXT NOT NULL,
	target_table   TEXT NOT NULL
);

CREATE TABLE assets (
	asset_name     TEXT NOT NULL,
	dataset        TEXT NOT NULL,
	table_name     TEXT NOT NULL,
	layer          TEXT NOT NULL DEFAULT '',
	grain          TEXT NOT NULL DEFAULT '',
	docstring      TEXT NOT NULL DEFAULT '',
	directive_json TEXT NOT NULL DEFAULT '{}'
);
`

func CreateOrReplace(dbPath string, schemas []Schema, lineage []Lineage, assets []Asset) error {
	dir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("creating context dir: %w", err)
	}

	tmpPath := dbPath + ".tmp"
	defer os.Remove(tmpPath)

	db, err := sql.Open("sqlite", tmpPath)
	if err != nil {
		return fmt.Errorf("opening temp context db: %w", err)
	}
	defer db.Close()

	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		return fmt.Errorf("enabling WAL: %w", err)
	}

	if _, err := db.Exec(ddl); err != nil {
		return fmt.Errorf("creating schema: %w", err)
	}

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	if err := insertSchemas(tx, schemas); err != nil {
		return err
	}
	if err := insertLineage(tx, lineage); err != nil {
		return err
	}
	if err := insertAssets(tx, assets); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	if err := db.Close(); err != nil {
		return fmt.Errorf("closing temp db: %w", err)
	}

	if err := os.Rename(tmpPath, dbPath); err != nil {
		return fmt.Errorf("atomic rename: %w", err)
	}

	return nil
}

func insertSchemas(tx *sql.Tx, schemas []Schema) error {
	if len(schemas) == 0 {
		return nil
	}
	stmt, err := tx.Prepare(`INSERT INTO schemas (dataset, table_name, column_name, data_type, ordinal, description) VALUES (?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("prepare schemas: %w", err)
	}
	defer stmt.Close()

	for i, s := range schemas {
		if _, err := stmt.Exec(s.Dataset, s.TableName, s.ColumnName, s.DataType, s.Ordinal, s.Description); err != nil {
			return fmt.Errorf("inserting schema row %d: %w", i, err)
		}
	}
	return nil
}

func insertLineage(tx *sql.Tx, lineage []Lineage) error {
	if len(lineage) == 0 {
		return nil
	}
	stmt, err := tx.Prepare(`INSERT INTO lineage (source_asset, target_asset, source_dataset, source_table, target_dataset, target_table) VALUES (?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("prepare lineage: %w", err)
	}
	defer stmt.Close()

	for i, l := range lineage {
		if _, err := stmt.Exec(l.SourceAsset, l.TargetAsset, l.SourceDataset, l.SourceTable, l.TargetDataset, l.TargetTable); err != nil {
			return fmt.Errorf("inserting lineage row %d: %w", i, err)
		}
	}
	return nil
}

func insertAssets(tx *sql.Tx, assets []Asset) error {
	if len(assets) == 0 {
		return nil
	}
	stmt, err := tx.Prepare(`INSERT INTO assets (asset_name, dataset, table_name, layer, grain, docstring, directive_json) VALUES (?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("prepare assets: %w", err)
	}
	defer stmt.Close()

	for i, a := range assets {
		if _, err := stmt.Exec(a.AssetName, a.Dataset, a.TableName, a.Layer, a.Grain, a.Docstring, a.DirectiveJSON); err != nil {
			return fmt.Errorf("inserting asset row %d: %w", i, err)
		}
	}
	return nil
}
