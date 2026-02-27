package monitor

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

const ddl = `
CREATE TABLE IF NOT EXISTS current_errors (
	pipeline     TEXT NOT NULL,
	asset        TEXT NOT NULL,
	check_name   TEXT NOT NULL,
	severity     TEXT NOT NULL,
	message      TEXT NOT NULL,
	details_json TEXT NOT NULL DEFAULT '{}',
	run_at       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS metric_snapshots (
	pipeline      TEXT NOT NULL,
	table_name    TEXT NOT NULL,
	column_name   TEXT NOT NULL,
	metric_name   TEXT NOT NULL,
	metric_value  REAL NOT NULL,
	segment_value TEXT NOT NULL DEFAULT '',
	captured_at   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS distribution_flags (
	pipeline      TEXT NOT NULL,
	table_name    TEXT NOT NULL,
	column_name   TEXT NOT NULL,
	metric_name   TEXT NOT NULL,
	window        TEXT NOT NULL,
	current_value REAL NOT NULL,
	prior_value   REAL NOT NULL,
	pct_change    REAL NOT NULL,
	severity      TEXT NOT NULL,
	captured_at   TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_snapshots_lookup
	ON metric_snapshots(table_name, column_name, metric_name, captured_at);
`

type CurrentError struct {
	Pipeline    string
	Asset       string
	CheckName   string
	Severity    string
	Message     string
	DetailsJSON string
	RunAt       string
}

type MetricSnapshot struct {
	Pipeline     string
	TableName    string
	ColumnName   string
	MetricName   string
	MetricValue  float64
	SegmentValue string
	CapturedAt   string
}

type DistributionFlag struct {
	Pipeline     string
	TableName    string
	ColumnName   string
	MetricName   string
	Window       string
	CurrentValue float64
	PriorValue   float64
	PctChange    float64
	Severity     string
	CapturedAt   string
}

func openDB(dbPath string) (*sql.DB, error) {
	dir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("creating monitor dir: %w", err)
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("opening monitor db: %w", err)
	}

	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("enabling WAL: %w", err)
	}

	if _, err := db.Exec(ddl); err != nil {
		db.Close()
		return nil, fmt.Errorf("creating schema: %w", err)
	}

	return db, nil
}

func WriteCurrentErrors(dbPath string, errors []CurrentError) error {
	db, err := openDB(dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.Exec("DELETE FROM current_errors"); err != nil {
		return fmt.Errorf("clearing current_errors: %w", err)
	}

	if err := insertCurrentErrors(tx, errors); err != nil {
		return err
	}

	return tx.Commit()
}

func AppendSnapshots(dbPath string, snapshots []MetricSnapshot) error {
	db, err := openDB(dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	if err := insertSnapshots(tx, snapshots); err != nil {
		return err
	}

	return tx.Commit()
}

func AppendFlags(dbPath string, flags []DistributionFlag) error {
	db, err := openDB(dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	if err := insertFlags(tx, flags); err != nil {
		return err
	}

	if err := insertFlagsAsErrors(tx, flags); err != nil {
		return err
	}

	return tx.Commit()
}

func insertCurrentErrors(tx *sql.Tx, errors []CurrentError) error {
	if len(errors) == 0 {
		return nil
	}
	stmt, err := tx.Prepare(`INSERT INTO current_errors (pipeline, asset, check_name, severity, message, details_json, run_at) VALUES (?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("prepare current_errors: %w", err)
	}
	defer stmt.Close()

	for i, e := range errors {
		if _, err := stmt.Exec(e.Pipeline, e.Asset, e.CheckName, e.Severity, e.Message, e.DetailsJSON, e.RunAt); err != nil {
			return fmt.Errorf("inserting current_error row %d: %w", i, err)
		}
	}
	return nil
}

func insertSnapshots(tx *sql.Tx, snapshots []MetricSnapshot) error {
	if len(snapshots) == 0 {
		return nil
	}
	stmt, err := tx.Prepare(`INSERT INTO metric_snapshots (pipeline, table_name, column_name, metric_name, metric_value, segment_value, captured_at) VALUES (?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("prepare metric_snapshots: %w", err)
	}
	defer stmt.Close()

	for i, s := range snapshots {
		if _, err := stmt.Exec(s.Pipeline, s.TableName, s.ColumnName, s.MetricName, s.MetricValue, s.SegmentValue, s.CapturedAt); err != nil {
			return fmt.Errorf("inserting snapshot row %d: %w", i, err)
		}
	}
	return nil
}

func insertFlags(tx *sql.Tx, flags []DistributionFlag) error {
	if len(flags) == 0 {
		return nil
	}
	stmt, err := tx.Prepare(`INSERT INTO distribution_flags (pipeline, table_name, column_name, metric_name, window, current_value, prior_value, pct_change, severity, captured_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("prepare distribution_flags: %w", err)
	}
	defer stmt.Close()

	for i, f := range flags {
		if _, err := stmt.Exec(f.Pipeline, f.TableName, f.ColumnName, f.MetricName, f.Window, f.CurrentValue, f.PriorValue, f.PctChange, f.Severity, f.CapturedAt); err != nil {
			return fmt.Errorf("inserting flag row %d: %w", i, err)
		}
	}
	return nil
}

func insertFlagsAsErrors(tx *sql.Tx, flags []DistributionFlag) error {
	if len(flags) == 0 {
		return nil
	}
	stmt, err := tx.Prepare(`INSERT INTO current_errors (pipeline, asset, check_name, severity, message, details_json, run_at) VALUES (?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("prepare flag->error: %w", err)
	}
	defer stmt.Close()

	for i, f := range flags {
		checkName := fmt.Sprintf("distribution_%s_%s_%s", f.TableName, f.ColumnName, f.MetricName)
		message := fmt.Sprintf("%s %s.%s %s changed %.1f%% over %s", f.Severity, f.TableName, f.ColumnName, f.MetricName, f.PctChange*100, f.Window)
		detailsJSON := fmt.Sprintf(`{"window":"%s","current_value":%g,"prior_value":%g,"pct_change":%g}`, f.Window, f.CurrentValue, f.PriorValue, f.PctChange)
		if _, err := stmt.Exec(f.Pipeline, f.TableName, checkName, f.Severity, message, detailsJSON, f.CapturedAt); err != nil {
			return fmt.Errorf("inserting flag-as-error row %d: %w", i, err)
		}
	}
	return nil
}
