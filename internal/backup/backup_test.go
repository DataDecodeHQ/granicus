package backup

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

func TestBackupStateDB(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "state.db")

	// Create a real SQLite DB
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY)")
	db.Exec("INSERT INTO test VALUES (1)")
	db.Close()

	backupPath, err := BackupStateDB(dbPath, "")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(backupPath); err != nil {
		t.Fatalf("backup file not created: %v", err)
	}

	// Verify backup is a valid SQLite DB
	bdb, err := sql.Open("sqlite", backupPath)
	if err != nil {
		t.Fatal(err)
	}
	defer bdb.Close()

	var count int
	bdb.QueryRow("SELECT COUNT(*) FROM test").Scan(&count)
	if count != 1 {
		t.Errorf("expected 1 row in backup, got %d", count)
	}
}

func TestBackupStateDB_MissingDB(t *testing.T) {
	_, err := BackupStateDB("/nonexistent/state.db", "")
	if err == nil {
		t.Error("expected error for missing db")
	}
}

func TestPruneBackups(t *testing.T) {
	dir := t.TempDir()

	// Create 5 backup files
	for i := 0; i < 5; i++ {
		name := filepath.Join(dir, "state.db.2026020"+string(rune('1'+i))+"-000000.bak")
		os.WriteFile(name, []byte("x"), 0644)
	}

	pruned, err := PruneBackups(dir, 3)
	if err != nil {
		t.Fatal(err)
	}

	if pruned != 2 {
		t.Errorf("expected 2 pruned, got %d", pruned)
	}

	// Check remaining
	entries, _ := os.ReadDir(dir)
	count := 0
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".bak" {
			count++
		}
	}
	if count != 3 {
		t.Errorf("expected 3 remaining, got %d", count)
	}
}
