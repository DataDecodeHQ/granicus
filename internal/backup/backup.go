package backup

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// BackupStateDB creates a consistent backup of the SQLite state database using VACUUM INTO.
func BackupStateDB(stateDBPath, outputPath string) (string, error) {
	if _, err := os.Stat(stateDBPath); os.IsNotExist(err) {
		return "", fmt.Errorf("state db not found: %s", stateDBPath)
	}

	if outputPath == "" {
		ts := time.Now().Format("20060102-150405")
		outputPath = stateDBPath + "." + ts + ".bak"
	}

	if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		return "", fmt.Errorf("creating backup dir: %w", err)
	}

	// Use VACUUM INTO for a safe, consistent backup
	db, err := sql.Open("sqlite", stateDBPath)
	if err != nil {
		return "", fmt.Errorf("opening state db: %w", err)
	}
	defer db.Close()

	_, err = db.Exec(fmt.Sprintf("VACUUM INTO '%s'", outputPath))
	if err != nil {
		return "", fmt.Errorf("backup failed: %w", err)
	}

	return outputPath, nil
}

// PruneBackups removes old state database backups from dir, keeping the most recent count specified by keep.
func PruneBackups(dir string, keep int) (int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, err
	}

	var backups []os.DirEntry
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".bak") && strings.Contains(e.Name(), "state.db.") {
			backups = append(backups, e)
		}
	}

	if len(backups) <= keep {
		return 0, nil
	}

	// Sort by name (which includes timestamp) in reverse order
	sort.Slice(backups, func(i, j int) bool {
		return backups[i].Name() > backups[j].Name()
	})

	pruned := 0
	for i := keep; i < len(backups); i++ {
		path := filepath.Join(dir, backups[i].Name())
		if err := os.Remove(path); err != nil {
			continue
		}
		pruned++
	}

	return pruned, nil
}
