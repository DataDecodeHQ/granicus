package gc

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type Result struct {
	RunsDeleted int
	BytesFreed  int64
	TestCleanup int
}

// dag:boundary
func Collect(projectRoot string, retentionDays int) (*Result, error) {
	result := &Result{}

	runsDir := filepath.Join(projectRoot, ".granicus", "runs")
	entries, err := os.ReadDir(runsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return result, nil
		}
		return nil, fmt.Errorf("reading runs dir: %w", err)
	}

	cutoff := time.Now().AddDate(0, 0, -retentionDays)

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}

		if info.ModTime().Before(cutoff) {
			dirPath := filepath.Join(runsDir, entry.Name())
			size := dirSize(dirPath)
			if err := os.RemoveAll(dirPath); err != nil {
				continue
			}
			result.RunsDeleted++
			result.BytesFreed += size
		}
	}

	// Clean up test artifacts
	testStateDB := filepath.Join(projectRoot, ".granicus", "test-state.db")
	if info, err := os.Stat(testStateDB); err == nil {
		if info.ModTime().Before(cutoff) {
			result.BytesFreed += info.Size()
			os.Remove(testStateDB)
			// Also remove WAL/SHM
			os.Remove(testStateDB + "-wal")
			os.Remove(testStateDB + "-shm")
			result.TestCleanup++
		}
	}

	// Clean up old test metadata files
	metaGlob := filepath.Join(projectRoot, ".granicus", "test-metadata-*.json")
	matches, _ := filepath.Glob(metaGlob)
	for _, m := range matches {
		if info, err := os.Stat(m); err == nil && info.ModTime().Before(cutoff) {
			result.BytesFreed += info.Size()
			os.Remove(m)
			result.TestCleanup++
		}
	}

	return result, nil
}

func dirSize(path string) int64 {
	var total int64
	filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		total += info.Size()
		return nil
	})
	return total
}

// FormatBytes formats a byte count as a human-readable string (B, KB, or MB).
func FormatBytes(b int64) string {
	if b < 1024 {
		return fmt.Sprintf("%d B", b)
	}
	if b < 1024*1024 {
		return fmt.Sprintf("%.1f KB", float64(b)/1024)
	}
	return fmt.Sprintf("%.1f MB", float64(b)/(1024*1024))
}
