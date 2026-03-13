package integration

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"database/sql"

	_ "modernc.org/sqlite"
)

const contextSchema = `
CREATE TABLE IF NOT EXISTS assets (
	name  TEXT PRIMARY KEY,
	layer TEXT,
	grain TEXT
);
CREATE TABLE IF NOT EXISTS schemas (
	table_name  TEXT,
	column_name TEXT,
	data_type   TEXT,
	PRIMARY KEY (table_name, column_name)
);
CREATE TABLE IF NOT EXISTS lineage (
	upstream   TEXT,
	downstream TEXT,
	PRIMARY KEY (upstream, downstream)
);
`

func createMinimalContextDB(t *testing.T, dbPath string) {
	t.Helper()
	dir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("opening context db: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(contextSchema); err != nil {
		t.Fatalf("creating context schema: %v", err)
	}

	if _, err := db.Exec(`INSERT INTO assets (name, layer, grain) VALUES ('stg_test', 'staging', 'id')`); err != nil {
		t.Fatalf("inserting asset: %v", err)
	}
}

func TestContextDBGlobFindsAllPipelines(t *testing.T) {
	tmpRoot := t.TempDir()

	pipelines := []string{"alpha", "beta", "gamma"}
	for _, name := range pipelines {
		dbPath := filepath.Join(tmpRoot, "project", "granicus_pipeline", name, ".granicus", "context.db")
		createMinimalContextDB(t, dbPath)
	}

	t.Setenv("ANALYTEHEALTH_ROOT", tmpRoot)

	root := os.Getenv("ANALYTEHEALTH_ROOT")
	pattern := filepath.Join(root, "project", "granicus_pipeline", "*", ".granicus", "context.db")

	matches, err := filepath.Glob(pattern)
	if err != nil {
		t.Fatalf("glob error: %v", err)
	}

	if len(matches) != len(pipelines) {
		t.Fatalf("expected %d context.db files, found %d", len(pipelines), len(matches))
	}

	found := map[string]bool{}
	for _, match := range matches {
		// Extract pipeline name: .../<pipeline>/.granicus/context.db
		parts := strings.Split(match, string(os.PathSeparator))
		for i, part := range parts {
			if part == ".granicus" && i > 0 {
				found[parts[i-1]] = true
				break
			}
		}
	}

	for _, name := range pipelines {
		if !found[name] {
			t.Errorf("pipeline %q not found via glob", name)
		}
	}
}

func TestContextDBPipelineNameExtraction(t *testing.T) {
	tmpRoot := t.TempDir()

	pipelines := []string{"alpha", "beta", "gamma"}
	for _, name := range pipelines {
		dbPath := filepath.Join(tmpRoot, "project", "granicus_pipeline", name, ".granicus", "context.db")
		createMinimalContextDB(t, dbPath)
	}

	t.Setenv("ANALYTEHEALTH_ROOT", tmpRoot)

	root := os.Getenv("ANALYTEHEALTH_ROOT")
	pattern := filepath.Join(root, "project", "granicus_pipeline", "*", ".granicus", "context.db")

	matches, err := filepath.Glob(pattern)
	if err != nil {
		t.Fatalf("glob error: %v", err)
	}

	pipelineBase := filepath.Join(root, "project", "granicus_pipeline")

	for _, match := range matches {
		rel, err := filepath.Rel(pipelineBase, match)
		if err != nil {
			t.Errorf("rel path error for %s: %v", match, err)
			continue
		}
		// rel is "<pipeline>/.granicus/context.db"
		pipelineName := strings.SplitN(rel, string(os.PathSeparator), 2)[0]

		validName := false
		for _, p := range pipelines {
			if p == pipelineName {
				validName = true
				break
			}
		}
		if !validName {
			t.Errorf("unexpected pipeline name extracted: %q from %s", pipelineName, match)
		}
	}
}

func TestContextDBIgnoresNonPipelineDirs(t *testing.T) {
	tmpRoot := t.TempDir()

	// Valid pipeline with context.db
	dbPath := filepath.Join(tmpRoot, "project", "granicus_pipeline", "alpha", ".granicus", "context.db")
	createMinimalContextDB(t, dbPath)

	// Directory without .granicus/context.db
	noCtxDir := filepath.Join(tmpRoot, "project", "granicus_pipeline", "no_context", ".granicus")
	if err := os.MkdirAll(noCtxDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Directory that's just a regular dir (no .granicus at all)
	if err := os.MkdirAll(filepath.Join(tmpRoot, "project", "granicus_pipeline", "plain_dir"), 0o755); err != nil {
		t.Fatal(err)
	}

	t.Setenv("ANALYTEHEALTH_ROOT", tmpRoot)

	root := os.Getenv("ANALYTEHEALTH_ROOT")
	pattern := filepath.Join(root, "project", "granicus_pipeline", "*", ".granicus", "context.db")

	matches, err := filepath.Glob(pattern)
	if err != nil {
		t.Fatalf("glob error: %v", err)
	}

	if len(matches) != 1 {
		t.Errorf("expected 1 context.db match, got %d: %v", len(matches), matches)
	}
}

func TestContextDBIsReadableAfterCreation(t *testing.T) {
	tmpRoot := t.TempDir()

	dbPath := filepath.Join(tmpRoot, "project", "granicus_pipeline", "alpha", ".granicus", "context.db")
	createMinimalContextDB(t, dbPath)

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("opening context db: %v", err)
	}
	defer db.Close()

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM assets").Scan(&count); err != nil {
		t.Fatalf("querying assets: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 asset row, got %d", count)
	}

	// Verify tables exist
	tables := []string{"assets", "schemas", "lineage"}
	for _, table := range tables {
		var name string
		err := db.QueryRow("SELECT name FROM sqlite_master WHERE type='table' AND name=?", table).Scan(&name)
		if err != nil {
			t.Errorf("table %q not found in context.db: %v", table, err)
		}
	}
}
