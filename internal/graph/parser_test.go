package graph

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func writeFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestParseDependencies_SQL(t *testing.T) {
	dir := t.TempDir()
	path := writeFile(t, dir, "test.sql", `-- depends_on: raw_transactions
-- depends_on: dim_customers
SELECT * FROM foo;
`)
	deps, err := ParseDependencies(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(deps) != 2 {
		t.Fatalf("expected 2 deps, got %d", len(deps))
	}
	if deps[0] != "raw_transactions" || deps[1] != "dim_customers" {
		t.Errorf("deps: %v", deps)
	}
}

func TestParseDependencies_Python(t *testing.T) {
	dir := t.TempDir()
	path := writeFile(t, dir, "test.py", `# depends_on: stg_transactions
# depends_on: stg_customers
import pandas
`)
	deps, err := ParseDependencies(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(deps) != 2 || deps[0] != "stg_transactions" || deps[1] != "stg_customers" {
		t.Errorf("deps: %v", deps)
	}
}

func TestParseDependencies_NoDeps(t *testing.T) {
	dir := t.TempDir()
	path := writeFile(t, dir, "test.sh", `#!/bin/bash
echo hello
`)
	deps, err := ParseDependencies(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(deps) != 0 {
		t.Errorf("expected 0 deps, got %v", deps)
	}
}

func TestParseDependencies_OnlyFirst50Lines(t *testing.T) {
	dir := t.TempDir()
	content := ""
	for i := 0; i < 50; i++ {
		content += fmt.Sprintf("-- line %d\n", i+1)
	}
	content += "-- depends_on: should_not_find\n"

	path := writeFile(t, dir, "test.sql", content)
	deps, err := ParseDependencies(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(deps) != 0 {
		t.Errorf("should not find dep on line 51, got %v", deps)
	}
}

func TestParseDependencies_MissingFile(t *testing.T) {
	_, err := ParseDependencies("/nonexistent/file.sql")
	if err == nil {
		t.Error("expected error for missing file")
	}
}

func TestParseDependencies_WhitespaceVariants(t *testing.T) {
	dir := t.TempDir()
	path := writeFile(t, dir, "test.sql", `  -- depends_on: asset_a
  --  depends_on:  asset_b
`)
	deps, err := ParseDependencies(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(deps) != 2 || deps[0] != "asset_a" || deps[1] != "asset_b" {
		t.Errorf("deps: %v", deps)
	}
}
