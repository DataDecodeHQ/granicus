package runner

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/analytehealth/granicus/internal/config"
)

func TestSQLRunner_TemplateSubstitution(t *testing.T) {
	dir := t.TempDir()
	sqlContent := `CREATE OR REPLACE TABLE {{.Project}}.{{.Dataset}}.test AS SELECT 1`
	os.WriteFile(filepath.Join(dir, "test.sql"), []byte(sqlContent), 0644)

	conn := &config.ConnectionConfig{
		Name: "bq",
		Type: "bigquery",
		Properties: map[string]string{
			"project": "my-project",
			"dataset": "my_dataset",
		},
	}
	runner := NewSQLRunner(conn)

	// We can't test actual BQ execution in unit tests, but we can test
	// that it reads the file and attempts to render the template.
	// The BQ client creation will fail without credentials, which is expected.
	result := runner.Run(&Asset{Name: "test", Type: "sql", Source: "test.sql"}, dir, "run1")

	// It should fail at BQ client creation (no credentials), but the template should have been rendered
	if result.Status != "failed" {
		t.Logf("result: %+v", result)
	}
	// The error should be about BQ, not about template parsing
	if strings.Contains(result.Error, "template") {
		t.Errorf("should not have template error: %s", result.Error)
	}
}

func TestSQLRunner_MissingFile(t *testing.T) {
	conn := &config.ConnectionConfig{
		Name:       "bq",
		Type:       "bigquery",
		Properties: map[string]string{"project": "p", "dataset": "d"},
	}
	runner := NewSQLRunner(conn)
	result := runner.Run(&Asset{Name: "missing", Type: "sql", Source: "nonexistent.sql"}, "/tmp", "run1")
	if result.Status != "failed" {
		t.Errorf("expected failed, got %s", result.Status)
	}
	if !strings.Contains(result.Error, "reading SQL") {
		t.Errorf("expected file read error: %s", result.Error)
	}
}

func TestSQLRunner_BadTemplate(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "bad.sql"), []byte(`SELECT {{.Invalid`), 0644)

	conn := &config.ConnectionConfig{
		Name:       "bq",
		Type:       "bigquery",
		Properties: map[string]string{"project": "p", "dataset": "d"},
	}
	runner := NewSQLRunner(conn)
	result := runner.Run(&Asset{Name: "bad", Type: "sql", Source: "bad.sql"}, dir, "run1")
	if result.Status != "failed" {
		t.Errorf("expected failed, got %s", result.Status)
	}
	if !strings.Contains(result.Error, "template") {
		t.Errorf("expected template error: %s", result.Error)
	}
}
