package doctor

import (
	"database/sql"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/DataDecodeHQ/granicus/internal/config"
)

func TestCheckGoVersion(t *testing.T) {
	result := checkGoVersion()

	if result.Name != "go_version" {
		t.Errorf("expected name 'go_version', got %q", result.Name)
	}
	if result.Status != StatusPass {
		t.Errorf("expected status pass, got %q", result.Status)
	}
	if result.Message != runtime.Version() {
		t.Errorf("expected message %q, got %q", runtime.Version(), result.Message)
	}
}

func TestCheckStateDB_Valid(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "state.db")

	// Create a valid SQLite DB.
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to create test db: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("failed to ping test db: %v", err)
	}
	db.Close()

	result := checkStateDB(dbPath)

	if result.Name != "state.db" {
		t.Errorf("expected name 'state.db', got %q", result.Name)
	}
	if result.Status != StatusPass {
		t.Errorf("expected status pass, got %q: %s", result.Status, result.Message)
	}
	if !strings.Contains(result.Message, "integrity ok") {
		t.Errorf("expected message to contain 'integrity ok', got %q", result.Message)
	}
}

func TestCheckStateDB_Missing(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "subdir", "state.db")

	result := checkStateDB(dbPath)

	if result.Name != "state.db" {
		t.Errorf("expected name 'state.db', got %q", result.Name)
	}
	if result.Status != StatusPass {
		t.Errorf("expected status pass (dir writable), got %q: %s", result.Status, result.Message)
	}
	if !strings.Contains(result.Message, "not yet created") {
		t.Errorf("expected message to mention 'not yet created', got %q", result.Message)
	}

	// Confirm the directory was created.
	if _, err := os.Stat(filepath.Dir(dbPath)); err != nil {
		t.Errorf("expected directory to be created, got error: %v", err)
	}
}

func TestCheckEventsDB_Valid(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "events.db")

	result := checkEventsDB(dbPath)

	if result.Name != "events.db" {
		t.Errorf("expected name 'events.db', got %q", result.Name)
	}
	if result.Status != StatusPass {
		t.Errorf("expected status pass, got %q: %s", result.Status, result.Message)
	}

	// Confirm the file was created.
	if _, err := os.Stat(dbPath); err != nil {
		t.Errorf("expected events.db to exist after check, got error: %v", err)
	}
}

func TestCheckDiskSpace_Valid(t *testing.T) {
	dir := t.TempDir()

	result := checkDiskSpace(dir)

	if result.Name != "disk_space" {
		t.Errorf("expected name 'disk_space', got %q", result.Name)
	}
	// A temp dir almost certainly has more than 1 GB; expect pass or warn but not fail due to the dir itself.
	if result.Status == StatusFail && strings.Contains(result.Message, "cannot check") {
		t.Errorf("unexpected fail: %s", result.Message)
	}
}

func TestCheckGCSConfig_MissingBucket(t *testing.T) {
	conn := &config.ConnectionConfig{
		Type:       "gcs",
		Properties: map[string]string{},
	}

	result := checkGCSConfig("myconn", conn)

	if result.Name != "gcs:myconn" {
		t.Errorf("expected name 'gcs:myconn', got %q", result.Name)
	}
	if result.Status != StatusFail {
		t.Errorf("expected status fail, got %q", result.Status)
	}
	if !strings.Contains(result.Message, "missing bucket") {
		t.Errorf("expected message to mention 'missing bucket', got %q", result.Message)
	}
}

func TestCheckGCSConfig_MissingCredentials(t *testing.T) {
	conn := &config.ConnectionConfig{
		Type: "gcs",
		Properties: map[string]string{
			"bucket":      "my-bucket",
			"credentials": "/nonexistent/path/creds.json",
		},
	}

	result := checkGCSConfig("myconn", conn)

	if result.Name != "gcs:myconn" {
		t.Errorf("expected name 'gcs:myconn', got %q", result.Name)
	}
	if result.Status != StatusFail {
		t.Errorf("expected status fail for missing credentials file, got %q", result.Status)
	}
	if !strings.Contains(result.Message, "credentials not found") {
		t.Errorf("expected message to mention 'credentials not found', got %q", result.Message)
	}
}

func TestCheckGCSConfig_ValidBucketADC(t *testing.T) {
	// No credentials property — falls back to ADC. Should pass without hitting GCP.
	t.Setenv("GCS_SERVICE_ACCOUNT", "")

	conn := &config.ConnectionConfig{
		Type: "gcs",
		Properties: map[string]string{
			"bucket": "my-bucket",
		},
	}

	result := checkGCSConfig("myconn", conn)

	if result.Name != "gcs:myconn" {
		t.Errorf("expected name 'gcs:myconn', got %q", result.Name)
	}
	if result.Status != StatusPass {
		t.Errorf("expected status pass for ADC path, got %q: %s", result.Status, result.Message)
	}
	if !strings.Contains(result.Message, "ADC") {
		t.Errorf("expected message to mention ADC, got %q", result.Message)
	}
}

func TestCheckBQConnectivity_Skip(t *testing.T) {
	t.Skip("requires BQ credentials")
}

func TestRunChecks_NilConfig(t *testing.T) {
	dir := t.TempDir()

	results := RunChecks(nil, dir)

	if len(results) == 0 {
		t.Fatal("expected at least one result, got none")
	}

	// go_version must always be present.
	found := false
	for _, r := range results {
		if r.Name == "go_version" {
			found = true
			if r.Status != StatusPass {
				t.Errorf("go_version check should pass, got %q", r.Status)
			}
		}
	}
	if !found {
		t.Error("expected go_version check in results")
	}

	// state.db and events.db checks must also be present.
	names := make(map[string]bool)
	for _, r := range results {
		names[r.Name] = true
	}
	for _, required := range []string{"state.db", "events.db", "disk_space"} {
		if !names[required] {
			t.Errorf("expected %q check in results", required)
		}
	}
}
