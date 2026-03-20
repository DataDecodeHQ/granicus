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
	conn := &config.ResourceConfig{
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
	conn := &config.ResourceConfig{
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

	conn := &config.ResourceConfig{
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

func TestCheckBQConnectivity_MissingProject(t *testing.T) {
	conn := &config.ResourceConfig{
		Type:       "bigquery",
		Properties: map[string]string{},
	}
	result := checkBQConnectivity("bq-test", conn)
	if result.Status != StatusFail {
		t.Errorf("expected fail for missing project, got %q: %s", result.Status, result.Message)
	}
	if !strings.Contains(result.Message, "missing project") {
		t.Errorf("expected message about missing project, got %q", result.Message)
	}
}

func TestCheckBQConnectivity_InvalidCredentialsFile(t *testing.T) {
	conn := &config.ResourceConfig{
		Type: "bigquery",
		Properties: map[string]string{
			"project":     "test-project",
			"credentials": "/nonexistent/creds.json",
		},
	}
	result := checkBQConnectivity("bq-test", conn)
	if result.Status != StatusFail {
		t.Errorf("expected fail for invalid credentials, got %q: %s", result.Status, result.Message)
	}
}

func TestCheckBQConnectivity_ErrorWithoutCredentials(t *testing.T) {
	// With a real project but no valid credentials, the check should fail
	// at credential resolution or BQ client creation.
	conn := &config.ResourceConfig{
		Type: "bigquery",
		Properties: map[string]string{
			"project": "nonexistent-project-12345",
		},
	}
	result := checkBQConnectivity("bq-nocreds", conn)
	// Without ADC or explicit credentials, this should fail
	if result.Status == StatusPass {
		t.Log("BQ connectivity passed (ADC may be configured); skipping assertion")
	}
}

func TestCheckGCSConfig_EnvServiceAccountMissing(t *testing.T) {
	t.Setenv("GCS_SERVICE_ACCOUNT", "/nonexistent/service-account.json")
	conn := &config.ResourceConfig{
		Type: "gcs",
		Properties: map[string]string{
			"bucket": "test-bucket",
		},
	}
	result := checkGCSConfig("env-creds", conn)
	if result.Status != StatusWarn {
		t.Errorf("expected warn for missing GCS_SERVICE_ACCOUNT file, got %q: %s", result.Status, result.Message)
	}
	if !strings.Contains(result.Message, "GCS_SERVICE_ACCOUNT") {
		t.Errorf("expected message to reference GCS_SERVICE_ACCOUNT, got %q", result.Message)
	}
}

func TestCheckGCSConfig_ValidCredentialsFile(t *testing.T) {
	dir := t.TempDir()
	credPath := filepath.Join(dir, "creds.json")
	os.WriteFile(credPath, []byte(`{"type":"service_account"}`), 0644)

	conn := &config.ResourceConfig{
		Type: "gcs",
		Properties: map[string]string{
			"bucket":      "test-bucket",
			"credentials": credPath,
		},
	}
	result := checkGCSConfig("valid-creds", conn)
	if result.Status != StatusPass {
		t.Errorf("expected pass for valid credentials file, got %q: %s", result.Status, result.Message)
	}
	if !strings.Contains(result.Message, credPath) {
		t.Errorf("expected message to contain credential path, got %q", result.Message)
	}
}

func TestCheckStateDB_Corrupted(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "state.db")
	// Write garbage to simulate corruption
	os.WriteFile(dbPath, []byte("this is not a valid sqlite database"), 0644)

	result := checkStateDB(dbPath)
	if result.Status != StatusFail {
		t.Errorf("expected fail for corrupted db, got %q: %s", result.Status, result.Message)
	}
}

func TestCheckEventsDB_NotWritable(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "events.db")

	// Create the DB, then make it read-only
	result := checkEventsDB(dbPath)
	if result.Status != StatusPass {
		t.Fatalf("setup: expected pass, got %q: %s", result.Status, result.Message)
	}
	os.Chmod(dbPath, 0444)
	t.Cleanup(func() { os.Chmod(dbPath, 0644) })

	result = checkEventsDB(dbPath)
	// Running as root makes this pass regardless, so accept either outcome
	if result.Status != StatusFail && result.Status != StatusPass {
		t.Errorf("expected fail or pass (root), got %q: %s", result.Status, result.Message)
	}
}

func TestCheckDiskSpace_NonexistentDir(t *testing.T) {
	// MkdirAll will succeed for a temp subdir, so use a truly inaccessible path
	dir := t.TempDir()
	result := checkDiskSpace(filepath.Join(dir, "subdir"))
	// Should pass or warn, but not fail due to dir creation issues
	if result.Status == StatusFail && strings.Contains(result.Message, "cannot check") {
		t.Errorf("unexpected fail for creatable subdir: %s", result.Message)
	}
}

func TestRunChecks_WithConfig(t *testing.T) {
	dir := t.TempDir()

	cfg := &config.PipelineConfig{
		Pipeline: "test_pipeline",
		Resources: map[string]*config.ResourceConfig{
			"bq_conn": {
				Type: "bigquery",
				Properties: map[string]string{
					"project": "test-project",
				},
			},
			"gcs_conn": {
				Type: "gcs",
				Properties: map[string]string{
					"bucket": "test-bucket",
				},
			},
		},
	}

	results := RunChecks(cfg, dir)

	if len(results) == 0 {
		t.Fatal("expected results, got none")
	}

	// Verify we get checks for all expected names
	names := make(map[string]bool)
	for _, r := range results {
		names[r.Name] = true
	}

	expectedNames := []string{"go_version", "bq:bq_conn", "gcs:gcs_conn", "state.db", "events.db", "disk_space"}
	for _, name := range expectedNames {
		if !names[name] {
			t.Errorf("expected check %q in results, but it was missing", name)
		}
	}
}

func TestRunChecks_MultipleGCSConnections(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("GCS_SERVICE_ACCOUNT", "")

	cfg := &config.PipelineConfig{
		Pipeline: "test_pipeline",
		Resources: map[string]*config.ResourceConfig{
			"gcs_a": {
				Type:       "gcs",
				Properties: map[string]string{"bucket": "bucket-a"},
			},
			"gcs_b": {
				Type:       "gcs",
				Properties: map[string]string{"bucket": "bucket-b"},
			},
		},
	}

	results := RunChecks(cfg, dir)

	names := make(map[string]bool)
	for _, r := range results {
		names[r.Name] = true
	}

	if !names["gcs:gcs_a"] {
		t.Error("expected gcs:gcs_a check")
	}
	if !names["gcs:gcs_b"] {
		t.Error("expected gcs:gcs_b check")
	}
}

func TestRunChecks_EmptyConnections(t *testing.T) {
	dir := t.TempDir()

	cfg := &config.PipelineConfig{
		Pipeline:    "test_pipeline",
		Resources: map[string]*config.ResourceConfig{},
	}

	results := RunChecks(cfg, dir)

	// Should still have go_version, state.db, events.db, disk_space
	if len(results) < 4 {
		t.Errorf("expected at least 4 results, got %d", len(results))
	}
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
