package runner

import (
	"strings"
	"testing"
)

func baseEnv() []string {
	return []string{
		"GRANICUS_ASSET_NAME=my-asset",
		"GRANICUS_RUN_ID=run-123",
		"GRANICUS_PROJECT_ROOT=/tmp/project",
		"GRANICUS_METADATA_PATH=/tmp/meta.json",
	}
}

func TestValidateEnv_AllRequired(t *testing.T) {
	err := validateEnv(baseEnv(), "my-asset", "run-123")
	if err != nil {
		t.Errorf("expected nil error with all required vars present, got: %v", err)
	}
}

func TestValidateEnv_MissingAssetName(t *testing.T) {
	env := []string{
		"GRANICUS_ASSET_NAME=",
		"GRANICUS_RUN_ID=run-123",
		"GRANICUS_PROJECT_ROOT=/tmp/project",
		"GRANICUS_METADATA_PATH=/tmp/meta.json",
	}
	err := validateEnv(env, "", "run-123")
	if err == nil {
		t.Fatal("expected error for empty GRANICUS_ASSET_NAME, got nil")
	}
	if !strings.Contains(err.Error(), "GRANICUS_ASSET_NAME") {
		t.Errorf("error should mention GRANICUS_ASSET_NAME, got: %v", err)
	}
}

func TestValidateEnv_MissingRunID(t *testing.T) {
	env := []string{
		"GRANICUS_ASSET_NAME=my-asset",
		"GRANICUS_RUN_ID=",
		"GRANICUS_PROJECT_ROOT=/tmp/project",
		"GRANICUS_METADATA_PATH=/tmp/meta.json",
	}
	err := validateEnv(env, "my-asset", "")
	if err == nil {
		t.Fatal("expected error for empty GRANICUS_RUN_ID, got nil")
	}
	if !strings.Contains(err.Error(), "GRANICUS_RUN_ID") {
		t.Errorf("error should mention GRANICUS_RUN_ID, got: %v", err)
	}
}

func TestValidateEnv_InvalidConnectionJSON(t *testing.T) {
	env := append(baseEnv(), "GRANICUS_DEST_RESOURCE={not-valid-json")
	err := validateEnv(env, "my-asset", "run-123")
	if err == nil {
		t.Fatal("expected error for malformed GRANICUS_DEST_RESOURCE, got nil")
	}
	if !strings.Contains(err.Error(), "GRANICUS_DEST_RESOURCE") {
		t.Errorf("error should mention GRANICUS_DEST_RESOURCE, got: %v", err)
	}
}

func TestValidateEnv_ValidConnectionJSON(t *testing.T) {
	env := append(baseEnv(), `GRANICUS_DEST_RESOURCE={"name":"bq","type":"bigquery","project":"my-project"}`)
	err := validateEnv(env, "my-asset", "run-123")
	if err != nil {
		t.Errorf("expected nil error with valid connection JSON, got: %v", err)
	}
}

func TestValidateEnv_ValidInterval(t *testing.T) {
	env := append(baseEnv(), "GRANICUS_INTERVAL_START=2025-01-01T00:00:00Z", "GRANICUS_INTERVAL_END=2025-01-02T00:00:00Z")
	err := validateEnv(env, "my-asset", "run-123")
	if err != nil {
		t.Errorf("expected nil error with valid interval, got: %v", err)
	}
}

func TestValidateEnv_InvalidIntervalFormat(t *testing.T) {
	env := append(baseEnv(), "GRANICUS_INTERVAL_START=2025-01-01", "GRANICUS_INTERVAL_END=2025-01-02")
	err := validateEnv(env, "my-asset", "run-123")
	if err == nil {
		t.Fatal("expected error for date-only interval format, got nil")
	}
	if !strings.Contains(err.Error(), "GRANICUS_INTERVAL_START") {
		t.Errorf("error should mention GRANICUS_INTERVAL_START, got: %v", err)
	}
}

func TestValidateEnv_ConnectionMissingName(t *testing.T) {
	env := append(baseEnv(), `GRANICUS_DEST_RESOURCE={"type":"bigquery"}`)
	err := validateEnv(env, "my-asset", "run-123")
	if err == nil {
		t.Fatal("expected error for connection missing 'name', got nil")
	}
	if !strings.Contains(err.Error(), "missing required field 'name'") {
		t.Errorf("error should mention missing 'name', got: %v", err)
	}
}

func TestValidateEnv_ConnectionMissingType(t *testing.T) {
	env := append(baseEnv(), `GRANICUS_DEST_RESOURCE={"name":"bq"}`)
	err := validateEnv(env, "my-asset", "run-123")
	if err == nil {
		t.Fatal("expected error for connection missing 'type', got nil")
	}
	if !strings.Contains(err.Error(), "missing required field 'type'") {
		t.Errorf("error should mention missing 'type', got: %v", err)
	}
}

func TestValidateEnv_EmptyEnv(t *testing.T) {
	err := validateEnv([]string{}, "", "")
	if err == nil {
		t.Fatal("expected error for completely empty env, got nil")
	}
	for _, required := range []string{
		"GRANICUS_ASSET_NAME",
		"GRANICUS_RUN_ID",
		"GRANICUS_PROJECT_ROOT",
		"GRANICUS_METADATA_PATH",
	} {
		if !strings.Contains(err.Error(), required) {
			t.Errorf("error should mention missing var %s, got: %v", required, err)
		}
	}
}
