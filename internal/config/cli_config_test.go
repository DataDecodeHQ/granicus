package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolveCLIConfig_EnvOverridesFile(t *testing.T) {
	dir := t.TempDir()

	// Write a user config
	userDir := filepath.Join(dir, ".granicus")
	os.MkdirAll(userDir, 0700)
	os.WriteFile(filepath.Join(userDir, "config.json"), []byte(`{"endpoint":"file-endpoint","api_key":"file-key"}`), 0600)

	t.Setenv("HOME", dir)
	t.Setenv("GRANICUS_API_KEY", "env-key")
	t.Setenv("GRANICUS_ENDPOINT", "")

	cfg, err := ResolveCLIConfig("", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.APIKey != "env-key" {
		t.Errorf("expected env-key, got %s", cfg.APIKey)
	}
	if cfg.Endpoint != "file-endpoint" {
		t.Errorf("expected file-endpoint, got %s", cfg.Endpoint)
	}
}

func TestResolveCLIConfig_FlagOverridesEnv(t *testing.T) {
	t.Setenv("GRANICUS_API_KEY", "env-key")

	cfg, err := ResolveCLIConfig("", "flag-endpoint", "flag-key")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.APIKey != "flag-key" {
		t.Errorf("expected flag-key, got %s", cfg.APIKey)
	}
	if cfg.Endpoint != "flag-endpoint" {
		t.Errorf("expected flag-endpoint, got %s", cfg.Endpoint)
	}
}

func TestResolveCLIConfig_ProjectConfig(t *testing.T) {
	dir := t.TempDir()

	projDir := filepath.Join(dir, ".granicus")
	os.MkdirAll(projDir, 0700)
	os.WriteFile(filepath.Join(projDir, "config.json"), []byte(`{"org":"test-org","default_pipeline":"my-pipe"}`), 0600)

	t.Setenv("GRANICUS_API_KEY", "")
	t.Setenv("GRANICUS_ENDPOINT", "")

	cfg, err := ResolveCLIConfig(dir, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Org != "test-org" {
		t.Errorf("expected test-org, got %s", cfg.Org)
	}
	if cfg.Pipeline != "my-pipe" {
		t.Errorf("expected my-pipe, got %s", cfg.Pipeline)
	}
}

func TestWriteAndLoadUserConfig(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	uc := &UserConfig{
		Endpoint: "https://api.example.com",
		APIKey:   "test-key-123",
		Org:      "my-org",
	}
	if err := WriteUserConfig(uc); err != nil {
		t.Fatal(err)
	}

	loaded, err := LoadUserConfig()
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Endpoint != uc.Endpoint {
		t.Errorf("endpoint: %s", loaded.Endpoint)
	}
	if loaded.APIKey != uc.APIKey {
		t.Errorf("api_key: %s", loaded.APIKey)
	}
	if loaded.Org != uc.Org {
		t.Errorf("org: %s", loaded.Org)
	}
}
