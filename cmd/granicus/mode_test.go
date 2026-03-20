package main

import (
	"testing"

	"github.com/DataDecodeHQ/granicus/internal/config"
)

func TestResolveMode_Default(t *testing.T) {
	cfg := &config.CLIConfig{}
	if mode := resolveMode(cfg, false); mode != ModeLocal {
		t.Errorf("expected local, got %s", mode)
	}
}

func TestResolveMode_ForceLocal(t *testing.T) {
	cfg := &config.CLIConfig{APIKey: "key"}
	if mode := resolveMode(cfg, true); mode != ModeLocal {
		t.Errorf("expected local with forceLocal, got %s", mode)
	}
}

func TestResolveMode_CloudWithAPIKey(t *testing.T) {
	cfg := &config.CLIConfig{APIKey: "key"}
	if mode := resolveMode(cfg, false); mode != ModeCloud {
		t.Errorf("expected cloud, got %s", mode)
	}
}

func TestRequireCloud_LocalMode(t *testing.T) {
	err := requireCloud(ModeLocal, "push")
	if err == nil {
		t.Fatal("expected error for local mode")
	}
	if err.Error() != "push is a cloud-only command; run 'granicus login' to configure cloud mode" {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRequireCloud_CloudMode(t *testing.T) {
	err := requireCloud(ModeCloud, "push")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}
