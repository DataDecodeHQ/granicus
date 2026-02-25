package runner

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeScript(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte("#!/bin/bash\n"+content), 0755); err != nil {
		t.Fatal(err)
	}
	return name
}

func TestShellRunner_Success(t *testing.T) {
	dir := t.TempDir()
	src := writeScript(t, dir, "ok.sh", `echo hello`)
	r := NewShellRunner()
	result := r.Run(&Asset{Name: "ok", Type: "shell", Source: src}, dir, "test-run")
	if result.Status != "success" {
		t.Errorf("expected success, got %s: %s", result.Status, result.Error)
	}
	if result.ExitCode != 0 {
		t.Errorf("expected exit 0, got %d", result.ExitCode)
	}
	if !strings.Contains(result.Stdout, "hello") {
		t.Errorf("stdout missing 'hello': %q", result.Stdout)
	}
}

func TestShellRunner_Failure(t *testing.T) {
	dir := t.TempDir()
	src := writeScript(t, dir, "fail.sh", `exit 1`)
	r := NewShellRunner()
	result := r.Run(&Asset{Name: "fail", Type: "shell", Source: src}, dir, "test-run")
	if result.Status != "failed" {
		t.Errorf("expected failed, got %s", result.Status)
	}
	if result.ExitCode != 1 {
		t.Errorf("expected exit 1, got %d", result.ExitCode)
	}
}

func TestShellRunner_Stderr(t *testing.T) {
	dir := t.TempDir()
	src := writeScript(t, dir, "stderr.sh", `echo errout >&2`)
	r := NewShellRunner()
	result := r.Run(&Asset{Name: "stderr", Type: "shell", Source: src}, dir, "test-run")
	if !strings.Contains(result.Stderr, "errout") {
		t.Errorf("stderr missing 'errout': %q", result.Stderr)
	}
}

func TestShellRunner_Timeout(t *testing.T) {
	dir := t.TempDir()
	src := writeScript(t, dir, "slow.sh", `sleep 60`)
	r := &ShellRunner{Timeout: 1 * time.Second}
	result := r.Run(&Asset{Name: "slow", Type: "shell", Source: src}, dir, "test-run")
	if result.Status != "failed" {
		t.Errorf("expected failed, got %s", result.Status)
	}
	if result.Error != "timeout" {
		t.Errorf("expected timeout error, got %q", result.Error)
	}
}

func TestShellRunner_EnvVars(t *testing.T) {
	dir := t.TempDir()
	src := writeScript(t, dir, "env.sh", `echo "NAME=$GRANICUS_ASSET_NAME RUN=$GRANICUS_RUN_ID ROOT=$GRANICUS_PROJECT_ROOT"`)
	r := NewShellRunner()
	result := r.Run(&Asset{Name: "myasset", Type: "shell", Source: src}, dir, "run123")
	if !strings.Contains(result.Stdout, "NAME=myasset") {
		t.Errorf("missing GRANICUS_ASSET_NAME: %q", result.Stdout)
	}
	if !strings.Contains(result.Stdout, "RUN=run123") {
		t.Errorf("missing GRANICUS_RUN_ID: %q", result.Stdout)
	}
	if !strings.Contains(result.Stdout, "ROOT="+dir) {
		t.Errorf("missing GRANICUS_PROJECT_ROOT: %q", result.Stdout)
	}
}

func TestShellRunner_IntervalEnvVars(t *testing.T) {
	dir := t.TempDir()
	src := writeScript(t, dir, "iv.sh", `echo "START=$GRANICUS_INTERVAL_START END=$GRANICUS_INTERVAL_END"`)
	r := NewShellRunner()

	// With interval set
	result := r.Run(&Asset{
		Name: "iv", Type: "shell", Source: src,
		IntervalStart: "2025-01-10", IntervalEnd: "2025-01-11",
	}, dir, "run1")
	if !strings.Contains(result.Stdout, "START=2025-01-10") {
		t.Errorf("missing INTERVAL_START: %q", result.Stdout)
	}
	if !strings.Contains(result.Stdout, "END=2025-01-11") {
		t.Errorf("missing INTERVAL_END: %q", result.Stdout)
	}

	// Without interval (full refresh) — env vars should NOT be set
	result2 := r.Run(&Asset{
		Name: "iv", Type: "shell", Source: src,
	}, dir, "run2")
	if strings.Contains(result2.Stdout, "START=2025") {
		t.Errorf("INTERVAL_START should not be set for full refresh: %q", result2.Stdout)
	}
}

func TestShellRunner_LargeOutput(t *testing.T) {
	dir := t.TempDir()
	// Generate ~2MB of output
	src := writeScript(t, dir, "big.sh", `python3 -c "import sys; sys.stdout.write('A' * 2097152)"`)

	r := NewShellRunner()
	result := r.Run(&Asset{Name: "big", Type: "shell", Source: src}, dir, "test-run")
	if result.Status != "success" {
		t.Errorf("expected success, got %s: %s", result.Status, result.Error)
	}
	if len(result.Stdout) > ResultTruncBytes+len(TruncMarker)+10 {
		t.Errorf("stdout should be truncated to ~10KB, got %d bytes", len(result.Stdout))
	}
	if !strings.HasSuffix(result.Stdout, TruncMarker) {
		t.Error("expected truncation marker")
	}
}
