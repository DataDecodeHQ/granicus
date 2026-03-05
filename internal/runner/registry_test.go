package runner

import (
	"testing"

	"github.com/analytehealth/granicus/internal/config"
)

func TestRegistry_DispatchShell(t *testing.T) {
	dir := t.TempDir()
	src := writeScript(t, dir, "hello.sh", `echo hello`)
	reg := NewRunnerRegistry(nil)

	result := reg.Run(&Asset{Name: "test", Type: "shell", Source: src}, dir, "run1")
	if result.Status != "success" {
		t.Errorf("expected success, got %s: %s", result.Status, result.Error)
	}
}

func TestRegistry_UnknownType(t *testing.T) {
	reg := NewRunnerRegistry(nil)
	result := reg.Run(&Asset{Name: "test", Type: "unknown", Source: "x.sh"}, "/tmp", "run1")
	if result.Status != "failed" {
		t.Errorf("expected failed, got %s", result.Status)
	}
	if result.Error == "" {
		t.Error("expected error message")
	}
}

func TestRegistry_ConnectionLookup(t *testing.T) {
	conns := map[string]*config.ConnectionConfig{
		"bq": {Name: "bq", Type: "bigquery", Properties: map[string]string{"project": "test"}},
	}
	reg := NewRunnerRegistry(conns)

	if conn := reg.Connection("bq"); conn == nil {
		t.Error("expected bq connection")
	} else if conn.Properties["project"] != "test" {
		t.Errorf("project: %q", conn.Properties["project"])
	}

	if conn := reg.Connection("missing"); conn != nil {
		t.Error("expected nil for missing connection")
	}
}

func TestRegistry_NilConnections(t *testing.T) {
	reg := NewRunnerRegistry(nil)
	if conn := reg.Connection("anything"); conn != nil {
		t.Error("expected nil")
	}
}
