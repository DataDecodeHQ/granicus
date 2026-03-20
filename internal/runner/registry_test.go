package runner

import (
	"testing"

	"github.com/DataDecodeHQ/granicus/internal/config"
)

func TestRegistry_NoDefaultRunners(t *testing.T) {
	reg := NewRunnerRegistry(nil)
	result := reg.Run(&Asset{Name: "test", Type: "shell", Source: "x.sh"}, "/tmp", "run1")
	if result.Status != "failed" {
		t.Errorf("expected failed for unregistered type, got %s", result.Status)
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
	conns := map[string]*config.ResourceConfig{
		"bq": {Name: "bq", Type: "bigquery", Properties: map[string]string{"project": "test"}},
	}
	reg := NewRunnerRegistry(conns)

	if conn := reg.Resource("bq"); conn == nil {
		t.Error("expected bq connection")
	} else if conn.Properties["project"] != "test" {
		t.Errorf("project: %q", conn.Properties["project"])
	}

	if conn := reg.Resource("missing"); conn != nil {
		t.Error("expected nil for missing connection")
	}
}

func TestRegistry_NilConnections(t *testing.T) {
	reg := NewRunnerRegistry(nil)
	if conn := reg.Resource("anything"); conn != nil {
		t.Error("expected nil")
	}
}
