package scheduler

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/analytehealth/granicus/internal/config"
)

func TestWatcher_DetectsNewConfig(t *testing.T) {
	db := newTestDB(t)
	configDir := t.TempDir()

	writeConfig(t, configDir, "p1.yaml", `
pipeline: p1
schedule: "0 * * * *"
assets:
  - name: a
    type: shell
    source: a.sh
`)

	s, _ := NewScheduler(configDir, "/tmp", db, func(cfg *config.PipelineConfig, pr string) {})
	s.LoadAndRegister()

	w, err := NewWatcher(s)
	if err != nil {
		t.Fatal(err)
	}
	w.Start()
	defer w.Stop()

	// Add a new config file
	writeConfig(t, configDir, "p2.yaml", `
pipeline: p2
schedule: "*/5 * * * *"
assets:
  - name: b
    type: shell
    source: b.sh
`)

	// Wait for debounce + processing
	time.Sleep(2 * time.Second)

	pipelines := s.Pipelines()
	if len(pipelines) != 2 {
		t.Errorf("expected 2 pipelines, got %d: %v", len(pipelines), pipelines)
	}
}

func TestWatcher_DetectsRemoval(t *testing.T) {
	db := newTestDB(t)
	configDir := t.TempDir()

	writeConfig(t, configDir, "p1.yaml", `
pipeline: p1
schedule: "0 * * * *"
assets:
  - name: a
    type: shell
    source: a.sh
`)
	writeConfig(t, configDir, "p2.yaml", `
pipeline: p2
schedule: "*/5 * * * *"
assets:
  - name: b
    type: shell
    source: b.sh
`)

	s, _ := NewScheduler(configDir, "/tmp", db, func(cfg *config.PipelineConfig, pr string) {})
	s.LoadAndRegister()

	w, err := NewWatcher(s)
	if err != nil {
		t.Fatal(err)
	}
	w.Start()
	defer w.Stop()

	os.Remove(filepath.Join(configDir, "p2.yaml"))

	time.Sleep(2 * time.Second)

	pipelines := s.Pipelines()
	if len(pipelines) != 1 {
		t.Errorf("expected 1 pipeline, got %d: %v", len(pipelines), pipelines)
	}
}

func TestWatcher_DebounceCoalesces(t *testing.T) {
	db := newTestDB(t)
	configDir := t.TempDir()

	s, _ := NewScheduler(configDir, "/tmp", db, func(cfg *config.PipelineConfig, pr string) {})
	s.LoadAndRegister()

	w, err := NewWatcher(s)
	if err != nil {
		t.Fatal(err)
	}
	w.Start()
	defer w.Stop()

	// Rapid-fire writes (simulates editor save)
	for i := 0; i < 5; i++ {
		writeConfig(t, configDir, "rapid.yaml", `
pipeline: rapid
schedule: "0 * * * *"
assets:
  - name: a
    type: shell
    source: a.sh
`)
		time.Sleep(100 * time.Millisecond)
	}

	time.Sleep(2 * time.Second)

	pipelines := s.Pipelines()
	if len(pipelines) != 1 {
		t.Errorf("expected 1 pipeline after rapid writes, got %d: %v", len(pipelines), pipelines)
	}
}
