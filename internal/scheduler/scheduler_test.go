package scheduler

import (
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/analytehealth/granicus/internal/config"
)

func newCronWithSeconds() *cron.Cron {
	return cron.New(cron.WithSeconds())
}

func writeConfig(t *testing.T, dir, filename, content string) {
	t.Helper()
	os.WriteFile(filepath.Join(dir, filename), []byte(content), 0644)
}

func TestScheduler_LoadAndRegister(t *testing.T) {
	db := newTestDB(t)
	configDir := t.TempDir()

	writeConfig(t, configDir, "pipeline1.yaml", `
pipeline: p1
schedule: "*/5 * * * *"
assets:
  - name: a
    type: shell
    source: a.sh
`)
	writeConfig(t, configDir, "pipeline2.yaml", `
pipeline: p2
assets:
  - name: b
    type: shell
    source: b.sh
`)

	var runCount int32
	s, err := NewScheduler(configDir, "/tmp", db, func(cfg *config.PipelineConfig, pr string) {
		atomic.AddInt32(&runCount, 1)
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := s.LoadAndRegister(); err != nil {
		t.Fatal(err)
	}

	// p1 has schedule, p2 doesn't
	pipelines := s.Pipelines()
	if len(pipelines) != 1 {
		t.Fatalf("expected 1 scheduled pipeline, got %d", len(pipelines))
	}
	if pipelines[0] != "p1" {
		t.Errorf("expected p1, got %q", pipelines[0])
	}
}

func TestScheduler_SkipsServerAndEnvYAML(t *testing.T) {
	db := newTestDB(t)
	configDir := t.TempDir()

	writeConfig(t, configDir, "granicus-server.yaml", `server: {}`)
	writeConfig(t, configDir, "granicus-env.yaml", `environments: {}`)
	writeConfig(t, configDir, "real.yaml", `
pipeline: real
schedule: "0 * * * *"
assets:
  - name: a
    type: shell
    source: a.sh
`)

	s, _ := NewScheduler(configDir, "/tmp", db, func(cfg *config.PipelineConfig, pr string) {}, nil)
	s.LoadAndRegister()

	pipelines := s.Pipelines()
	if len(pipelines) != 1 || pipelines[0] != "real" {
		t.Errorf("expected [real], got %v", pipelines)
	}
}

func TestScheduler_Reload(t *testing.T) {
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

	s, _ := NewScheduler(configDir, "/tmp", db, func(cfg *config.PipelineConfig, pr string) {}, nil)
	s.LoadAndRegister()

	// Add a new pipeline
	writeConfig(t, configDir, "p2.yaml", `
pipeline: p2
schedule: "*/10 * * * *"
assets:
  - name: b
    type: shell
    source: b.sh
`)

	added, removed, _ := s.Reload()
	if len(added) != 1 || added[0] != "p2" {
		t.Errorf("added: %v", added)
	}
	if len(removed) != 0 {
		t.Errorf("removed: %v", removed)
	}

	// Remove p1
	os.Remove(filepath.Join(configDir, "p1.yaml"))
	_, removed2, _ := s.Reload()
	if len(removed2) != 1 || removed2[0] != "p1" {
		t.Errorf("removed: %v", removed2)
	}
}

func TestScheduler_CronExecution(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping cron test in short mode")
	}

	db := newTestDB(t)
	configDir := t.TempDir()

	writeConfig(t, configDir, "fast.yaml", `
pipeline: fast
schedule: "* * * * * *"
assets:
  - name: a
    type: shell
    source: a.sh
`)

	var runCount int32
	s, _ := NewScheduler(configDir, "/tmp", db, func(cfg *config.PipelineConfig, pr string) {
		atomic.AddInt32(&runCount, 1)
	}, nil)

	// Use cron with seconds support for fast testing
	s.cron = newCronWithSeconds()
	s.LoadAndRegister()
	s.Start()
	defer s.Stop()

	time.Sleep(3500 * time.Millisecond)

	count := atomic.LoadInt32(&runCount)
	if count < 2 {
		t.Errorf("expected at least 2 runs in 3.5s, got %d", count)
	}
}

func TestScheduler_LockSkipsOverlap(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping lock test in short mode")
	}

	db := newTestDB(t)
	configDir := t.TempDir()

	writeConfig(t, configDir, "slow.yaml", `
pipeline: slow
schedule: "* * * * * *"
assets:
  - name: a
    type: shell
    source: a.sh
`)

	var runCount int32
	s, _ := NewScheduler(configDir, "/tmp", db, func(cfg *config.PipelineConfig, pr string) {
		atomic.AddInt32(&runCount, 1)
		time.Sleep(2 * time.Second) // Hold lock for 2 seconds
	}, nil)

	s.cron = newCronWithSeconds()
	s.LoadAndRegister()
	s.Start()
	defer s.Stop()

	time.Sleep(3500 * time.Millisecond)

	count := atomic.LoadInt32(&runCount)
	// Only 1-2 runs should execute (lock prevents overlap)
	if count > 2 {
		t.Errorf("lock should prevent overlap, got %d runs", count)
	}
}
