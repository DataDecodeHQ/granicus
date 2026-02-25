package scheduler

import (
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/analytehealth/granicus/internal/config"
)

func TestIntegration_ScheduledRunsAndLocking(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
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
		time.Sleep(100 * time.Millisecond)
	}, nil)

	s.cron = newCronWithSeconds()
	s.LoadAndRegister()
	s.Start()
	defer s.Stop()

	time.Sleep(3500 * time.Millisecond)

	count := atomic.LoadInt32(&runCount)
	if count < 2 {
		t.Errorf("expected at least 2 scheduled runs, got %d", count)
	}
}

func TestIntegration_ConfigReloadWhileRunning(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

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
	s.Start()
	defer s.Stop()

	w, err := NewWatcher(s)
	if err != nil {
		t.Fatal(err)
	}
	w.Start()
	defer w.Stop()

	// Initially 1 pipeline
	if len(s.Pipelines()) != 1 {
		t.Fatalf("expected 1 pipeline, got %d", len(s.Pipelines()))
	}

	// Add a pipeline config
	writeConfig(t, configDir, "p2.yaml", `
pipeline: p2
schedule: "*/5 * * * *"
assets:
  - name: b
    type: shell
    source: b.sh
`)

	time.Sleep(2 * time.Second)
	if len(s.Pipelines()) != 2 {
		t.Errorf("expected 2 pipelines after add, got %d: %v", len(s.Pipelines()), s.Pipelines())
	}

	// Remove a pipeline config
	os.Remove(filepath.Join(configDir, "p1.yaml"))
	time.Sleep(2 * time.Second)
	pipelines := s.Pipelines()
	if len(pipelines) != 1 {
		t.Errorf("expected 1 pipeline after remove, got %d: %v", len(pipelines), pipelines)
	}
	if len(pipelines) == 1 && pipelines[0] != "p2" {
		t.Errorf("expected p2, got %q", pipelines[0])
	}

	// Modify schedule
	writeConfig(t, configDir, "p2.yaml", `
pipeline: p2
schedule: "*/10 * * * *"
assets:
  - name: b
    type: shell
    source: b.sh
`)
	time.Sleep(2 * time.Second)
	cfg := s.Config("p2")
	if cfg == nil || cfg.Schedule != "*/10 * * * *" {
		schedule := ""
		if cfg != nil {
			schedule = cfg.Schedule
		}
		t.Errorf("expected updated schedule, got %q", schedule)
	}
}

func TestIntegration_StaleLockRecoveryOnStartup(t *testing.T) {
	db := newTestDB(t)
	lockStore, err := NewLockStore(db)
	if err != nil {
		t.Fatal(err)
	}

	// Insert a stale lock (7 hours old)
	staleTime := time.Now().UTC().Add(-7 * time.Hour).Format(time.RFC3339)
	db.Exec(`INSERT INTO pipeline_locks (pipeline_name, run_id, started_at, status) VALUES (?, ?, ?, 'running')`,
		"stale_pipe", "old_run", staleTime)

	// Insert a recent lock
	lockStore.AcquireLock("recent_pipe", "new_run")

	// Recover
	recovered, err := lockStore.RecoverStaleLocks(6 * time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if recovered != 1 {
		t.Errorf("expected 1 recovered, got %d", recovered)
	}

	// Stale should be acquirable, recent should not
	acq1, _ := lockStore.AcquireLock("stale_pipe", "retry")
	if !acq1 {
		t.Error("stale pipeline should be acquirable after recovery")
	}
	acq2, _ := lockStore.AcquireLock("recent_pipe", "retry")
	if acq2 {
		t.Error("recent pipeline should still be locked")
	}
}

func TestIntegration_BackwardsCompat_NoSchedule(t *testing.T) {
	db := newTestDB(t)
	configDir := t.TempDir()

	// Phase 1/2 style config — no schedule field
	writeConfig(t, configDir, "legacy.yaml", `
pipeline: legacy
assets:
  - name: a
    type: shell
    source: a.sh
`)

	s, err := NewScheduler(configDir, "/tmp", db, func(cfg *config.PipelineConfig, pr string) {}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.LoadAndRegister(); err != nil {
		t.Fatal(err)
	}

	// No scheduled pipelines (no schedule field)
	if len(s.Pipelines()) != 0 {
		t.Errorf("expected 0 scheduled pipelines for legacy config, got %d", len(s.Pipelines()))
	}

	// Config still loads fine via config.LoadConfig
	cfg, err := config.LoadConfig(filepath.Join(configDir, "legacy.yaml"))
	if err != nil {
		t.Fatalf("legacy config should load: %v", err)
	}
	if cfg.Pipeline != "legacy" {
		t.Errorf("expected 'legacy', got %q", cfg.Pipeline)
	}
}

func TestIntegration_EnvironmentIsolation(t *testing.T) {
	projectRoot := t.TempDir()

	devPath := config.StateDBPath(projectRoot, "dev")
	prodPath := config.StateDBPath(projectRoot, "prod")

	if devPath == prodPath {
		t.Error("dev and prod state paths should differ")
	}

	if devPath != projectRoot+"/.granicus/dev-state.db" {
		t.Errorf("unexpected dev path: %s", devPath)
	}
	if prodPath != projectRoot+"/.granicus/prod-state.db" {
		t.Errorf("unexpected prod path: %s", prodPath)
	}

	// Default env is dev
	defaultPath := config.StateDBPath(projectRoot, "")
	if defaultPath != projectRoot+"/.granicus/dev-state.db" {
		t.Errorf("expected default=dev, got %s", defaultPath)
	}
}
