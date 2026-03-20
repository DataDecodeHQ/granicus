package scheduler

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/robfig/cron/v3"

	"github.com/DataDecodeHQ/granicus/internal/config"
	"github.com/DataDecodeHQ/granicus/internal/events"
	"github.com/DataDecodeHQ/granicus/internal/source"
)

type RunFunc func(cfg *config.PipelineConfig, projectRoot string)

type Scheduler struct {
	mu          sync.Mutex
	cron        *cron.Cron
	configDir   string // kept for watcher compatibility
	source      source.PipelineSource
	projectRoot string
	runFunc     RunFunc
	lockStore   *LockStore
	eventStore  *events.Store
	entries     map[string]cron.EntryID // pipeline name -> cron entry ID
	configs     map[string]*config.PipelineConfig
	configPaths map[string]string // pipeline name -> config file path
}

// NewScheduler creates a scheduler that loads pipeline configs from the given source and registers cron jobs.
func NewScheduler(src source.PipelineSource, projectRoot string, db *sql.DB, runFunc RunFunc, eventStore *events.Store) (*Scheduler, error) {
	lockStore, err := NewLockStore(db)
	if err != nil {
		return nil, fmt.Errorf("lock store: %w", err)
	}

	// Resolve the config directory from the source
	dir, cleanup, err := src.Fetch(context.Background(), "", "")
	if err != nil {
		return nil, fmt.Errorf("fetching pipeline source: %w", err)
	}
	// For LocalSource cleanup is a no-op; for GCS we need the dir to persist
	// for the scheduler's lifetime, so we skip cleanup here (the dir is
	// re-fetched on each LoadAndRegister/Reload for GCS).
	_ = cleanup

	return &Scheduler{
		cron:        cron.New(),
		configDir:   dir,
		source:      src,
		projectRoot: projectRoot,
		runFunc:     runFunc,
		lockStore:   lockStore,
		eventStore:  eventStore,
		entries:     make(map[string]cron.EntryID),
		configs:     make(map[string]*config.PipelineConfig),
		configPaths: make(map[string]string),
	}, nil
}

// ConfigDir returns the resolved config directory path.
func (s *Scheduler) ConfigDir() string {
	return s.configDir
}

// Source returns the pipeline source backing this scheduler.
func (s *Scheduler) Source() source.PipelineSource {
	return s.source
}

// LockStore returns the scheduler's pipeline lock store.
func (s *Scheduler) LockStore() *LockStore {
	return s.lockStore
}

// LoadAndRegister scans the config directory and registers all pipeline schedules, replacing any existing entries.
func (s *Scheduler) LoadAndRegister() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	results, err := scanConfigDir(s.configDir)
	if err != nil {
		return err
	}

	// Unregister all existing entries
	for name, entryID := range s.entries {
		s.cron.Remove(entryID)
		delete(s.entries, name)
	}
	s.configs = make(map[string]*config.PipelineConfig)
	s.configPaths = make(map[string]string)

	// Register new entries
	for name, sr := range results {
		if sr.cfg.Schedule == "" {
			continue
		}
		if err := s.registerPipeline(name, sr.cfg, sr.path); err != nil {
			slog.Warn("scheduler skipping pipeline", "pipeline", name, "error", err)
		}
	}

	return nil
}

// Reload re-scans configs and incrementally updates cron entries, returning added, removed, and updated pipeline names.
func (s *Scheduler) Reload() (added, removed, updated []string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	results, err := scanConfigDir(s.configDir)
	if err != nil {
		slog.Error("scheduler reload error", "error", err)
		return
	}

	// Find removed
	for name, entryID := range s.entries {
		if _, ok := results[name]; !ok {
			s.cron.Remove(entryID)
			delete(s.entries, name)
			delete(s.configs, name)
			delete(s.configPaths, name)
			removed = append(removed, name)
		}
	}

	// Find added and updated
	for name, sr := range results {
		if sr.cfg.Schedule == "" {
			continue
		}
		oldCfg, exists := s.configs[name]
		if !exists {
			if err := s.registerPipeline(name, sr.cfg, sr.path); err == nil {
				added = append(added, name)
			}
		} else if oldCfg.Schedule != sr.cfg.Schedule {
			if id, ok := s.entries[name]; ok {
				s.cron.Remove(id)
			}
			if err := s.registerPipeline(name, sr.cfg, sr.path); err == nil {
				updated = append(updated, name)
			}
		}
	}

	return
}

// RegisterAssetPolls registers cron entries for gcs_ingest assets with poll_interval.
// Each poll triggers a run targeting just that asset.
func (s *Scheduler) RegisterAssetPolls() {
	for name, cfg := range s.configs {
		configPath := s.configPaths[name]
		for _, asset := range cfg.Assets {
			if asset.Type != "gcs_ingest" || asset.PollInterval == "" {
				continue
			}
			assetName := asset.Name
			pipelineName := cfg.Pipeline
			cfgPath := configPath
			entryID, err := s.cron.AddFunc(asset.PollInterval, func() {
				freshCfg, err := config.LoadConfig(cfgPath)
				if err != nil {
					slog.Error("config reload failed for poll", "pipeline", pipelineName, "asset", assetName, "error", err)
					return
				}
				s.runWithLock(pipelineName+"/"+assetName, freshCfg)
			})
			if err != nil {
				slog.Warn("scheduler: invalid poll_interval", "asset", assetName, "schedule", asset.PollInterval, "error", err)
				continue
			}
			s.entries[cfg.Pipeline+"/poll:"+assetName] = entryID
		}
	}
}

func (s *Scheduler) registerPipeline(name string, cfg *config.PipelineConfig, configPath string) error {
	pipelineName := name
	cfgPath := configPath
	entryID, err := s.cron.AddFunc(cfg.Schedule, func() {
		freshCfg, err := config.LoadConfig(cfgPath)
		if err != nil {
			slog.Error("config reload failed", "pipeline", pipelineName, "path", cfgPath, "error", err)
			s.emitEvent(events.Event{
				Pipeline: pipelineName, EventType: "run_failed", Severity: "error",
				Summary: fmt.Sprintf("Config reload failed for %s: %v", pipelineName, err),
			})
			return
		}
		s.runWithLock(pipelineName, freshCfg)
	})
	if err != nil {
		return fmt.Errorf("invalid schedule %q: %w", cfg.Schedule, err)
	}
	s.entries[name] = entryID
	s.configs[name] = cfg
	s.configPaths[name] = configPath
	return nil
}

func (s *Scheduler) runWithLock(pipeline string, cfg *config.PipelineConfig) {
	acquired, err := s.lockStore.AcquireLock(pipeline, "scheduled")
	if err != nil {
		slog.Error("scheduler lock error", "pipeline", pipeline, "error", err)
		return
	}
	if !acquired {
		slog.Info("scheduler skipping pipeline (already running)", "pipeline", pipeline)
		s.emitEvent(events.Event{
			Pipeline: pipeline, EventType: "lock_contention", Severity: "warning",
			Summary: fmt.Sprintf("Skipped %s: pipeline already running", pipeline),
		})
		return
	}

	s.emitEvent(events.Event{
		Pipeline: pipeline, EventType: "lock_acquired", Severity: "info",
		Summary: fmt.Sprintf("Lock acquired for %s", pipeline),
	})

	defer func() {
		s.lockStore.ReleaseLock(pipeline, "scheduled")
		s.emitEvent(events.Event{
			Pipeline: pipeline, EventType: "lock_released", Severity: "info",
			Summary: fmt.Sprintf("Lock released for %s", pipeline),
		})
	}()

	s.runFunc(cfg, s.projectRoot)
}

func (s *Scheduler) emitEvent(event events.Event) {
	if s.eventStore != nil {
		_ = s.eventStore.Emit(event)
	}
}

// EventStore returns the scheduler's event store (may be nil).
func (s *Scheduler) EventStore() *events.Store {
	return s.eventStore
}

// Start begins the cron scheduler.
func (s *Scheduler) Start() {
	s.cron.Start()
}

// Stop halts the cron scheduler.
func (s *Scheduler) Stop() {
	s.cron.Stop()
}

// Pipelines returns the names of all registered pipeline entries.
func (s *Scheduler) Pipelines() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var names []string
	for name := range s.entries {
		names = append(names, name)
	}
	return names
}

// Config returns the pipeline configuration for the given pipeline name, or nil if not found.
func (s *Scheduler) Config(name string) *config.PipelineConfig {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.configs[name]
}

type scanResult struct {
	cfg  *config.PipelineConfig
	path string
}

func scanConfigDir(dir string) (map[string]scanResult, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("reading config dir: %w", err)
	}

	results := make(map[string]scanResult)
	for _, entry := range entries {
		if entry.IsDir() {
			// Check subdirectory for pipeline.yaml (supports multi-pipeline dirs)
			subPath := filepath.Join(dir, entry.Name(), "pipeline.yaml")
			if _, serr := os.Stat(subPath); serr == nil {
				cfg, cerr := config.LoadConfig(subPath)
				if cerr != nil {
					slog.Warn("scheduler skipping config", "file", subPath, "error", cerr)
					continue
				}
				results[cfg.Pipeline] = scanResult{cfg: cfg, path: subPath}
			}
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".yaml") && !strings.HasSuffix(name, ".yml") {
			continue
		}
		if name == "granicus-server.yaml" || name == "granicus-env.yaml" {
			continue
		}

		path := filepath.Join(dir, name)
		cfg, err := config.LoadConfig(path)
		if err != nil {
			slog.Warn("scheduler skipping config", "file", name, "error", err)
			continue
		}
		results[cfg.Pipeline] = scanResult{cfg: cfg, path: path}
	}

	return results, nil
}
