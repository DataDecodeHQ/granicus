package scheduler

import (
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
)

type RunFunc func(cfg *config.PipelineConfig, projectRoot string)

type Scheduler struct {
	mu          sync.Mutex
	cron        *cron.Cron
	configDir   string
	projectRoot string
	runFunc     RunFunc
	lockStore   *LockStore
	eventStore  *events.Store
	entries     map[string]cron.EntryID // pipeline name -> cron entry ID
	configs     map[string]*config.PipelineConfig
}

func NewScheduler(configDir, projectRoot string, db *sql.DB, runFunc RunFunc, eventStore *events.Store) (*Scheduler, error) {
	lockStore, err := NewLockStore(db)
	if err != nil {
		return nil, fmt.Errorf("lock store: %w", err)
	}

	return &Scheduler{
		cron:        cron.New(),
		configDir:   configDir,
		projectRoot: projectRoot,
		runFunc:     runFunc,
		lockStore:   lockStore,
		eventStore:  eventStore,
		entries:     make(map[string]cron.EntryID),
		configs:     make(map[string]*config.PipelineConfig),
	}, nil
}

func (s *Scheduler) LockStore() *LockStore {
	return s.lockStore
}

func (s *Scheduler) LoadAndRegister() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	configs, err := scanConfigDir(s.configDir)
	if err != nil {
		return err
	}

	// Unregister all existing entries
	for name, entryID := range s.entries {
		s.cron.Remove(entryID)
		delete(s.entries, name)
	}
	s.configs = make(map[string]*config.PipelineConfig)

	// Register new entries
	for name, cfg := range configs {
		if cfg.Schedule == "" {
			continue
		}
		if err := s.registerPipeline(name, cfg); err != nil {
			slog.Warn("scheduler skipping pipeline", "pipeline", name, "error", err)
		}
	}

	return nil
}

func (s *Scheduler) Reload() (added, removed, updated []string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	configs, err := scanConfigDir(s.configDir)
	if err != nil {
		slog.Error("scheduler reload error", "error", err)
		return
	}

	// Find removed
	for name, entryID := range s.entries {
		if _, ok := configs[name]; !ok {
			s.cron.Remove(entryID)
			delete(s.entries, name)
			delete(s.configs, name)
			removed = append(removed, name)
		}
	}

	// Find added and updated
	for name, cfg := range configs {
		if cfg.Schedule == "" {
			continue
		}
		oldCfg, exists := s.configs[name]
		if !exists {
			if err := s.registerPipeline(name, cfg); err == nil {
				added = append(added, name)
			}
		} else if oldCfg.Schedule != cfg.Schedule {
			if id, ok := s.entries[name]; ok {
				s.cron.Remove(id)
			}
			if err := s.registerPipeline(name, cfg); err == nil {
				updated = append(updated, name)
			}
		}
	}

	return
}

// RegisterAssetPolls registers cron entries for gcs_ingest assets with poll_interval.
// Each poll triggers a run targeting just that asset.
func (s *Scheduler) RegisterAssetPolls() {
	for _, cfg := range s.configs {
		for _, asset := range cfg.Assets {
			if asset.Type != "gcs_ingest" || asset.PollInterval == "" {
				continue
			}
			assetName := asset.Name
			cfgCopy := *cfg
			entryID, err := s.cron.AddFunc(asset.PollInterval, func() {
				s.runWithLock(cfgCopy.Pipeline+"/"+assetName, &cfgCopy)
			})
			if err != nil {
				slog.Warn("scheduler: invalid poll_interval", "asset", assetName, "schedule", asset.PollInterval, "error", err)
				continue
			}
			s.entries[cfgCopy.Pipeline+"/poll:"+assetName] = entryID
		}
	}
}

func (s *Scheduler) registerPipeline(name string, cfg *config.PipelineConfig) error {
	cfgCopy := *cfg
	entryID, err := s.cron.AddFunc(cfg.Schedule, func() {
		s.runWithLock(name, &cfgCopy)
	})
	if err != nil {
		return fmt.Errorf("invalid schedule %q: %w", cfg.Schedule, err)
	}
	s.entries[name] = entryID
	s.configs[name] = cfg
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

func (s *Scheduler) Start() {
	s.cron.Start()
}

func (s *Scheduler) Stop() {
	s.cron.Stop()
}

func (s *Scheduler) Pipelines() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var names []string
	for name := range s.entries {
		names = append(names, name)
	}
	return names
}

func (s *Scheduler) Config(name string) *config.PipelineConfig {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.configs[name]
}

func scanConfigDir(dir string) (map[string]*config.PipelineConfig, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("reading config dir: %w", err)
	}

	configs := make(map[string]*config.PipelineConfig)
	for _, entry := range entries {
		if entry.IsDir() {
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
		configs[cfg.Pipeline] = cfg
	}

	return configs, nil
}
