package scheduler

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/robfig/cron/v3"

	"github.com/analytehealth/granicus/internal/config"
)

type RunFunc func(cfg *config.PipelineConfig, projectRoot string)

type Scheduler struct {
	mu          sync.Mutex
	cron        *cron.Cron
	configDir   string
	projectRoot string
	runFunc     RunFunc
	lockStore   *LockStore
	entries     map[string]cron.EntryID // pipeline name -> cron entry ID
	configs     map[string]*config.PipelineConfig
}

func NewScheduler(configDir, projectRoot string, db *sql.DB, runFunc RunFunc) (*Scheduler, error) {
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
			log.Printf("scheduler: skipping %s: %v", name, err)
		}
	}

	return nil
}

func (s *Scheduler) Reload() (added, removed, updated []string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	configs, err := scanConfigDir(s.configDir)
	if err != nil {
		log.Printf("scheduler: reload error: %v", err)
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
		log.Printf("scheduler: lock error for %s: %v", pipeline, err)
		return
	}
	if !acquired {
		log.Printf("scheduler: skipping %s (already running)", pipeline)
		return
	}
	defer s.lockStore.ReleaseLock(pipeline, "scheduled")

	s.runFunc(cfg, s.projectRoot)
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
			log.Printf("scheduler: skipping %s: %v", name, err)
			continue
		}
		configs[cfg.Pipeline] = cfg
	}

	return configs, nil
}
