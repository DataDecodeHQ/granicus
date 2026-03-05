package scheduler

import (
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"

	"github.com/Andrew-DataDecode/Granicus/internal/events"
)

type Watcher struct {
	scheduler *Scheduler
	watcher   *fsnotify.Watcher
	stop      chan struct{}
	wg        sync.WaitGroup
}

func NewWatcher(s *Scheduler) (*Watcher, error) {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	if err := w.Add(s.configDir); err != nil {
		w.Close()
		return nil, err
	}
	return &Watcher{
		scheduler: s,
		watcher:   w,
		stop:      make(chan struct{}),
	}, nil
}

func (w *Watcher) Start() {
	w.wg.Add(1)
	go w.loop()
}

func (w *Watcher) Stop() {
	close(w.stop)
	w.watcher.Close()
	w.wg.Wait()
}

func (w *Watcher) loop() {
	defer w.wg.Done()

	var debounce *time.Timer
	var debounceMu sync.Mutex

	for {
		select {
		case <-w.stop:
			return

		case event, ok := <-w.watcher.Events:
			if !ok {
				return
			}
			name := event.Name
			if !strings.HasSuffix(name, ".yaml") && !strings.HasSuffix(name, ".yml") {
				continue
			}

			debounceMu.Lock()
			if debounce != nil {
				debounce.Stop()
			}
			debounce = time.AfterFunc(1*time.Second, func() {
				added, removed, updated := w.scheduler.Reload()
				if len(added) > 0 {
					slog.Info("scheduler added pipelines", "pipelines", added)
				}
				if len(removed) > 0 {
					slog.Info("scheduler removed pipelines", "pipelines", removed)
				}
				if len(updated) > 0 {
					slog.Info("scheduler updated pipelines", "pipelines", updated)
				}
				if len(added)+len(removed)+len(updated) > 0 {
					w.scheduler.emitEvent(events.Event{
						EventType: "config_reloaded", Severity: "info",
						Summary: fmt.Sprintf("Config reloaded: %d added, %d removed, %d updated", len(added), len(removed), len(updated)),
						Details: map[string]any{
							"added": added, "removed": removed, "updated": updated,
						},
					})
				}
			})
			debounceMu.Unlock()

		case err, ok := <-w.watcher.Errors:
			if !ok {
				return
			}
			slog.Error("scheduler watcher error", "error", err)
		}
	}
}
