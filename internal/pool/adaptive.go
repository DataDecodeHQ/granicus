package pool

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/DataDecodeHQ/granicus/internal/config"
)

const (
	minCooldown = 10 * time.Second
	maxCooldown = 5 * time.Minute
)

// PoolObserver is a callback invoked at key pool lifecycle points.
// eventType identifies the event; data carries event-specific key-value pairs.
type PoolObserver func(eventType string, data map[string]any)

// PoolStats holds cumulative counters for pool activity.
type PoolStats struct {
	PeakSlots         int `json:"peak_slots"`
	TotalAcquires     int `json:"total_acquires"`
	TotalWaits        int `json:"total_waits"`
	BackpressureCount int `json:"backpressure_count"`
}

type AdaptivePool struct {
	mu           sync.Mutex
	name         string
	limit        ResourceLimit
	currentSlots int
	sem          chan struct{}
	lastRamp     time.Time
	backoffUntil time.Time
	cooldown     time.Duration
	observer     PoolObserver

	// stats counters
	peakSlots         int
	totalAcquires     int
	totalWaits        int
	backpressureCount int
}

func NewAdaptivePool(name string, limit ResourceLimit) *AdaptivePool {
	slots := limit.InitialSlots
	if slots <= 0 {
		slots = 1
	}
	return &AdaptivePool{
		name:         name,
		limit:        limit,
		currentSlots: slots,
		sem:          make(chan struct{}, slots),
		lastRamp:     time.Now(),
		cooldown:     minCooldown,
		peakSlots:    slots,
	}
}

// SetObserver registers a callback that is invoked at key pool lifecycle points.
func (p *AdaptivePool) SetObserver(fn PoolObserver) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.observer = fn
	p.emit("pool_created", map[string]any{
		"resource":      p.name,
		"initial_slots": p.currentSlots,
		"max_concurrent": p.limit.MaxConcurrent,
	})
}

// emit calls the observer if set. Must NOT be called with mu held if the
// observer might call back into the pool. For simplicity, callers snapshot
// data before calling emit outside the lock, or accept that emit is called
// under the lock (observers should not block).
func (p *AdaptivePool) emit(eventType string, data map[string]any) {
	if p.observer != nil {
		p.observer(eventType, data)
	}
}

// Stats returns the current pool activity counters.
func (p *AdaptivePool) Stats() PoolStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return PoolStats{
		PeakSlots:         p.peakSlots,
		TotalAcquires:     p.totalAcquires,
		TotalWaits:        p.totalWaits,
		BackpressureCount: p.backpressureCount,
	}
}

// EmitStats emits a pool_stats event with current counters via the observer.
func (p *AdaptivePool) EmitStats() {
	p.mu.Lock()
	s := PoolStats{
		PeakSlots:         p.peakSlots,
		TotalAcquires:     p.totalAcquires,
		TotalWaits:        p.totalWaits,
		BackpressureCount: p.backpressureCount,
	}
	obs := p.observer
	name := p.name
	p.mu.Unlock()

	if obs != nil {
		obs("pool_stats", map[string]any{
			"resource":           name,
			"peak_slots":         s.PeakSlots,
			"total_acquires":     s.TotalAcquires,
			"total_waits":        s.TotalWaits,
			"backpressure_count": s.BackpressureCount,
		})
	}
}

func (p *AdaptivePool) Acquire(ctx context.Context) error {
	p.maybeRamp()

	p.mu.Lock()
	sem := p.sem
	name := p.name
	slots := p.currentSlots
	p.mu.Unlock()

	slog.Debug("adaptive pool acquiring slot", "pool", name, "in_use", len(sem), "slots", slots)

	select {
	case sem <- struct{}{}:
		p.mu.Lock()
		p.totalAcquires++
		p.mu.Unlock()
		slog.Debug("adaptive pool slot acquired", "pool", name, "in_use", len(sem), "slots", slots)
		return nil
	default:
		// Would block — count as a wait
		p.mu.Lock()
		p.totalWaits++
		p.mu.Unlock()
	}

	// Block until slot available or context cancelled
	select {
	case sem <- struct{}{}:
		p.mu.Lock()
		p.totalAcquires++
		p.mu.Unlock()
		slog.Debug("adaptive pool slot acquired (after wait)", "pool", name, "in_use", len(sem), "slots", slots)
		return nil
	case <-ctx.Done():
		return fmt.Errorf("adaptive pool %s: context cancelled while waiting for slot: %w", name, ctx.Err())
	}
}

func (p *AdaptivePool) Release() {
	p.mu.Lock()
	sem := p.sem
	p.mu.Unlock()

	<-sem
	slog.Debug("adaptive pool slot released", "pool", p.name, "in_use", len(sem), "slots", cap(sem))
}

func (p *AdaptivePool) SignalBackpressure() {
	p.mu.Lock()

	oldSlots := p.currentSlots
	newSlots := p.currentSlots / 2
	if newSlots < p.limit.InitialSlots {
		newSlots = p.limit.InitialSlots
	}

	if !p.backoffUntil.IsZero() && time.Now().Before(p.backoffUntil) {
		p.cooldown *= 2
	} else {
		p.cooldown = minCooldown
	}
	if p.cooldown > maxCooldown {
		p.cooldown = maxCooldown
	}

	p.backoffUntil = time.Now().Add(p.cooldown)
	p.lastRamp = time.Now()
	p.backpressureCount++
	cd := p.cooldown

	slog.Info("adaptive pool backpressure", "pool", p.name, "old_slots", oldSlots, "new_slots", newSlots, "cooldown", cd)

	p.resize(newSlots)
	obs := p.observer
	p.mu.Unlock()

	if obs != nil {
		obs("pool_backpressure", map[string]any{
			"resource":  p.name,
			"old_slots": oldSlots,
			"new_slots": newSlots,
			"cooldown":  cd.String(),
		})
	}
}

func (p *AdaptivePool) CurrentSlots() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.currentSlots
}

func (p *AdaptivePool) InUse() int {
	p.mu.Lock()
	sem := p.sem
	p.mu.Unlock()
	return len(sem)
}

func (p *AdaptivePool) maybeRamp() {
	p.mu.Lock()

	now := time.Now()
	if now.Before(p.backoffUntil) {
		p.mu.Unlock()
		return
	}
	if now.Sub(p.lastRamp) < p.limit.RampInterval {
		p.mu.Unlock()
		return
	}
	if p.currentSlots >= p.limit.MaxConcurrent {
		p.mu.Unlock()
		return
	}

	oldSlots := p.currentSlots
	newSlots := p.currentSlots + p.limit.RampStep
	if newSlots > p.limit.MaxConcurrent {
		newSlots = p.limit.MaxConcurrent
	}

	slog.Debug("adaptive pool ramping up", "pool", p.name, "old_slots", oldSlots, "new_slots", newSlots)

	p.resize(newSlots)
	p.lastRamp = now
	if newSlots > p.peakSlots {
		p.peakSlots = newSlots
	}
	obs := p.observer
	p.mu.Unlock()

	if obs != nil {
		obs("pool_ramp_up", map[string]any{
			"resource":  p.name,
			"old_slots": oldSlots,
			"new_slots": newSlots,
		})
	}
}

// resize changes the channel capacity. Must be called with mu held.
func (p *AdaptivePool) resize(newSlots int) {
	if newSlots == p.currentSlots {
		return
	}

	oldSem := p.sem
	inUse := len(oldSem)

	// Drain all tokens from the old channel
	drained := 0
	for {
		select {
		case <-oldSem:
			drained++
		default:
			goto done
		}
	}
done:

	newSem := make(chan struct{}, newSlots)

	// Put back tokens for slots currently in use, capped at new size
	putBack := inUse
	if putBack > newSlots {
		putBack = newSlots
	}
	for i := 0; i < putBack; i++ {
		newSem <- struct{}{}
	}

	p.sem = newSem
	p.currentSlots = newSlots
}

type AdaptivePoolManager struct {
	mu    sync.RWMutex
	pools map[string]*AdaptivePool
}

func NewAdaptivePoolManager(resources map[string]*config.ResourceConfig) *AdaptivePoolManager {
	m := &AdaptivePoolManager{
		pools: make(map[string]*AdaptivePool, len(resources)),
	}
	for name, rc := range resources {
		limit := DefaultLimit(rc.Type)
		m.pools[name] = NewAdaptivePool(name, limit)
	}
	return m
}

func (m *AdaptivePoolManager) Acquire(ctx context.Context, resourceType string) error {
	m.mu.RLock()
	p, ok := m.pools[resourceType]
	m.mu.RUnlock()
	if !ok {
		return fmt.Errorf("adaptive pool %q not found", resourceType)
	}
	return p.Acquire(ctx)
}

func (m *AdaptivePoolManager) Release(resourceType string) {
	m.mu.RLock()
	p, ok := m.pools[resourceType]
	m.mu.RUnlock()
	if !ok {
		return
	}
	p.Release()
}

func (m *AdaptivePoolManager) SignalBackpressure(resourceType string) {
	m.mu.RLock()
	p, ok := m.pools[resourceType]
	m.mu.RUnlock()
	if !ok {
		return
	}
	p.SignalBackpressure()
}

func (m *AdaptivePoolManager) Pool(resourceType string) *AdaptivePool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.pools[resourceType]
}
