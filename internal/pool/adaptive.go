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

type AdaptivePool struct {
	mu           sync.Mutex
	name         string
	limit        ResourceLimit
	currentSlots int
	sem          chan struct{}
	lastRamp     time.Time
	backoffUntil time.Time
	cooldown     time.Duration
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
		slog.Debug("adaptive pool slot acquired", "pool", name, "in_use", len(sem), "slots", slots)
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
	defer p.mu.Unlock()

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

	slog.Info("adaptive pool backpressure", "pool", p.name, "old_slots", p.currentSlots, "new_slots", newSlots, "cooldown", p.cooldown)

	p.resize(newSlots)
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
	defer p.mu.Unlock()

	now := time.Now()
	if now.Before(p.backoffUntil) {
		return
	}
	if now.Sub(p.lastRamp) < p.limit.RampInterval {
		return
	}
	if p.currentSlots >= p.limit.MaxConcurrent {
		return
	}

	newSlots := p.currentSlots + p.limit.RampStep
	if newSlots > p.limit.MaxConcurrent {
		newSlots = p.limit.MaxConcurrent
	}

	slog.Debug("adaptive pool ramping up", "pool", p.name, "old_slots", p.currentSlots, "new_slots", newSlots)

	p.resize(newSlots)
	p.lastRamp = now
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
