package pool

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

const DefaultTimeout = 5 * time.Minute

type Pool struct {
	Name    string
	Slots   int
	Timeout time.Duration
	sem     chan struct{}
}

// NewPool creates a concurrency pool with the given name, slot count, and acquisition timeout.
func NewPool(name string, slots int, timeout time.Duration) *Pool {
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	return &Pool{
		Name:    name,
		Slots:   slots,
		Timeout: timeout,
		sem:     make(chan struct{}, slots),
	}
}

// Acquire blocks until a pool slot is available, the context is cancelled, or the timeout expires.
func (p *Pool) Acquire(ctx context.Context) error {
	slog.Debug("pool acquiring slot", "pool", p.Name, "in_use", len(p.sem), "slots", p.Slots)

	timer := time.NewTimer(p.Timeout)
	defer timer.Stop()

	select {
	case p.sem <- struct{}{}:
		slog.Debug("pool slot acquired", "pool", p.Name, "in_use", len(p.sem), "slots", p.Slots)
		return nil
	case <-ctx.Done():
		return fmt.Errorf("pool %s: context cancelled while waiting for slot: %w", p.Name, ctx.Err())
	case <-timer.C:
		return fmt.Errorf("pool %s: timeout after %v waiting for slot (%d/%d in use)", p.Name, p.Timeout, len(p.sem), p.Slots)
	}
}

// Release returns a slot back to the pool.
func (p *Pool) Release() {
	<-p.sem
	slog.Debug("pool slot released", "pool", p.Name, "in_use", len(p.sem), "slots", p.Slots)
}

// InUse returns the number of currently acquired slots.
func (p *Pool) InUse() int {
	return len(p.sem)
}

type PoolManager struct {
	mu    sync.RWMutex
	pools map[string]*Pool
}

// NewPoolManager creates a PoolManager with pools initialized from the given configs.
func NewPoolManager(configs map[string]PoolConfig) *PoolManager {
	pm := &PoolManager{
		pools: make(map[string]*Pool, len(configs)),
	}
	for name, cfg := range configs {
		timeout := cfg.ParsedTimeout
		if timeout <= 0 {
			timeout = DefaultTimeout
		}
		pm.pools[name] = NewPool(name, cfg.Slots, timeout)
	}
	return pm
}

type PoolConfig struct {
	Slots         int
	ParsedTimeout time.Duration
	DefaultFor    string
}

// Acquire acquires a slot from the named pool, returning an error if the pool is not found.
func (pm *PoolManager) Acquire(ctx context.Context, poolName string) error {
	pm.mu.RLock()
	p, ok := pm.pools[poolName]
	pm.mu.RUnlock()
	if !ok {
		return fmt.Errorf("pool %q not found", poolName)
	}
	return p.Acquire(ctx)
}

// Release returns a slot to the named pool.
func (pm *PoolManager) Release(poolName string) {
	pm.mu.RLock()
	p, ok := pm.pools[poolName]
	pm.mu.RUnlock()
	if !ok {
		return
	}
	p.Release()
}

// Pool returns the pool with the given name, or nil if not found.
func (pm *PoolManager) Pool(name string) *Pool {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return pm.pools[name]
}
