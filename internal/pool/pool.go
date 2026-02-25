package pool

import (
	"context"
	"fmt"
	"log"
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

func (p *Pool) Acquire(ctx context.Context) error {
	log.Printf("pool %s: acquiring slot (%d/%d in use)", p.Name, len(p.sem), p.Slots)

	timer := time.NewTimer(p.Timeout)
	defer timer.Stop()

	select {
	case p.sem <- struct{}{}:
		log.Printf("pool %s: slot acquired (%d/%d in use)", p.Name, len(p.sem), p.Slots)
		return nil
	case <-ctx.Done():
		return fmt.Errorf("pool %s: context cancelled while waiting for slot: %w", p.Name, ctx.Err())
	case <-timer.C:
		return fmt.Errorf("pool %s: timeout after %v waiting for slot (%d/%d in use)", p.Name, p.Timeout, len(p.sem), p.Slots)
	}
}

func (p *Pool) Release() {
	<-p.sem
	log.Printf("pool %s: slot released (%d/%d in use)", p.Name, len(p.sem), p.Slots)
}

func (p *Pool) InUse() int {
	return len(p.sem)
}

type PoolManager struct {
	mu    sync.RWMutex
	pools map[string]*Pool
}

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

func (pm *PoolManager) Acquire(ctx context.Context, poolName string) error {
	pm.mu.RLock()
	p, ok := pm.pools[poolName]
	pm.mu.RUnlock()
	if !ok {
		return fmt.Errorf("pool %q not found", poolName)
	}
	return p.Acquire(ctx)
}

func (pm *PoolManager) Release(poolName string) {
	pm.mu.RLock()
	p, ok := pm.pools[poolName]
	pm.mu.RUnlock()
	if !ok {
		return
	}
	p.Release()
}

func (pm *PoolManager) Pool(name string) *Pool {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return pm.pools[name]
}
