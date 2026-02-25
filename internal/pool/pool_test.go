package pool

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPool_AcquireRelease(t *testing.T) {
	p := NewPool("test", 2, 5*time.Second)

	if err := p.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	if p.InUse() != 1 {
		t.Errorf("expected 1 in use, got %d", p.InUse())
	}

	if err := p.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	if p.InUse() != 2 {
		t.Errorf("expected 2 in use, got %d", p.InUse())
	}

	p.Release()
	if p.InUse() != 1 {
		t.Errorf("expected 1 in use after release, got %d", p.InUse())
	}

	p.Release()
	if p.InUse() != 0 {
		t.Errorf("expected 0 in use, got %d", p.InUse())
	}
}

func TestPool_Timeout(t *testing.T) {
	p := NewPool("test", 1, 100*time.Millisecond)

	if err := p.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Second acquire should timeout
	err := p.Acquire(context.Background())
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if got := err.Error(); got == "" {
		t.Error("expected non-empty error")
	}

	p.Release()
}

func TestPool_ContextCancellation(t *testing.T) {
	p := NewPool("test", 1, 10*time.Second)

	if err := p.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err := p.Acquire(ctx)
	if err == nil {
		t.Fatal("expected context cancelled error")
	}

	p.Release()
}

func TestPool_ConcurrencyLimit(t *testing.T) {
	p := NewPool("test", 3, 5*time.Second)

	var maxConcurrent int32
	var current int32
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := p.Acquire(context.Background()); err != nil {
				t.Errorf("acquire: %v", err)
				return
			}
			cur := atomic.AddInt32(&current, 1)
			for {
				old := atomic.LoadInt32(&maxConcurrent)
				if cur <= old || atomic.CompareAndSwapInt32(&maxConcurrent, old, cur) {
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
			atomic.AddInt32(&current, -1)
			p.Release()
		}()
	}

	wg.Wait()

	if mc := atomic.LoadInt32(&maxConcurrent); mc > 3 {
		t.Errorf("max concurrent %d exceeded pool size 3", mc)
	}
	if mc := atomic.LoadInt32(&maxConcurrent); mc < 2 {
		t.Errorf("max concurrent %d too low, pool not utilized", mc)
	}
}

func TestPool_DefaultTimeout(t *testing.T) {
	p := NewPool("test", 1, 0)
	if p.Timeout != DefaultTimeout {
		t.Errorf("expected default timeout %v, got %v", DefaultTimeout, p.Timeout)
	}
}

func TestPoolManager_AcquireRelease(t *testing.T) {
	pm := NewPoolManager(map[string]PoolConfig{
		"bq": {Slots: 2, ParsedTimeout: 5 * time.Second},
	})

	if err := pm.Acquire(context.Background(), "bq"); err != nil {
		t.Fatal(err)
	}
	pm.Release("bq")

	// Non-existent pool
	if err := pm.Acquire(context.Background(), "nope"); err == nil {
		t.Fatal("expected error for non-existent pool")
	}

	// Release non-existent (should not panic)
	pm.Release("nope")
}

func TestPoolManager_MultiplePools(t *testing.T) {
	pm := NewPoolManager(map[string]PoolConfig{
		"bq":  {Slots: 1, ParsedTimeout: time.Second},
		"pg":  {Slots: 2, ParsedTimeout: time.Second},
	})

	// Fill bq
	if err := pm.Acquire(context.Background(), "bq"); err != nil {
		t.Fatal(err)
	}

	// pg should still be available
	if err := pm.Acquire(context.Background(), "pg"); err != nil {
		t.Fatal(err)
	}

	// bq should timeout
	err := pm.Acquire(context.Background(), "bq")
	if err == nil {
		t.Fatal("expected bq timeout")
	}

	pm.Release("bq")
	pm.Release("pg")
}

func TestPoolManager_Pool(t *testing.T) {
	pm := NewPoolManager(map[string]PoolConfig{
		"bq": {Slots: 5},
	})

	p := pm.Pool("bq")
	if p == nil {
		t.Fatal("expected pool")
	}
	if p.Slots != 5 {
		t.Errorf("expected 5 slots, got %d", p.Slots)
	}

	if pm.Pool("nope") != nil {
		t.Error("expected nil for non-existent pool")
	}
}
