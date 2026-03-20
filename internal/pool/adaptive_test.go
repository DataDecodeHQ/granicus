package pool

import (
	"context"
	"testing"
	"time"
)

func TestAdaptivePool_InitialSlots(t *testing.T) {
	limit := DefaultLimit("bigquery")
	p := NewAdaptivePool("bq", limit)

	if p.CurrentSlots() != limit.InitialSlots {
		t.Errorf("expected initial slots %d, got %d", limit.InitialSlots, p.CurrentSlots())
	}
	if p.InUse() != 0 {
		t.Errorf("expected 0 in use, got %d", p.InUse())
	}
}

func TestAdaptivePool_RampUp(t *testing.T) {
	limit := ResourceLimit{
		MaxConcurrent: 20,
		InitialSlots:  2,
		RampStep:      3,
		RampInterval:  50 * time.Millisecond,
	}
	p := NewAdaptivePool("test", limit)

	if p.CurrentSlots() != 2 {
		t.Fatalf("expected 2 initial slots, got %d", p.CurrentSlots())
	}

	// Force lastRamp into the past so the next Acquire triggers a ramp
	p.mu.Lock()
	p.lastRamp = time.Now().Add(-100 * time.Millisecond)
	p.mu.Unlock()

	if err := p.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	p.Release()

	if p.CurrentSlots() != 5 {
		t.Errorf("expected 5 slots after ramp, got %d", p.CurrentSlots())
	}

	// Another ramp
	p.mu.Lock()
	p.lastRamp = time.Now().Add(-100 * time.Millisecond)
	p.mu.Unlock()

	if err := p.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	p.Release()

	if p.CurrentSlots() != 8 {
		t.Errorf("expected 8 slots after second ramp, got %d", p.CurrentSlots())
	}
}

func TestAdaptivePool_RampCapsAtMax(t *testing.T) {
	limit := ResourceLimit{
		MaxConcurrent: 5,
		InitialSlots:  3,
		RampStep:      4,
		RampInterval:  50 * time.Millisecond,
	}
	p := NewAdaptivePool("test", limit)

	p.mu.Lock()
	p.lastRamp = time.Now().Add(-100 * time.Millisecond)
	p.mu.Unlock()

	if err := p.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	p.Release()

	if p.CurrentSlots() != 5 {
		t.Errorf("expected slots capped at 5, got %d", p.CurrentSlots())
	}
}

func TestAdaptivePool_BackpressureHalvesSlots(t *testing.T) {
	limit := ResourceLimit{
		MaxConcurrent: 20,
		InitialSlots:  2,
		RampStep:      2,
		RampInterval:  time.Hour,
	}
	p := NewAdaptivePool("test", limit)

	// Manually set slots higher to test halving
	p.mu.Lock()
	p.resize(10)
	p.mu.Unlock()

	if p.CurrentSlots() != 10 {
		t.Fatalf("expected 10 slots, got %d", p.CurrentSlots())
	}

	p.SignalBackpressure()

	if p.CurrentSlots() != 5 {
		t.Errorf("expected 5 slots after backpressure, got %d", p.CurrentSlots())
	}
}

func TestAdaptivePool_BackpressureFloor(t *testing.T) {
	limit := ResourceLimit{
		MaxConcurrent: 20,
		InitialSlots:  4,
		RampStep:      2,
		RampInterval:  time.Hour,
	}
	p := NewAdaptivePool("test", limit)

	// Slots at 4, halving to 2 is below InitialSlots=4, should stay at 4
	p.SignalBackpressure()

	if p.CurrentSlots() != 4 {
		t.Errorf("expected slots floored at InitialSlots 4, got %d", p.CurrentSlots())
	}
}

func TestAdaptivePool_ConsecutiveBackpressureDoublesCooldown(t *testing.T) {
	limit := ResourceLimit{
		MaxConcurrent: 100,
		InitialSlots:  2,
		RampStep:      2,
		RampInterval:  time.Hour,
	}
	p := NewAdaptivePool("test", limit)

	p.mu.Lock()
	p.resize(64)
	p.mu.Unlock()

	p.SignalBackpressure() // 64 -> 32, cooldown = 10s

	p.mu.Lock()
	cd1 := p.cooldown
	p.mu.Unlock()
	if cd1 != 10*time.Second {
		t.Errorf("expected first cooldown 10s, got %v", cd1)
	}

	p.SignalBackpressure() // 32 -> 16, cooldown = 20s (consecutive)

	p.mu.Lock()
	cd2 := p.cooldown
	p.mu.Unlock()
	if cd2 != 20*time.Second {
		t.Errorf("expected second cooldown 20s, got %v", cd2)
	}

	p.SignalBackpressure() // 16 -> 8, cooldown = 40s

	p.mu.Lock()
	cd3 := p.cooldown
	p.mu.Unlock()
	if cd3 != 40*time.Second {
		t.Errorf("expected third cooldown 40s, got %v", cd3)
	}
}

func TestAdaptivePool_CooldownCapsAt5Min(t *testing.T) {
	limit := ResourceLimit{
		MaxConcurrent: 10000,
		InitialSlots:  1,
		RampStep:      1,
		RampInterval:  time.Hour,
	}
	p := NewAdaptivePool("test", limit)

	p.mu.Lock()
	p.resize(4096)
	p.mu.Unlock()

	// Signal enough times to exceed 5 min cap
	// 10s -> 20s -> 40s -> 80s -> 160s -> 320s (>300s, capped)
	for i := 0; i < 6; i++ {
		p.SignalBackpressure()
	}

	p.mu.Lock()
	cd := p.cooldown
	p.mu.Unlock()
	if cd != 5*time.Minute {
		t.Errorf("expected cooldown capped at 5m, got %v", cd)
	}
}

func TestAdaptivePool_AcquireBlocksWhenFull(t *testing.T) {
	limit := ResourceLimit{
		MaxConcurrent: 1,
		InitialSlots:  1,
		RampStep:      1,
		RampInterval:  time.Hour,
	}
	p := NewAdaptivePool("test", limit)

	if err := p.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := p.Acquire(ctx)
	if err == nil {
		t.Fatal("expected acquire to fail when pool is full")
	}

	p.Release()
}

func TestAdaptivePool_ReleaseUnblocksWaiting(t *testing.T) {
	limit := ResourceLimit{
		MaxConcurrent: 1,
		InitialSlots:  1,
		RampStep:      1,
		RampInterval:  time.Hour,
	}
	p := NewAdaptivePool("test", limit)

	if err := p.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		done <- p.Acquire(context.Background())
	}()

	time.Sleep(50 * time.Millisecond)
	p.Release()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected successful acquire after release, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("acquire did not unblock after release")
	}

	p.Release()
}

func TestAdaptivePool_BackpressureWithInUseSlots(t *testing.T) {
	limit := ResourceLimit{
		MaxConcurrent: 20,
		InitialSlots:  2,
		RampStep:      2,
		RampInterval:  time.Hour,
	}
	p := NewAdaptivePool("test", limit)

	p.mu.Lock()
	p.resize(10)
	p.mu.Unlock()

	// Acquire 3 slots
	for i := 0; i < 3; i++ {
		if err := p.Acquire(context.Background()); err != nil {
			t.Fatal(err)
		}
	}

	p.SignalBackpressure() // 10 -> 5, 3 in use should be preserved

	if p.CurrentSlots() != 5 {
		t.Errorf("expected 5 slots, got %d", p.CurrentSlots())
	}

	// Should be able to release the 3 acquired slots
	for i := 0; i < 3; i++ {
		p.Release()
	}

	if p.InUse() != 0 {
		t.Errorf("expected 0 in use after releasing all, got %d", p.InUse())
	}
}

func TestAdaptivePoolManager_Basic(t *testing.T) {
	// Use config.ResourceConfig directly
	configs := map[string]*struct {
		Type string
	}{
		"bq": {Type: "bigquery"},
	}

	// Build pools manually since we can't import config in tests easily
	m := &AdaptivePoolManager{
		pools: make(map[string]*AdaptivePool),
	}
	for name, rc := range configs {
		limit := DefaultLimit(rc.Type)
		m.pools[name] = NewAdaptivePool(name, limit)
	}

	if err := m.Acquire(context.Background(), "bq"); err != nil {
		t.Fatal(err)
	}
	m.Release("bq")

	if err := m.Acquire(context.Background(), "nope"); err == nil {
		t.Fatal("expected error for non-existent pool")
	}

	m.Release("nope") // should not panic

	p := m.Pool("bq")
	if p == nil {
		t.Fatal("expected pool")
	}

	if m.Pool("nope") != nil {
		t.Error("expected nil for non-existent pool")
	}
}
