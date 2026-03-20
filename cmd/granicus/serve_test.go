package main

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/DataDecodeHQ/granicus/internal/config"
	"github.com/DataDecodeHQ/granicus/internal/events"
	"github.com/DataDecodeHQ/granicus/internal/server"
)

// newTestPEC builds a minimal PipelineExecContext backed by a temp event store.
func newTestPEC(t *testing.T, cfg *config.PipelineConfig) PipelineExecContext {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "events.db")
	store, err := events.New(dbPath)
	if err != nil {
		t.Fatalf("creating event store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return PipelineExecContext{
		cfg:         cfg,
		projectRoot: t.TempDir(),
		eventStore:  store,
		ctx:         context.Background(),
	}
}

// ---- PipelineExecContext field tests ----

func TestPipelineExecContext_FieldsPreserved(t *testing.T) {
	cfg := minimalPipelineConfig()
	pec := newTestPEC(t, cfg)

	if pec.cfg == nil {
		t.Error("expected non-nil cfg")
	}
	if pec.cfg.Pipeline != "test_pipeline" {
		t.Errorf("pipeline: expected %q, got %q", "test_pipeline", pec.cfg.Pipeline)
	}
	if pec.projectRoot == "" {
		t.Error("expected non-empty projectRoot")
	}
	if pec.eventStore == nil {
		t.Error("expected non-nil eventStore")
	}
	if pec.ctx == nil {
		t.Error("expected non-nil ctx")
	}
}

func TestPipelineExecContext_RunIDDefaultsEmpty(t *testing.T) {
	pec := newTestPEC(t, minimalPipelineConfig())
	if pec.runID != "" {
		t.Errorf("expected empty runID before assignment, got %q", pec.runID)
	}
}

func TestPipelineExecContext_RunIDAssignment(t *testing.T) {
	pec := newTestPEC(t, minimalPipelineConfig())
	pec.runID = "run_abc123"
	if pec.runID != "run_abc123" {
		t.Errorf("expected %q, got %q", "run_abc123", pec.runID)
	}
}

func TestPipelineExecContext_PoolFieldsDefaultNil(t *testing.T) {
	pec := newTestPEC(t, minimalPipelineConfig())
	if pec.poolMgr != nil {
		t.Error("expected nil poolMgr before assignment")
	}
	if pec.assetPools != nil {
		t.Error("expected nil assetPools before assignment")
	}
}

func TestPipelineExecContext_DispatchDefaultsNil(t *testing.T) {
	pec := newTestPEC(t, minimalPipelineConfig())
	if pec.dispatch != nil {
		t.Error("expected nil dispatch before assignment")
	}
}

func TestPipelineExecContext_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	dbPath := filepath.Join(t.TempDir(), "events.db")
	store, err := events.New(dbPath)
	if err != nil {
		t.Fatalf("creating event store: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	pec := PipelineExecContext{
		cfg:         minimalPipelineConfig(),
		projectRoot: t.TempDir(),
		eventStore:  store,
		ctx:         ctx,
	}

	select {
	case <-pec.ctx.Done():
		t.Error("context should not be cancelled yet")
	default:
	}

	cancel()

	select {
	case <-pec.ctx.Done():
	default:
		t.Error("context should be cancelled after cancel()")
	}
}

// ---- runPipelineForScheduler ----

func TestRunPipelineForScheduler_NilEnvCfg_DoesNotPanic(t *testing.T) {
	cfg := &config.PipelineConfig{Pipeline: "noop", MaxParallel: 1}
	pec := newTestPEC(t, cfg)

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("unexpected panic: %v", r)
		}
	}()
	runPipelineForScheduler(pec, "dev", nil)
}

func TestRunPipelineForScheduler_WithEnvMerge_DoesNotPanic(t *testing.T) {
	cfg := &config.PipelineConfig{Pipeline: "merge_test", MaxParallel: 1, Prefix: "original"}
	envCfg := &config.EnvironmentConfig{
		Environments: map[string]*config.EnvironmentOverride{
			"dev": {Prefix: "dev_prefix"},
		},
	}
	pec := newTestPEC(t, cfg)

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("unexpected panic during env merge: %v", r)
		}
	}()
	runPipelineForScheduler(pec, "dev", envCfg)
}

// ---- runPipelineForTrigger ----

func TestRunPipelineForTrigger_NilEnvCfg_DoesNotPanic(t *testing.T) {
	cfg := &config.PipelineConfig{Pipeline: "noop", MaxParallel: 1}
	pec := newTestPEC(t, cfg)
	pec.runID = events.GenerateRunID()

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("unexpected panic: %v", r)
		}
	}()

	req := server.TriggerRequest{Assets: []string{"stg_orders"}, FromDate: "2025-01-01", ToDate: "2025-01-31"}
	runPipelineForTrigger(pec, "dev", nil, req)
}
