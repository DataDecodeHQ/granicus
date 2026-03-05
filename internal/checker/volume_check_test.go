package checker

import (
	"fmt"
	"strings"
	"testing"

	"github.com/Andrew-DataDecode/Granicus/internal/events"
)

func newVolumeTestStore(t *testing.T) *events.Store {
	t.Helper()
	s, err := events.New(t.TempDir() + "/events.db")
	if err != nil {
		t.Fatalf("creating store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func defaultVolumeCfg() VolumeCheckConfig {
	return VolumeCheckConfig{
		WindowSize:       10,
		AnomalyThreshold: 0.25,
		Response:         VolumeCheckWarn,
	}
}

func TestCheckVolumeAnomaly_FirstRun(t *testing.T) {
	store := newVolumeTestStore(t)

	result, err := CheckVolumeAnomaly(store, "pipe", "asset1", "run_001", 1000, defaultVolumeCfg())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result.IsAnomaly {
		t.Error("first run should not be flagged as anomaly")
	}
	if result.RowCount != 1000 {
		t.Errorf("expected row count 1000, got %d", result.RowCount)
	}
}

func TestCheckVolumeAnomaly_NormalVariation(t *testing.T) {
	store := newVolumeTestStore(t)
	cfg := defaultVolumeCfg()

	// Seed 5 runs with ~1000 rows each.
	for i, count := range []int64{1000, 1010, 990, 1005, 995} {
		runID := fmt.Sprintf("run_%03d", i+1)
		_, _ = CheckVolumeAnomaly(store, "pipe", "asset1", runID, count, cfg)
	}

	// Current run: 1020 rows — well within 25%.
	result, err := CheckVolumeAnomaly(store, "pipe", "asset1", "run_006", 1020, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsAnomaly {
		t.Errorf("expected no anomaly for small deviation, got deviation %.2f%%", result.DeviationPct*100)
	}
}

func TestCheckVolumeAnomaly_LargeIncrease(t *testing.T) {
	store := newVolumeTestStore(t)
	cfg := defaultVolumeCfg()

	// Seed baseline of 1000 rows.
	for i := 0; i < 5; i++ {
		runID := fmt.Sprintf("run_%03d", i+1)
		_, _ = CheckVolumeAnomaly(store, "pipe", "asset1", runID, 1000, cfg)
	}

	// Current run: 2000 rows — 100% above baseline, well over 25%.
	result, err := CheckVolumeAnomaly(store, "pipe", "asset1", "run_006", 2000, cfg)
	if err != nil {
		t.Fatalf("unexpected error on warn response: %v", err)
	}
	if !result.IsAnomaly {
		t.Error("expected anomaly for 100% increase")
	}
	if result.RowCount != 2000 {
		t.Errorf("expected row count 2000, got %d", result.RowCount)
	}
	if result.Average != 1000 {
		t.Errorf("expected average 1000, got %.2f", result.Average)
	}
}

func TestCheckVolumeAnomaly_LargeDecrease(t *testing.T) {
	store := newVolumeTestStore(t)
	cfg := defaultVolumeCfg()

	for i := 0; i < 5; i++ {
		runID := fmt.Sprintf("run_%03d", i+1)
		_, _ = CheckVolumeAnomaly(store, "pipe", "asset1", runID, 1000, cfg)
	}

	// 400 rows — 60% drop.
	result, err := CheckVolumeAnomaly(store, "pipe", "asset1", "run_006", 400, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.IsAnomaly {
		t.Error("expected anomaly for 60% decrease")
	}
}

func TestCheckVolumeAnomaly_EmitsVolumeAnomalyEvent(t *testing.T) {
	store := newVolumeTestStore(t)
	cfg := defaultVolumeCfg()

	for i := 0; i < 5; i++ {
		runID := fmt.Sprintf("run_%03d", i+1)
		_, _ = CheckVolumeAnomaly(store, "pipe", "asset1", runID, 1000, cfg)
	}

	_, _ = CheckVolumeAnomaly(store, "pipe", "asset1", "run_006", 2000, cfg)

	evts, err := store.Query(events.QueryFilters{
		Asset:     "asset1",
		EventType: "volume_anomaly",
	})
	if err != nil {
		t.Fatalf("querying events: %v", err)
	}
	if len(evts) != 1 {
		t.Fatalf("expected 1 volume_anomaly event, got %d", len(evts))
	}
	if !strings.Contains(evts[0].Summary, "asset1") {
		t.Errorf("summary should mention asset name: %s", evts[0].Summary)
	}
	if evts[0].Severity != "warning" {
		t.Errorf("expected severity warning, got %s", evts[0].Severity)
	}
}

func TestCheckVolumeAnomaly_ErrorResponse_BlocksOnAnomaly(t *testing.T) {
	store := newVolumeTestStore(t)
	cfg := VolumeCheckConfig{
		WindowSize:       10,
		AnomalyThreshold: 0.25,
		Response:         VolumeCheckError,
	}

	for i := 0; i < 5; i++ {
		runID := fmt.Sprintf("run_%03d", i+1)
		_, _ = CheckVolumeAnomaly(store, "pipe", "asset1", runID, 1000, cfg)
	}

	result, err := CheckVolumeAnomaly(store, "pipe", "asset1", "run_006", 2000, cfg)
	if err == nil {
		t.Error("expected error when response=error and anomaly detected")
	}
	if result == nil || !result.IsAnomaly {
		t.Error("expected anomaly result alongside error")
	}
}

func TestCheckVolumeAnomaly_ErrorResponse_EmitsErrorSeverity(t *testing.T) {
	store := newVolumeTestStore(t)
	cfg := VolumeCheckConfig{
		WindowSize:       10,
		AnomalyThreshold: 0.25,
		Response:         VolumeCheckError,
	}

	for i := 0; i < 3; i++ {
		runID := fmt.Sprintf("run_%03d", i+1)
		_, _ = CheckVolumeAnomaly(store, "pipe", "asset1", runID, 1000, cfg)
	}

	_, _ = CheckVolumeAnomaly(store, "pipe", "asset1", "run_004", 2000, cfg)

	evts, _ := store.Query(events.QueryFilters{Asset: "asset1", EventType: "volume_anomaly"})
	if len(evts) != 1 {
		t.Fatalf("expected 1 volume_anomaly event, got %d", len(evts))
	}
	if evts[0].Severity != "error" {
		t.Errorf("expected severity error, got %s", evts[0].Severity)
	}
}

func TestCheckVolumeAnomaly_ErrorResponse_NoError_WhenNormal(t *testing.T) {
	store := newVolumeTestStore(t)
	cfg := VolumeCheckConfig{
		WindowSize:       10,
		AnomalyThreshold: 0.25,
		Response:         VolumeCheckError,
	}

	for i := 0; i < 5; i++ {
		runID := fmt.Sprintf("run_%03d", i+1)
		_, _ = CheckVolumeAnomaly(store, "pipe", "asset1", runID, 1000, cfg)
	}

	_, err := CheckVolumeAnomaly(store, "pipe", "asset1", "run_006", 1050, cfg)
	if err != nil {
		t.Errorf("expected no error for normal deviation, got: %v", err)
	}
}

func TestCheckVolumeAnomaly_IgnoreResponse(t *testing.T) {
	store := newVolumeTestStore(t)
	cfg := VolumeCheckConfig{Response: VolumeCheckIgnore}

	result, err := CheckVolumeAnomaly(store, "pipe", "asset1", "run_001", 1000, cfg)
	if err != nil {
		t.Errorf("ignore response should not return error: %v", err)
	}
	if result != nil {
		t.Errorf("ignore response should return nil result, got %v", result)
	}
}

func TestCheckVolumeAnomaly_WindowSize_LimitsHistory(t *testing.T) {
	store := newVolumeTestStore(t)
	cfg := VolumeCheckConfig{
		WindowSize:       3,
		AnomalyThreshold: 0.25,
		Response:         VolumeCheckWarn,
	}

	// 7 runs with 1000 rows.
	for i := 0; i < 7; i++ {
		runID := fmt.Sprintf("run_%03d", i+1)
		_, _ = CheckVolumeAnomaly(store, "pipe", "asset1", runID, 1000, cfg)
	}

	// With window=3, the average is still 1000. 2000 should be anomaly.
	result, _ := CheckVolumeAnomaly(store, "pipe", "asset1", "run_008", 2000, cfg)
	if !result.IsAnomaly {
		t.Error("expected anomaly with window=3")
	}
}

func TestCheckVolumeAnomaly_StoresVolumeSnapshots(t *testing.T) {
	store := newVolumeTestStore(t)
	cfg := defaultVolumeCfg()

	for i, count := range []int64{100, 200, 300} {
		runID := fmt.Sprintf("run_%03d", i+1)
		_, _ = CheckVolumeAnomaly(store, "pipe", "asset1", runID, count, cfg)
	}

	evts, err := store.Query(events.QueryFilters{
		Asset:     "asset1",
		EventType: "volume_snapshot",
	})
	if err != nil {
		t.Fatalf("querying snapshots: %v", err)
	}
	if len(evts) != 3 {
		t.Errorf("expected 3 volume_snapshot events, got %d", len(evts))
	}
}

func TestCheckVolumeAnomaly_ExactThreshold_NotAnomaly(t *testing.T) {
	store := newVolumeTestStore(t)
	cfg := VolumeCheckConfig{
		WindowSize:       5,
		AnomalyThreshold: 0.25,
		Response:         VolumeCheckWarn,
	}

	for i := 0; i < 5; i++ {
		runID := fmt.Sprintf("run_%03d", i+1)
		_, _ = CheckVolumeAnomaly(store, "pipe", "asset1", runID, 1000, cfg)
	}

	// Exactly 25% above — deviation == threshold, should NOT be anomaly.
	result, err := CheckVolumeAnomaly(store, "pipe", "asset1", "run_006", 1250, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsAnomaly {
		t.Errorf("deviation exactly at threshold should not be anomaly, got %.4f", result.DeviationPct)
	}
}

func TestCheckVolumeAnomaly_JustAboveThreshold_IsAnomaly(t *testing.T) {
	store := newVolumeTestStore(t)
	cfg := VolumeCheckConfig{
		WindowSize:       5,
		AnomalyThreshold: 0.25,
		Response:         VolumeCheckWarn,
	}

	for i := 0; i < 5; i++ {
		runID := fmt.Sprintf("run_%03d", i+1)
		_, _ = CheckVolumeAnomaly(store, "pipe", "asset1", runID, 1000, cfg)
	}

	// 1251 rows — just over 25% threshold.
	result, _ := CheckVolumeAnomaly(store, "pipe", "asset1", "run_006", 1251, cfg)
	if !result.IsAnomaly {
		t.Errorf("deviation just above threshold should be anomaly, got %.4f", result.DeviationPct)
	}
}

func TestCheckVolumeAnomaly_DefaultConfig(t *testing.T) {
	store := newVolumeTestStore(t)

	// Zero-value config should apply defaults.
	cfg := VolumeCheckConfig{}

	for i := 0; i < 5; i++ {
		runID := fmt.Sprintf("run_%03d", i+1)
		_, _ = CheckVolumeAnomaly(store, "pipe", "asset1", runID, 1000, cfg)
	}

	result, err := CheckVolumeAnomaly(store, "pipe", "asset1", "run_006", 1000, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsAnomaly {
		t.Error("stable count should not be anomaly with default config")
	}
}
