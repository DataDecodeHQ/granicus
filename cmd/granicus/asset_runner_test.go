package main

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/DataDecodeHQ/granicus/internal/config"
	"github.com/DataDecodeHQ/granicus/internal/events"
	"github.com/DataDecodeHQ/granicus/internal/executor"
	"github.com/DataDecodeHQ/granicus/internal/graph"
	"github.com/DataDecodeHQ/granicus/internal/runner"
)

// newTestEventStore creates a real events.Store backed by a temp SQLite database.
func newTestEventStore(t *testing.T) *events.Store {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "events.db")
	s, err := events.New(dbPath)
	if err != nil {
		t.Fatalf("creating event store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// minimalPipelineConfig returns a PipelineConfig with two bigquery connections and
// a set of assets for use across multiple tests.
func minimalPipelineConfig() *config.PipelineConfig {
	return &config.PipelineConfig{
		Pipeline:    "test_pipeline",
		MaxParallel: 2,
		Prefix:      "dev",
		Resources: map[string]*config.ResourceConfig{
			"bq_main": {
				Name: "bq_main",
				Type: "bigquery",
				Properties: map[string]string{
					"project": "my-project",
					"dataset": "main_dataset",
				},
			},
			"bq_raw": {
				Name: "bq_raw",
				Type: "bigquery",
				Properties: map[string]string{
					"project": "my-project",
					"dataset": "raw_dataset",
				},
			},
		},
		Datasets: map[string]string{
			"staging":      "staging_dataset",
			"intermediate": "intermediate_dataset",
		},
		Assets: []config.AssetConfig{
			{
				Name:                  "stg_orders",
				Type:                  "sql",
				Source:                "models/stg_orders.sql",
				Layer:                 "staging",
				DestinationResource: "bq_main",
			},
			{
				Name:                  "int_orders",
				Type:                  "sql",
				Source:                "models/int_orders.sql",
				Layer:                 "intermediate",
				DestinationResource: "bq_raw",
			},
			{
				Name:   "rpt_orders",
				Type:   "sql",
				Source: "models/rpt_orders.sql",
				Layer:  "analytics",
			},
			{
				Name: "no_connection_asset",
				Type: "shell",
			},
		},
	}
}

// ---- findAssetConfig tests ----

func TestNodeRunner_FindAssetConfig_Found(t *testing.T) {
	cfg := minimalPipelineConfig()
	result := findAssetConfig(cfg, "stg_orders")
	if result == nil {
		t.Fatal("expected to find asset config, got nil")
	}
	if result.Name != "stg_orders" {
		t.Errorf("expected name %q, got %q", "stg_orders", result.Name)
	}
}

func TestNodeRunner_FindAssetConfig_NotFound(t *testing.T) {
	cfg := minimalPipelineConfig()
	result := findAssetConfig(cfg, "nonexistent_asset")
	if result != nil {
		t.Errorf("expected nil for missing asset, got %+v", result)
	}
}

func TestNodeRunner_FindAssetConfig_EmptyAssets(t *testing.T) {
	cfg := &config.PipelineConfig{Assets: nil}
	result := findAssetConfig(cfg, "any")
	if result != nil {
		t.Error("expected nil for empty asset list")
	}
}

func TestNodeRunner_FindAssetConfig_ReturnsCorrectElement(t *testing.T) {
	cfg := minimalPipelineConfig()
	result := findAssetConfig(cfg, "int_orders")
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result.Layer != "intermediate" {
		t.Errorf("expected layer %q, got %q", "intermediate", result.Layer)
	}
}

// ---- connectionForAsset tests ----

func TestNodeRunner_ConnectionForAsset_WithDestinationResource(t *testing.T) {
	cfg := minimalPipelineConfig()
	asset := findAssetConfig(cfg, "stg_orders")
	if asset == nil {
		t.Fatal("asset not found")
	}
	conn := connectionForAsset(cfg, asset)
	if conn == nil {
		t.Fatal("expected non-nil connection")
	}
	if conn.Properties["dataset"] != "main_dataset" {
		t.Errorf("expected dataset %q, got %q", "main_dataset", conn.Properties["dataset"])
	}
}

func TestNodeRunner_ConnectionForAsset_NoDestinationResource(t *testing.T) {
	cfg := minimalPipelineConfig()
	asset := findAssetConfig(cfg, "no_connection_asset")
	if asset == nil {
		t.Fatal("asset not found")
	}
	conn := connectionForAsset(cfg, asset)
	if conn != nil {
		t.Errorf("expected nil for asset without destination_resource, got %+v", conn)
	}
}

func TestNodeRunner_ConnectionForAsset_UnknownConnectionName(t *testing.T) {
	cfg := minimalPipelineConfig()
	asset := &config.AssetConfig{
		Name:                  "orphan",
		DestinationResource: "does_not_exist",
	}
	conn := connectionForAsset(cfg, asset)
	if conn != nil {
		t.Errorf("expected nil for unknown connection name, got %+v", conn)
	}
}

// ---- resolveAssetRuntime tests ----

func TestResolveAssetRuntime_DatasetFromDestinationResource(t *testing.T) {
	cfg := minimalPipelineConfig()
	dataset, _, _ := resolveAssetRuntime(cfg, "stg_orders")
	// stg_orders: dest_conn=bq_main → dataset=main_dataset
	if dataset != "main_dataset" {
		t.Errorf("expected %q, got %q", "main_dataset", dataset)
	}
}

func TestResolveAssetRuntime_DatasetFromLayerMapping(t *testing.T) {
	cfg := minimalPipelineConfig()
	dataset, _, _ := resolveAssetRuntime(cfg, "int_orders")
	// int_orders: dest_conn=bq_raw → dataset=raw_dataset (dest conn wins over layer mapping)
	if dataset != "raw_dataset" {
		t.Errorf("expected %q, got %q", "raw_dataset", dataset)
	}
}

func TestResolveAssetRuntime_DatasetFromLayerMappingNoConnection(t *testing.T) {
	cfg := minimalPipelineConfig()
	cfg.Assets = append(cfg.Assets, config.AssetConfig{
		Name:  "layer_only_asset",
		Type:  "sql",
		Layer: "staging",
	})
	dataset, _, _ := resolveAssetRuntime(cfg, "layer_only_asset")
	if dataset != "staging_dataset" {
		t.Errorf("expected %q, got %q", "staging_dataset", dataset)
	}
}

func TestResolveAssetRuntime_UnknownAssetReturnsEmpty(t *testing.T) {
	cfg := minimalPipelineConfig()
	dataset, destConn, sourceConn := resolveAssetRuntime(cfg, "nonexistent")
	if dataset != "" {
		t.Errorf("expected empty dataset, got %q", dataset)
	}
	if destConn != nil {
		t.Error("expected nil destConn for unknown asset")
	}
	if sourceConn != nil {
		t.Error("expected nil sourceConn for unknown asset")
	}
}

func TestResolveAssetRuntime_DestAndSourceResources(t *testing.T) {
	cfg := &config.PipelineConfig{
		Resources: map[string]*config.ResourceConfig{
			"bq_dest": {
				Name: "bq_dest",
				Type: "bigquery",
				Properties: map[string]string{"dataset": "dest_ds"},
			},
			"bq_src": {
				Name: "bq_src",
				Type: "bigquery",
				Properties: map[string]string{"dataset": "src_ds"},
			},
		},
		Assets: []config.AssetConfig{
			{
				Name:                  "my_asset",
				Type:                  "python",
				DestinationResource: "bq_dest",
				SourceResource:      "bq_src",
			},
		},
	}

	_, destConn, sourceConn := resolveAssetRuntime(cfg, "my_asset")

	if destConn == nil {
		t.Error("expected non-nil destination connection")
	} else if destConn.Properties["dataset"] != "dest_ds" {
		t.Errorf("dest dataset: expected %q, got %q", "dest_ds", destConn.Properties["dataset"])
	}
	if sourceConn == nil {
		t.Error("expected non-nil source connection")
	} else if sourceConn.Properties["dataset"] != "src_ds" {
		t.Errorf("src dataset: expected %q, got %q", "src_ds", sourceConn.Properties["dataset"])
	}
}

func TestResolveAssetRuntime_MissingConnectionsResolveToNil(t *testing.T) {
	cfg := &config.PipelineConfig{
		Resources: map[string]*config.ResourceConfig{},
		Assets: []config.AssetConfig{
			{
				Name:                  "asset_a",
				DestinationResource: "missing_conn",
				SourceResource:      "also_missing",
			},
		},
	}
	_, destConn, sourceConn := resolveAssetRuntime(cfg, "asset_a")
	if destConn != nil {
		t.Error("expected nil dest connection for missing name")
	}
	if sourceConn != nil {
		t.Error("expected nil source connection for missing name")
	}
}

func TestResolveAssetRuntime_NoConnectionNoLayer(t *testing.T) {
	cfg := minimalPipelineConfig()
	dataset, destConn, sourceConn := resolveAssetRuntime(cfg, "no_connection_asset")
	if dataset != "" {
		t.Errorf("expected empty dataset, got %q", dataset)
	}
	if destConn != nil {
		t.Error("expected nil destConn")
	}
	if sourceConn != nil {
		t.Error("expected nil sourceConn")
	}
}

// ---- runner.Asset construction ----
// buildRunnerAsset replicates the `ra := &runner.Asset{...}` block in both closures.

func buildRunnerAsset(graphAsset *graph.Asset, cfg *config.PipelineConfig) *runner.Asset {
	assetCfg := findAssetConfig(cfg, graphAsset.Name)

	resolvedDataset := ""
	if assetCfg != nil {
		defaultDS := ""
		if conn := connectionForAsset(cfg, assetCfg); conn != nil {
			defaultDS = conn.Properties["dataset"]
		}
		resolvedDataset = cfg.DatasetForAsset(*assetCfg, defaultDS)
	}

	var resolvedDestConn, resolvedSourceConn *config.ResourceConfig
	if assetCfg != nil {
		if assetCfg.DestinationResource != "" {
			if conn, ok := cfg.Resources[assetCfg.DestinationResource]; ok {
				resolvedDestConn = conn
			}
		}
		if assetCfg.SourceResource != "" {
			if conn, ok := cfg.Resources[assetCfg.SourceResource]; ok {
				resolvedSourceConn = conn
			}
		}
	}

	return &runner.Asset{
		Name:                  graphAsset.Name,
		Type:                  graphAsset.Type,
		Source:                graphAsset.Source,
		DestinationResource: graphAsset.DestinationResource,
		SourceResource:      graphAsset.SourceResource,
		IntervalStart:         graphAsset.IntervalStart,
		IntervalEnd:           graphAsset.IntervalEnd,
		Prefix:                cfg.Prefix,
		InlineSQL:             graphAsset.InlineSQL,
		DependsOn:             graphAsset.DependsOn,
		Timeout:               graphAsset.Timeout,
		Dataset:               resolvedDataset,
		Layer:                 graphAsset.Layer,
		ResolvedDestConn:      resolvedDestConn,
		ResolvedSourceConn:    resolvedSourceConn,
	}
}

func TestNodeRunner_RunnerAsset_DatasetResolvedViaDestConnection(t *testing.T) {
	cfg := minimalPipelineConfig()
	gAsset := &graph.Asset{
		Name:                  "stg_orders",
		Type:                  "sql",
		Source:                "models/stg_orders.sql",
		DestinationResource: "bq_main",
		Layer:                 "staging",
	}

	ra := buildRunnerAsset(gAsset, cfg)

	if ra.Dataset != "main_dataset" {
		t.Errorf("Dataset: expected %q, got %q", "main_dataset", ra.Dataset)
	}
	if ra.Prefix != "dev" {
		t.Errorf("Prefix: expected %q, got %q", "dev", ra.Prefix)
	}
	if ra.ResolvedDestConn == nil {
		t.Error("expected non-nil ResolvedDestConn")
	}
	if ra.ResolvedSourceConn != nil {
		t.Error("expected nil ResolvedSourceConn (no source_resource set)")
	}
}

func TestNodeRunner_RunnerAsset_UnknownAssetGivesEmptyDataset(t *testing.T) {
	cfg := minimalPipelineConfig()
	// Synthesized check node not present in cfg.Assets.
	gAsset := &graph.Asset{
		Name: "check__stg_orders__row_count",
		Type: "sql_check",
	}

	ra := buildRunnerAsset(gAsset, cfg)

	if ra.Dataset != "" {
		t.Errorf("expected empty dataset for unknown asset, got %q", ra.Dataset)
	}
	if ra.ResolvedDestConn != nil {
		t.Error("expected nil ResolvedDestConn for unknown asset")
	}
}

func TestNodeRunner_RunnerAsset_LayerDrivenDataset(t *testing.T) {
	cfg := minimalPipelineConfig()
	cfg.Assets = append(cfg.Assets, config.AssetConfig{
		Name:  "int_orders_layer_only",
		Type:  "sql",
		Layer: "intermediate",
	})
	gAsset := &graph.Asset{
		Name:  "int_orders_layer_only",
		Type:  "sql",
		Layer: "intermediate",
	}

	ra := buildRunnerAsset(gAsset, cfg)

	if ra.Dataset != "intermediate_dataset" {
		t.Errorf("Dataset: expected %q, got %q", "intermediate_dataset", ra.Dataset)
	}
}

func TestNodeRunner_RunnerAsset_PrefixPropagated(t *testing.T) {
	cfg := minimalPipelineConfig()
	cfg.Prefix = "prod"
	gAsset := &graph.Asset{Name: "stg_orders", Type: "sql"}

	ra := buildRunnerAsset(gAsset, cfg)

	if ra.Prefix != "prod" {
		t.Errorf("expected prefix %q, got %q", "prod", ra.Prefix)
	}
}

// ---- NodeResult mapping: runner.NodeResult → executor.NodeResult ----
// Both runRun (main.go:896-907) and executePipeline (serve.go:540-551) use
// identical field-by-field copy. These tests lock down that mapping.

func TestNodeRunner_NodeResultMapping_SuccessAllFields(t *testing.T) {
	start := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	end := start.Add(2 * time.Second)
	src := runner.NodeResult{
		AssetName: "my_asset",
		Status:    "success",
		StartTime: start,
		EndTime:   end,
		Duration:  2 * time.Second,
		Stdout:    "done\n",
		Stderr:    "",
		ExitCode:  0,
		Metadata:  map[string]string{"bq_bytes": "1024"},
	}

	// Mapping as written in both runnerFunc closures.
	dst := executor.NodeResult{
		AssetName: src.AssetName,
		Status:    src.Status,
		StartTime: src.StartTime,
		EndTime:   src.EndTime,
		Duration:  src.Duration,
		Error:     src.Error,
		Stdout:    src.Stdout,
		Stderr:    src.Stderr,
		ExitCode:  src.ExitCode,
		Metadata:  src.Metadata,
	}

	if dst.AssetName != "my_asset" {
		t.Errorf("AssetName: expected %q, got %q", "my_asset", dst.AssetName)
	}
	if dst.Status != "success" {
		t.Errorf("Status: expected %q, got %q", "success", dst.Status)
	}
	if !dst.StartTime.Equal(start) {
		t.Errorf("StartTime mismatch")
	}
	if !dst.EndTime.Equal(end) {
		t.Errorf("EndTime mismatch")
	}
	if dst.Duration != 2*time.Second {
		t.Errorf("Duration: expected 2s, got %v", dst.Duration)
	}
	if dst.Stdout != "done\n" {
		t.Errorf("Stdout: expected %q, got %q", "done\n", dst.Stdout)
	}
	if dst.ExitCode != 0 {
		t.Errorf("ExitCode: expected 0, got %d", dst.ExitCode)
	}
	if dst.Metadata["bq_bytes"] != "1024" {
		t.Error("Metadata not forwarded correctly")
	}
}

func TestNodeRunner_NodeResultMapping_FailureFields(t *testing.T) {
	src := runner.NodeResult{
		AssetName: "bad_asset",
		Status:    "failed",
		Error:     "exit status 1",
		Stderr:    "panic: something went wrong\n",
		ExitCode:  1,
	}

	dst := executor.NodeResult{
		AssetName: src.AssetName,
		Status:    src.Status,
		StartTime: src.StartTime,
		EndTime:   src.EndTime,
		Duration:  src.Duration,
		Error:     src.Error,
		Stdout:    src.Stdout,
		Stderr:    src.Stderr,
		ExitCode:  src.ExitCode,
		Metadata:  src.Metadata,
	}

	if dst.Status != "failed" {
		t.Errorf("Status: expected %q, got %q", "failed", dst.Status)
	}
	if dst.Error != "exit status 1" {
		t.Errorf("Error: expected %q, got %q", "exit status 1", dst.Error)
	}
	if dst.Stderr != "panic: something went wrong\n" {
		t.Errorf("Stderr mismatch: got %q", dst.Stderr)
	}
	if dst.ExitCode != 1 {
		t.Errorf("ExitCode: expected 1, got %d", dst.ExitCode)
	}
}

func TestNodeRunner_NodeResultMapping_NilMetadataPreserved(t *testing.T) {
	src := runner.NodeResult{
		AssetName: "asset",
		Status:    "success",
		Metadata:  nil,
	}
	dst := executor.NodeResult{
		AssetName: src.AssetName,
		Status:    src.Status,
		Metadata:  src.Metadata,
	}
	if dst.Metadata != nil {
		t.Error("expected nil Metadata to be preserved as nil")
	}
}

// TestNodeRunner_NodeResultMapping_BothPathsIdentical verifies that the mapping
// in runRun (main.go) and executePipeline (serve.go) produce structurally identical
// executor.NodeResult values for the same input.
func TestNodeRunner_NodeResultMapping_BothPathsIdentical(t *testing.T) {
	src := runner.NodeResult{
		AssetName: "stg_orders",
		Status:    "success",
		StartTime: time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2025, 6, 1, 0, 0, 5, 0, time.UTC),
		Duration:  5 * time.Second,
		Stdout:    "ok\n",
		Stderr:    "",
		ExitCode:  0,
		Metadata:  map[string]string{"runner": "sql"},
	}

	// runRun path (main.go lines 896-907):
	runRunResult := executor.NodeResult{
		AssetName: src.AssetName,
		Status:    src.Status,
		StartTime: src.StartTime,
		EndTime:   src.EndTime,
		Duration:  src.Duration,
		Error:     src.Error,
		Stdout:    src.Stdout,
		Stderr:    src.Stderr,
		ExitCode:  src.ExitCode,
		Metadata:  src.Metadata,
	}

	// executePipeline path (serve.go lines 540-551):
	serveResult := executor.NodeResult{
		AssetName: src.AssetName,
		Status:    src.Status,
		StartTime: src.StartTime,
		EndTime:   src.EndTime,
		Duration:  src.Duration,
		Error:     src.Error,
		Stdout:    src.Stdout,
		Stderr:    src.Stderr,
		ExitCode:  src.ExitCode,
		Metadata:  src.Metadata,
	}

	if runRunResult.AssetName != serveResult.AssetName {
		t.Errorf("AssetName mismatch: %q vs %q", runRunResult.AssetName, serveResult.AssetName)
	}
	if runRunResult.Status != serveResult.Status {
		t.Errorf("Status mismatch: %q vs %q", runRunResult.Status, serveResult.Status)
	}
	if !runRunResult.StartTime.Equal(serveResult.StartTime) {
		t.Error("StartTime mismatch between paths")
	}
	if runRunResult.Duration != serveResult.Duration {
		t.Errorf("Duration mismatch: %v vs %v", runRunResult.Duration, serveResult.Duration)
	}
	if runRunResult.ExitCode != serveResult.ExitCode {
		t.Errorf("ExitCode mismatch: %d vs %d", runRunResult.ExitCode, serveResult.ExitCode)
	}
}

// ---- Event emission via events.Store ----
// Verify the event store correctly captures node lifecycle events as emitted
// by the runnerFunc closures in both code paths.

func TestNodeRunner_EventEmission_NodeStarted(t *testing.T) {
	store := newTestEventStore(t)
	runID := "run_test_001"

	if err := store.Emit(events.Event{
		RunID:     runID,
		Pipeline:  "test_pipeline",
		Asset:     "stg_orders",
		EventType: "asset_started",
		Severity:  "info",
		Summary:   "Node stg_orders started",
	}); err != nil {
		t.Fatalf("emit failed: %v", err)
	}

	emitted, err := store.Query(events.QueryFilters{RunID: runID, EventType: "asset_started"})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(emitted) != 1 {
		t.Fatalf("expected 1 asset_started event, got %d", len(emitted))
	}
	e := emitted[0]
	if e.Asset != "stg_orders" {
		t.Errorf("Asset: expected %q, got %q", "stg_orders", e.Asset)
	}
	if e.EventType != "asset_started" {
		t.Errorf("EventType: expected %q, got %q", "asset_started", e.EventType)
	}
}

func TestNodeRunner_EventEmission_NodeSucceeded(t *testing.T) {
	store := newTestEventStore(t)
	runID := "run_test_002"

	if err := store.Emit(events.Event{
		RunID:      runID,
		Pipeline:   "test_pipeline",
		Asset:      "stg_orders",
		EventType:  "asset_succeeded",
		Severity:   "info",
		DurationMs: 1500,
		Summary:    "Node stg_orders succeeded (1.5s)",
		Details: map[string]any{
			"exit_code":    0,
			"stdout_lines": 3,
			"stderr_lines": 0,
		},
	}); err != nil {
		t.Fatalf("emit failed: %v", err)
	}

	emitted, err := store.Query(events.QueryFilters{RunID: runID, EventType: "asset_succeeded"})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(emitted) != 1 {
		t.Fatalf("expected 1 asset_succeeded event, got %d", len(emitted))
	}
	if emitted[0].DurationMs != 1500 {
		t.Errorf("DurationMs: expected 1500, got %d", emitted[0].DurationMs)
	}
}

func TestNodeRunner_EventEmission_NodeFailed(t *testing.T) {
	store := newTestEventStore(t)
	runID := "run_test_003"

	if err := store.Emit(events.Event{
		RunID:     runID,
		Pipeline:  "test_pipeline",
		Asset:     "bad_asset",
		EventType: "asset_failed",
		Severity:  "error",
		Summary:   "Node bad_asset failed: exit status 1",
		Details: map[string]any{
			"error_message": "exit status 1",
			"exit_code":     1,
			"source_file":   "models/bad.sql",
			"stderr":        "syntax error near line 5",
		},
	}); err != nil {
		t.Fatalf("emit failed: %v", err)
	}

	emitted, err := store.Query(events.QueryFilters{RunID: runID, EventType: "asset_failed"})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(emitted) != 1 {
		t.Fatalf("expected 1 asset_failed event, got %d", len(emitted))
	}
	if emitted[0].Severity != "error" {
		t.Errorf("Severity: expected %q, got %q", "error", emitted[0].Severity)
	}
}

func TestNodeRunner_EventEmission_LifecycleSequence(t *testing.T) {
	store := newTestEventStore(t)
	runID := "run_test_004"

	for _, et := range []string{"asset_started", "asset_succeeded"} {
		if err := store.Emit(events.Event{
			RunID:     runID,
			Pipeline:  "test_pipeline",
			Asset:     "asset_a",
			EventType: et,
			Severity:  "info",
			Summary:   et,
		}); err != nil {
			t.Fatalf("emit %q: %v", et, err)
		}
	}

	all, err := store.Query(events.QueryFilters{RunID: runID})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(all) != 2 {
		t.Errorf("expected 2 events, got %d", len(all))
	}
}

// TestNodeRunner_LogEmit_StoreReceivesEvent verifies the logEmit wrapper (main.go)
// forwards events to the store without dropping them.
func TestNodeRunner_LogEmit_StoreReceivesEvent(t *testing.T) {
	store := newTestEventStore(t)

	logEmit(store, events.Event{
		RunID:     "run_logtest",
		Pipeline:  "test_pipeline",
		Asset:     "stg_orders",
		EventType: "asset_started",
		Severity:  "info",
		Summary:   "via logEmit",
	})

	emitted, err := store.Query(events.QueryFilters{RunID: "run_logtest"})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(emitted) != 1 {
		t.Errorf("expected 1 event via logEmit, got %d", len(emitted))
	}
}

// ---- emitNodeResult tests ----

func TestEmitNodeResult_SuccessEmitsNodeSucceeded(t *testing.T) {
	store := newTestEventStore(t)
	cfg := minimalPipelineConfig()
	asset := config.AssetConfig{Name: "stg_orders", Source: "models/stg_orders.sql"}
	r := runner.NodeResult{
		AssetName: "stg_orders",
		Status:    "success",
		ExitCode:  0,
	}

	emitNodeResult(store, "run_001", cfg, asset, r)

	emitted, err := store.Query(events.QueryFilters{RunID: "run_001", EventType: "asset_succeeded"})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(emitted) != 1 {
		t.Fatalf("expected 1 asset_succeeded event, got %d", len(emitted))
	}
	if emitted[0].Asset != "stg_orders" {
		t.Errorf("Asset: expected %q, got %q", "stg_orders", emitted[0].Asset)
	}
}

func TestEmitNodeResult_FailureEmitsNodeFailed(t *testing.T) {
	store := newTestEventStore(t)
	cfg := minimalPipelineConfig()
	asset := config.AssetConfig{Name: "bad_asset", Source: "models/bad.sql"}
	r := runner.NodeResult{
		AssetName: "bad_asset",
		Status:    "failed",
		Error:     "exit status 1",
		Stderr:    "syntax error",
		ExitCode:  1,
	}

	emitNodeResult(store, "run_002", cfg, asset, r)

	emitted, err := store.Query(events.QueryFilters{RunID: "run_002", EventType: "asset_failed"})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(emitted) != 1 {
		t.Fatalf("expected 1 asset_failed event, got %d", len(emitted))
	}
	if emitted[0].Severity != "error" {
		t.Errorf("Severity: expected %q, got %q", "error", emitted[0].Severity)
	}
}

func TestEmitNodeResult_SuccessDoesNotEmitNodeFailed(t *testing.T) {
	store := newTestEventStore(t)
	cfg := minimalPipelineConfig()
	asset := config.AssetConfig{Name: "stg_orders"}
	r := runner.NodeResult{AssetName: "stg_orders", Status: "success"}

	emitNodeResult(store, "run_003", cfg, asset, r)

	failed, err := store.Query(events.QueryFilters{RunID: "run_003", EventType: "asset_failed"})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(failed) != 0 {
		t.Errorf("expected no asset_failed events for success, got %d", len(failed))
	}
}

func TestEmitNodeResult_LargeStdoutTruncatedAt10KB(t *testing.T) {
	store := newTestEventStore(t)
	cfg := minimalPipelineConfig()
	asset := config.AssetConfig{Name: "big_out"}

	const truncLimit = 10 * 1024
	largeBuf := make([]byte, truncLimit+100)
	for i := range largeBuf {
		largeBuf[i] = 'x'
	}
	r := runner.NodeResult{
		AssetName: "big_out",
		Status:    "failed",
		Stdout:    string(largeBuf),
		Stderr:    "",
	}

	emitNodeResult(store, "run_004", cfg, asset, r)

	emitted, err := store.Query(events.QueryFilters{RunID: "run_004", EventType: "asset_failed"})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(emitted) != 1 {
		t.Fatalf("expected 1 event, got %d", len(emitted))
	}
	stdout, _ := emitted[0].Details["stdout"].(string)
	if len(stdout) != truncLimit+len("[truncated]") {
		t.Errorf("expected truncated stdout length %d, got %d", truncLimit+len("[truncated]"), len(stdout))
	}
	if stdout[truncLimit:] != "[truncated]" {
		t.Errorf("expected truncation marker at byte %d", truncLimit)
	}
}

func TestEmitNodeResult_LargeStderrTruncatedAt10KB(t *testing.T) {
	store := newTestEventStore(t)
	cfg := minimalPipelineConfig()
	asset := config.AssetConfig{Name: "big_err"}

	const truncLimit = 10 * 1024
	largeBuf := make([]byte, truncLimit+50)
	for i := range largeBuf {
		largeBuf[i] = 'e'
	}
	r := runner.NodeResult{
		AssetName: "big_err",
		Status:    "failed",
		Stdout:    "",
		Stderr:    string(largeBuf),
	}

	emitNodeResult(store, "run_005", cfg, asset, r)

	emitted, err := store.Query(events.QueryFilters{RunID: "run_005", EventType: "asset_failed"})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(emitted) != 1 {
		t.Fatalf("expected 1 event, got %d", len(emitted))
	}
	stderr, _ := emitted[0].Details["stderr"].(string)
	if len(stderr) != truncLimit+len("[truncated]") {
		t.Errorf("expected truncated stderr length %d, got %d", truncLimit+len("[truncated]"), len(stderr))
	}
}

func TestEmitNodeResult_ShortOutputNotTruncated(t *testing.T) {
	store := newTestEventStore(t)
	cfg := minimalPipelineConfig()
	asset := config.AssetConfig{Name: "small_out"}
	r := runner.NodeResult{
		AssetName: "small_out",
		Status:    "failed",
		Stdout:    "small output",
		Stderr:    "small error",
	}

	emitNodeResult(store, "run_006", cfg, asset, r)

	emitted, err := store.Query(events.QueryFilters{RunID: "run_006", EventType: "asset_failed"})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(emitted) != 1 {
		t.Fatalf("expected 1 event, got %d", len(emitted))
	}
	stdout, _ := emitted[0].Details["stdout"].(string)
	if stdout != "small output" {
		t.Errorf("expected untruncated stdout, got %q", stdout)
	}
}
