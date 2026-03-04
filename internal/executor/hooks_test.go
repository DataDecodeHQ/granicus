package executor

import (
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/analytehealth/granicus/internal/config"
	"github.com/analytehealth/granicus/internal/graph"
)

func testHookGraph(t *testing.T) (*graph.Graph, *config.PipelineConfig) {
	t.Helper()
	cfg := &config.PipelineConfig{
		Pipeline: "test_pipe",
		Connections: map[string]*config.ConnectionConfig{
			"bq": {Name: "bq", Type: "bigquery", Properties: map[string]string{"project": "p", "dataset": "dev_analytics"}},
		},
		Assets: []config.AssetConfig{
			{Name: "stg_orders", Type: "sql", Source: "models/stg_orders.sql", DestinationConnection: "bq", Layer: "staging"},
		},
	}
	inputs := []graph.AssetInput{
		{Name: "stg_orders", Type: "sql", Source: "models/stg_orders.sql", DestinationConnection: "bq", Layer: "staging"},
	}
	g, err := graph.BuildGraph(inputs, nil)
	if err != nil {
		t.Fatal(err)
	}
	return g, cfg
}

func TestWriteContextHook_NilClient(t *testing.T) {
	g, cfg := testHookGraph(t)
	projectRoot := t.TempDir()

	hook := WriteContextHook(nil)
	err := hook(g, cfg, projectRoot, nil)
	if err != nil {
		t.Fatalf("expected nil error with nil BQ client, got: %v", err)
	}

	dbPath := filepath.Join(projectRoot, ".granicus", "context.db")
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Fatal("context.db was not created")
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var count int
	db.QueryRow("SELECT count(*) FROM assets").Scan(&count)
	if count != 1 {
		t.Errorf("expected 1 asset row, got %d", count)
	}
}

func TestDuckDBAssemblyHook_SkipsWhenAssetNotInResults(t *testing.T) {
	g, cfg := testHookGraph(t)
	projectRoot := t.TempDir()

	rr := &RunResult{
		Results: []NodeResult{
			{AssetName: "stg_orders", Status: "success"},
		},
	}

	hook := DuckDBAssemblyHook()
	err := hook(g, cfg, projectRoot, rr)
	if err != nil {
		t.Fatalf("expected nil error when asset not in results, got: %v", err)
	}
}

func TestDuckDBAssemblyHook_SkipsWhenAssetFailed(t *testing.T) {
	g, cfg := testHookGraph(t)
	projectRoot := t.TempDir()

	rr := &RunResult{
		Results: []NodeResult{
			{AssetName: "publish_dashboard_parquet", Status: "failed"},
		},
	}

	hook := DuckDBAssemblyHook()
	err := hook(g, cfg, projectRoot, rr)
	if err != nil {
		t.Fatalf("expected nil error when asset failed, got: %v", err)
	}
}

func TestDuckDBAssemblyHook_SkipsWhenNilRunResult(t *testing.T) {
	g, cfg := testHookGraph(t)
	projectRoot := t.TempDir()

	hook := DuckDBAssemblyHook()
	err := hook(g, cfg, projectRoot, nil)
	if err != nil {
		t.Fatalf("expected nil error with nil RunResult, got: %v", err)
	}
}

func TestAssetSucceeded(t *testing.T) {
	tests := []struct {
		name     string
		rr       *RunResult
		asset    string
		expected bool
	}{
		{"nil result", nil, "x", false},
		{"not found", &RunResult{Results: []NodeResult{{AssetName: "y", Status: "success"}}}, "x", false},
		{"failed", &RunResult{Results: []NodeResult{{AssetName: "x", Status: "failed"}}}, "x", false},
		{"skipped", &RunResult{Results: []NodeResult{{AssetName: "x", Status: "skipped"}}}, "x", false},
		{"success", &RunResult{Results: []NodeResult{{AssetName: "x", Status: "success"}}}, "x", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := assetSucceeded(tt.rr, tt.asset); got != tt.expected {
				t.Errorf("assetSucceeded() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestRunPostHooks_ContinuesOnError(t *testing.T) {
	g, cfg := testHookGraph(t)
	projectRoot := t.TempDir()

	called := 0
	failHook := func(g *graph.Graph, cfg *config.PipelineConfig, pr string, rr *RunResult) error {
		called++
		return errors.New("deliberate failure")
	}
	okHook := func(g *graph.Graph, cfg *config.PipelineConfig, pr string, rr *RunResult) error {
		called++
		return nil
	}

	failures := RunPostHooks([]PostRunHook{failHook, okHook}, g, cfg, projectRoot, nil)

	if called != 2 {
		t.Errorf("expected both hooks to run, got %d calls", called)
	}
	if failures != 1 {
		t.Errorf("expected 1 failure, got %d", failures)
	}
}
