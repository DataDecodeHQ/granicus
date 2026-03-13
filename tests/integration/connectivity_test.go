package integration

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Andrew-DataDecode/Granicus/internal/config"
	"github.com/Andrew-DataDecode/Granicus/internal/graph"
)

func TestSQLDirectiveParsing(t *testing.T) {
	tmpRoot := t.TempDir()

	pipelineDir := filepath.Join(tmpRoot, "project", "granicus_pipeline", "test_pipeline")
	sqlDir := filepath.Join(pipelineDir, "sql", "staging")
	if err := os.MkdirAll(sqlDir, 0o755); err != nil {
		t.Fatal(err)
	}

	sqlFiles := map[string]string{
		"stg_orders.sql": `-- granicus:
--   depends_on:
--     - source.orders
--   layer: staging
--   grain: order_id
SELECT * FROM {{ source "orders" }}
`,
		"stg_accounts.sql": `-- granicus:
--   depends_on:
--     - source.accounts
--   layer: staging
--   grain: account_id
--   time_column: created_at
--   interval_unit: day
--   start_date: "2020-01-01"
SELECT * FROM {{ source "accounts" }}
`,
		"int_order_summary.sql": `-- granicus:
--   depends_on:
--     - stg_orders
--     - stg_accounts
--   layer: intermediate
--   grain: order_id
SELECT o.*, a.name
FROM {{ ref "stg_orders" }} o
JOIN {{ ref "stg_accounts" }} a ON o.account_id = a.account_id
`,
	}

	for name, content := range sqlFiles {
		if err := os.WriteFile(filepath.Join(sqlDir, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	for name := range sqlFiles {
		path := filepath.Join(sqlDir, name)
		found, directives, err := graph.ParseDirectivesWithBlock(path)
		if err != nil {
			t.Errorf("parsing %s: %v", name, err)
			continue
		}
		if !found {
			t.Errorf("%s: no granicus block found", name)
			continue
		}
		if directives.Layer == "" {
			t.Errorf("%s: layer not parsed", name)
		}
		if directives.Grain == "" {
			t.Errorf("%s: grain not parsed", name)
		}
		if len(directives.DependsOn) == 0 {
			t.Errorf("%s: depends_on not parsed", name)
		}
	}

	// Verify specific values
	_, d, _ := graph.ParseDirectivesWithBlock(filepath.Join(sqlDir, "stg_orders.sql"))
	if d.Layer != "staging" {
		t.Errorf("stg_orders: layer = %q, want staging", d.Layer)
	}
	if d.Grain != "order_id" {
		t.Errorf("stg_orders: grain = %q, want order_id", d.Grain)
	}

	_, d, _ = graph.ParseDirectivesWithBlock(filepath.Join(sqlDir, "stg_accounts.sql"))
	if d.TimeColumn != "created_at" {
		t.Errorf("stg_accounts: time_column = %q, want created_at", d.TimeColumn)
	}
	if d.IntervalUnit != "day" {
		t.Errorf("stg_accounts: interval_unit = %q, want day", d.IntervalUnit)
	}

	_, d, _ = graph.ParseDirectivesWithBlock(filepath.Join(sqlDir, "int_order_summary.sql"))
	if len(d.DependsOn) != 2 {
		t.Errorf("int_order_summary: depends_on has %d entries, want 2", len(d.DependsOn))
	}
}

func TestFullDAGConstruction(t *testing.T) {
	tmpRoot := t.TempDir()

	pipelineDir := filepath.Join(tmpRoot, "project", "granicus_pipeline", "dag_test")
	sqlDir := filepath.Join(pipelineDir, "sql")
	if err := os.MkdirAll(sqlDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Write SQL files with directives
	stgSQL := `-- granicus:
--   layer: staging
--   grain: id
SELECT * FROM raw_table
`
	intSQL := `-- granicus:
--   depends_on:
--     - stg_data
--   layer: intermediate
--   grain: id
SELECT * FROM {{ ref "stg_data" }}
`
	rptSQL := `-- granicus:
--   depends_on:
--     - int_summary
--   layer: report
--   grain: id
SELECT * FROM {{ ref "int_summary" }}
`

	if err := os.WriteFile(filepath.Join(sqlDir, "stg_data.sql"), []byte(stgSQL), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sqlDir, "int_summary.sql"), []byte(intSQL), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sqlDir, "rpt_final.sql"), []byte(rptSQL), 0o644); err != nil {
		t.Fatal(err)
	}

	// Write pipeline.yaml
	yaml := `pipeline: dag_test
connections:
  bq:
    type: bigquery
    project: test
    dataset: test
assets:
  - name: stg_data
    type: sql
    source: sql/stg_data.sql
    destination_connection: bq
  - name: int_summary
    type: sql
    source: sql/int_summary.sql
    destination_connection: bq
  - name: rpt_final
    type: sql
    source: sql/rpt_final.sql
    destination_connection: bq
`
	if err := os.WriteFile(filepath.Join(pipelineDir, "pipeline.yaml"), []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg, err := config.LoadConfig(filepath.Join(pipelineDir, "pipeline.yaml"))
	if err != nil {
		t.Fatalf("loading config: %v", err)
	}

	deps, directives, err := graph.ParseAllDirectives(cfg, pipelineDir)
	if err != nil {
		t.Fatalf("parsing directives: %v", err)
	}

	inputs := graph.ConfigToAssetInputs(cfg)
	for i := range inputs {
		if d, ok := directives[inputs[i].Name]; ok {
			if d.Layer != "" {
				inputs[i].Layer = d.Layer
			}
			if d.Grain != "" {
				inputs[i].Grain = d.Grain
			}
		}
	}

	g, err := graph.BuildGraph(inputs, deps)
	if err != nil {
		t.Fatalf("building graph: %v", err)
	}

	// Verify DAG structure
	order := g.TopologicalSort()
	if len(order) != 3 {
		t.Errorf("expected 3 nodes in DAG, got %d", len(order))
	}

	// stg_data should come before int_summary, which comes before rpt_final
	positions := map[string]int{}
	for i, name := range order {
		positions[name] = i
	}

	if positions["stg_data"] >= positions["int_summary"] {
		t.Errorf("stg_data should come before int_summary in topological sort")
	}
	if positions["int_summary"] >= positions["rpt_final"] {
		t.Errorf("int_summary should come before rpt_final in topological sort")
	}

	// Verify directive metadata
	if d, ok := directives["stg_data"]; !ok || d.Layer != "staging" {
		t.Error("stg_data: missing or wrong layer directive")
	}
	if d, ok := directives["int_summary"]; !ok || d.Layer != "intermediate" {
		t.Error("int_summary: missing or wrong layer directive")
	}
	if d, ok := directives["rpt_final"]; !ok || d.Layer != "report" {
		t.Error("rpt_final: missing or wrong layer directive")
	}
}
