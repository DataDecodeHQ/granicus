package checker

import (
	"strings"
	"testing"

	"github.com/analytehealth/granicus/internal/config"
	"github.com/analytehealth/granicus/internal/graph"
)

func makeSourceCfg(sources map[string]config.SourceConfig, connections map[string]*config.ConnectionConfig) *config.PipelineConfig {
	return &config.PipelineConfig{
		Pipeline:    "test",
		Connections: connections,
		Sources:     sources,
		Assets: []config.AssetConfig{
			{Name: "placeholder", Type: "shell", Source: "placeholder.sh"},
		},
	}
}

func bqConn(project string) map[string]*config.ConnectionConfig {
	return map[string]*config.ConnectionConfig{
		"bq": {
			Type:       "bigquery",
			Properties: map[string]string{"project": project, "dataset": "raw"},
		},
	}
}

func TestGenerateSourceCheckNodes_ExistsNotEmpty(t *testing.T) {
	cfg := makeSourceCfg(map[string]config.SourceConfig{
		"orders": {
			Connection: "bq",
			Identifier: "myproject.raw.orders",
		},
	}, bqConn("myproject"))

	nodes, deps := GenerateSourceCheckNodes(cfg)

	if len(nodes) != 1 {
		t.Fatalf("expected 1 check node, got %d", len(nodes))
	}

	n := nodes[0]
	if n.Name != "check:source:orders:default:exists_not_empty" {
		t.Errorf("unexpected name: %s", n.Name)
	}
	if n.Type != "sql_check" {
		t.Errorf("expected sql_check, got %s", n.Type)
	}
	if n.DestinationConnection != "bq" {
		t.Errorf("expected connection bq, got %s", n.DestinationConnection)
	}
	if n.SourceAsset != "orders" {
		t.Errorf("expected SourceAsset orders, got %s", n.SourceAsset)
	}

	if !strings.Contains(n.InlineSQL, "EMPTY_OR_MISSING") {
		t.Errorf("SQL missing EMPTY_OR_MISSING: %s", n.InlineSQL)
	}
	if !strings.Contains(n.InlineSQL, "NOT EXISTS") {
		t.Errorf("SQL missing NOT EXISTS: %s", n.InlineSQL)
	}
	if !strings.Contains(n.InlineSQL, "myproject.raw.orders") {
		t.Errorf("SQL missing identifier: %s", n.InlineSQL)
	}

	d := deps[n.Name]
	if len(d) != 1 || d[0] != "source:orders" {
		t.Errorf("unexpected deps for %s: %v", n.Name, d)
	}
}

func TestGenerateSourceCheckNodes_Freshness(t *testing.T) {
	cfg := makeSourceCfg(map[string]config.SourceConfig{
		"orders": {
			Connection:    "bq",
			Identifier:    "myproject.raw.orders",
			ExpectedFresh: "2h",
		},
	}, bqConn("myproject"))

	nodes, _ := GenerateSourceCheckNodes(cfg)

	if len(nodes) != 2 {
		t.Fatalf("expected 2 check nodes (exists_not_empty + freshness), got %d", len(nodes))
	}

	names := nodeNames(nodes)
	if !names["check:source:orders:default:freshness"] {
		t.Error("missing freshness check")
	}

	for _, n := range nodes {
		if strings.Contains(n.Name, "freshness") {
			if !strings.Contains(n.InlineSQL, "STALE_SOURCE") {
				t.Errorf("freshness SQL missing STALE_SOURCE: %s", n.InlineSQL)
			}
			if !strings.Contains(n.InlineSQL, "staleness_minutes") {
				t.Errorf("freshness SQL missing staleness_minutes: %s", n.InlineSQL)
			}
			// 2h = 120 minutes
			if !strings.Contains(n.InlineSQL, "> 120") {
				t.Errorf("freshness SQL missing expected minute threshold (120): %s", n.InlineSQL)
			}
		}
	}
}

func TestGenerateSourceCheckNodes_PrimaryKey(t *testing.T) {
	cfg := makeSourceCfg(map[string]config.SourceConfig{
		"orders": {
			Connection: "bq",
			Identifier: "myproject.raw.orders",
			PrimaryKey: "order_id",
		},
	}, bqConn("myproject"))

	nodes, _ := GenerateSourceCheckNodes(cfg)

	// exists_not_empty + pk_not_null + pk_unique
	if len(nodes) != 3 {
		t.Fatalf("expected 3 check nodes, got %d", len(nodes))
	}

	names := nodeNames(nodes)
	if !names["check:source:orders:default:pk_not_null"] {
		t.Error("missing pk_not_null check")
	}
	if !names["check:source:orders:default:pk_unique"] {
		t.Error("missing pk_unique check")
	}

	for _, n := range nodes {
		if strings.Contains(n.Name, "pk_not_null") {
			if !strings.Contains(n.InlineSQL, "NULL_PK") {
				t.Errorf("pk_not_null SQL missing NULL_PK: %s", n.InlineSQL)
			}
			if !strings.Contains(n.InlineSQL, "order_id IS NULL") {
				t.Errorf("pk_not_null SQL missing IS NULL clause: %s", n.InlineSQL)
			}
		}
		if strings.Contains(n.Name, "pk_unique") {
			if !strings.Contains(n.InlineSQL, "dupe_count") {
				t.Errorf("pk_unique SQL missing dupe_count: %s", n.InlineSQL)
			}
			if !strings.Contains(n.InlineSQL, "GROUP BY order_id") {
				t.Errorf("pk_unique SQL missing GROUP BY: %s", n.InlineSQL)
			}
			if !strings.Contains(n.InlineSQL, "HAVING COUNT(*) > 1") {
				t.Errorf("pk_unique SQL missing HAVING COUNT(*) > 1: %s", n.InlineSQL)
			}
		}
	}
}

func TestGenerateSourceCheckNodes_ExpectedColumns(t *testing.T) {
	cfg := makeSourceCfg(map[string]config.SourceConfig{
		"orders": {
			Connection:      "bq",
			Identifier:      "myproject.raw.orders",
			ExpectedColumns: []string{"order_id", "status", "created_at"},
		},
	}, bqConn("myproject"))

	nodes, _ := GenerateSourceCheckNodes(cfg)

	// exists_not_empty + expected_columns
	if len(nodes) != 2 {
		t.Fatalf("expected 2 check nodes, got %d", len(nodes))
	}

	names := nodeNames(nodes)
	if !names["check:source:orders:default:expected_columns"] {
		t.Error("missing expected_columns check")
	}

	for _, n := range nodes {
		if strings.Contains(n.Name, "expected_columns") {
			if !strings.Contains(n.InlineSQL, "COLUMN_MISSING") {
				t.Errorf("expected_columns SQL missing COLUMN_MISSING: %s", n.InlineSQL)
			}
			if !strings.Contains(n.InlineSQL, "'order_id'") {
				t.Errorf("expected_columns SQL missing quoted column: %s", n.InlineSQL)
			}
			if !strings.Contains(n.InlineSQL, "INFORMATION_SCHEMA.COLUMNS") {
				t.Errorf("expected_columns SQL missing INFORMATION_SCHEMA: %s", n.InlineSQL)
			}
			if !strings.Contains(n.InlineSQL, "table_name = 'orders'") {
				t.Errorf("expected_columns SQL missing table_name filter: %s", n.InlineSQL)
			}
			if !strings.Contains(n.InlineSQL, "myproject.raw.INFORMATION_SCHEMA.COLUMNS") {
				t.Errorf("expected_columns SQL missing dataset reference: %s", n.InlineSQL)
			}
		}
	}
}

func TestGenerateSourceCheckNodes_AllChecks(t *testing.T) {
	cfg := makeSourceCfg(map[string]config.SourceConfig{
		"orders": {
			Connection:      "bq",
			Identifier:      "myproject.raw.orders",
			PrimaryKey:      "order_id",
			ExpectedFresh:   "30m",
			ExpectedColumns: []string{"order_id", "status"},
		},
	}, bqConn("myproject"))

	nodes, _ := GenerateSourceCheckNodes(cfg)

	// exists_not_empty + freshness + pk_not_null + pk_unique + expected_columns
	if len(nodes) != 5 {
		t.Fatalf("expected 5 check nodes, got %d", len(nodes))
	}

	names := nodeNames(nodes)
	for _, expected := range []string{
		"check:source:orders:default:exists_not_empty",
		"check:source:orders:default:freshness",
		"check:source:orders:default:pk_not_null",
		"check:source:orders:default:pk_unique",
		"check:source:orders:default:expected_columns",
	} {
		if !names[expected] {
			t.Errorf("missing check: %s", expected)
		}
	}

	// 30m = 30 minutes
	for _, n := range nodes {
		if strings.Contains(n.Name, "freshness") {
			if !strings.Contains(n.InlineSQL, "> 30") {
				t.Errorf("freshness SQL missing 30 minute threshold: %s", n.InlineSQL)
			}
		}
	}
}

func TestGenerateSourceCheckNodes_NoSources(t *testing.T) {
	cfg := makeSourceCfg(nil, bqConn("myproject"))

	nodes, deps := GenerateSourceCheckNodes(cfg)

	if len(nodes) != 0 {
		t.Errorf("expected 0 nodes for empty sources, got %d", len(nodes))
	}
	if len(deps) != 0 {
		t.Errorf("expected 0 deps for empty sources, got %d", len(deps))
	}
}

func TestGenerateSourceCheckNodes_MultipleSources(t *testing.T) {
	cfg := makeSourceCfg(map[string]config.SourceConfig{
		"orders": {
			Connection: "bq",
			Identifier: "myproject.raw.orders",
		},
		"customers": {
			Connection: "bq",
			Identifier: "myproject.raw.customers",
			PrimaryKey: "customer_id",
		},
	}, bqConn("myproject"))

	nodes, deps := GenerateSourceCheckNodes(cfg)

	// orders: 1, customers: 3
	if len(nodes) != 4 {
		t.Fatalf("expected 4 check nodes, got %d: %v", len(nodes), nodeNameList(nodes))
	}

	for _, n := range nodes {
		d := deps[n.Name]
		if len(d) != 1 {
			t.Errorf("expected 1 dep for %s, got %v", n.Name, d)
		}
	}
}

func TestGenerateSourceCheckNodes_DepsPointToSource(t *testing.T) {
	cfg := makeSourceCfg(map[string]config.SourceConfig{
		"events": {
			Connection: "bq",
			Identifier: "myproject.raw.events",
			PrimaryKey: "event_id",
		},
	}, bqConn("myproject"))

	_, deps := GenerateSourceCheckNodes(cfg)

	for checkName, sources := range deps {
		if len(sources) != 1 || sources[0] != "source:events" {
			t.Errorf("check %s deps should point to 'source:events', got %v", checkName, sources)
		}
	}
}

func TestParseIdentifierDatasetTable(t *testing.T) {
	tests := []struct {
		identifier string
		dataset    string
		tableName  string
	}{
		{"project.dataset.table", "dataset", "table"},
		{"myproject.raw.orders", "raw", "orders"},
		{"dataset.table", "dataset", "table"},
		{"singlepart", "", ""},
		{"", "", ""},
	}

	for _, tc := range tests {
		ds, tbl := parseIdentifierDatasetTable(tc.identifier)
		if ds != tc.dataset {
			t.Errorf("identifier %q: expected dataset %q, got %q", tc.identifier, tc.dataset, ds)
		}
		if tbl != tc.tableName {
			t.Errorf("identifier %q: expected table %q, got %q", tc.identifier, tc.tableName, tbl)
		}
	}
}

func nodeNames(nodes []graph.AssetInput) map[string]bool {
	names := map[string]bool{}
	for _, n := range nodes {
		names[n.Name] = true
	}
	return names
}

func nodeNameList(nodes []graph.AssetInput) []string {
	var names []string
	for _, n := range nodes {
		names = append(names, n.Name)
	}
	return names
}
