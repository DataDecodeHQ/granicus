package checker

import (
	"strings"
	"testing"

	"github.com/Andrew-DataDecode/Granicus/internal/config"
	"github.com/Andrew-DataDecode/Granicus/internal/graph"
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
			Identifier: "raw",
			Tables:     []string{"orders"},
		},
	}, bqConn("myproject"))

	nodes, deps := GenerateSourceCheckNodes(cfg)

	if len(nodes) != 1 {
		t.Fatalf("expected 1 check node, got %d: %v", len(nodes), nodeNameList(nodes))
	}

	n := nodes[0]
	if n.Name != "check:source:orders:orders:exists_not_empty" {
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
	if !strings.Contains(n.InlineSQL, `source "orders" "orders"`) {
		t.Errorf("SQL missing source() call: %s", n.InlineSQL)
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
			Identifier:    "raw",
			Tables:        []string{"orders"},
			ExpectedFresh: "2h",
		},
	}, bqConn("myproject"))

	nodes, _ := GenerateSourceCheckNodes(cfg)

	if len(nodes) != 2 {
		t.Fatalf("expected 2 check nodes (exists_not_empty + freshness), got %d: %v", len(nodes), nodeNameList(nodes))
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
			if !strings.Contains(n.InlineSQL, "> 120") {
				t.Errorf("freshness SQL missing expected minute threshold (120): %s", n.InlineSQL)
			}
			if !strings.Contains(n.InlineSQL, `source "orders" "orders"`) {
				t.Errorf("freshness SQL missing source() call: %s", n.InlineSQL)
			}
		}
	}
}

func TestGenerateSourceCheckNodes_PrimaryKey(t *testing.T) {
	cfg := makeSourceCfg(map[string]config.SourceConfig{
		"orders": {
			Connection: "bq",
			Identifier: "raw",
			Tables:     []string{"orders"},
			PrimaryKey: "order_id",
		},
	}, bqConn("myproject"))

	nodes, _ := GenerateSourceCheckNodes(cfg)

	// exists_not_empty + pk_not_null + pk_unique
	if len(nodes) != 3 {
		t.Fatalf("expected 3 check nodes, got %d: %v", len(nodes), nodeNameList(nodes))
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
			if !strings.Contains(n.InlineSQL, `source "orders" "orders"`) {
				t.Errorf("pk_not_null SQL missing source() call: %s", n.InlineSQL)
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

func TestGenerateSourceCheckNodes_PrimaryKeyMultiTableSkipped(t *testing.T) {
	cfg := makeSourceCfg(map[string]config.SourceConfig{
		"mydb": {
			Connection: "bq",
			Identifier: "raw",
			Tables:     []string{"orders", "customers"},
			PrimaryKey: "id",
		},
	}, bqConn("myproject"))

	nodes, _ := GenerateSourceCheckNodes(cfg)

	names := nodeNames(nodes)
	if names["check:source:mydb:default:pk_not_null"] {
		t.Error("pk_not_null should not be generated for multi-table source")
	}
	if names["check:source:mydb:default:pk_unique"] {
		t.Error("pk_unique should not be generated for multi-table source")
	}
}

func TestGenerateSourceCheckNodes_ExpectedColumns(t *testing.T) {
	cfg := makeSourceCfg(map[string]config.SourceConfig{
		"orders": {
			Connection:      "bq",
			Identifier:      "raw",
			Tables:          []string{"orders"},
			ExpectedColumns: []string{"order_id", "status", "created_at"},
		},
	}, bqConn("myproject"))

	nodes, _ := GenerateSourceCheckNodes(cfg)

	// exists_not_empty + expected_columns
	if len(nodes) != 2 {
		t.Fatalf("expected 2 check nodes, got %d: %v", len(nodes), nodeNameList(nodes))
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
			Identifier:      "raw",
			Tables:          []string{"orders"},
			PrimaryKey:      "order_id",
			ExpectedFresh:   "30m",
			ExpectedColumns: []string{"order_id", "status"},
		},
	}, bqConn("myproject"))

	nodes, _ := GenerateSourceCheckNodes(cfg)

	// exists_not_empty + freshness + pk_not_null + pk_unique + expected_columns
	if len(nodes) != 5 {
		t.Fatalf("expected 5 check nodes, got %d: %v", len(nodes), nodeNameList(nodes))
	}

	names := nodeNames(nodes)
	for _, expected := range []string{
		"check:source:orders:orders:exists_not_empty",
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
			Identifier: "raw",
			Tables:     []string{"orders"},
		},
		"customers": {
			Connection: "bq",
			Identifier: "raw",
			Tables:     []string{"customers"},
			PrimaryKey: "customer_id",
		},
	}, bqConn("myproject"))

	nodes, deps := GenerateSourceCheckNodes(cfg)

	// orders: 1 exists_not_empty, customers: 1 exists_not_empty + pk_not_null + pk_unique = 4
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

func TestGenerateSourceCheckNodes_MultiTable(t *testing.T) {
	cfg := makeSourceCfg(map[string]config.SourceConfig{
		"mydb": {
			Connection:    "bq",
			Identifier:    "raw",
			Tables:        []string{"orders", "customers", "products"},
			ExpectedFresh: "1h",
		},
	}, bqConn("myproject"))

	nodes, _ := GenerateSourceCheckNodes(cfg)

	// 3 exists_not_empty + 1 freshness = 4
	if len(nodes) != 4 {
		t.Fatalf("expected 4 check nodes, got %d: %v", len(nodes), nodeNameList(nodes))
	}

	names := nodeNames(nodes)
	if !names["check:source:mydb:orders:exists_not_empty"] {
		t.Error("missing orders exists_not_empty")
	}
	if !names["check:source:mydb:customers:exists_not_empty"] {
		t.Error("missing customers exists_not_empty")
	}
	if !names["check:source:mydb:products:exists_not_empty"] {
		t.Error("missing products exists_not_empty")
	}
	if !names["check:source:mydb:default:freshness"] {
		t.Error("missing freshness check")
	}

	// Freshness should use first table
	for _, n := range nodes {
		if strings.Contains(n.Name, "freshness") {
			if !strings.Contains(n.InlineSQL, `source "mydb" "orders"`) {
				t.Errorf("freshness SQL should use first table: %s", n.InlineSQL)
			}
		}
	}
}

func TestGenerateSourceCheckNodes_BareDatasetWithTablesGeneratesChecks(t *testing.T) {
	cfg := makeSourceCfg(map[string]config.SourceConfig{
		"stdlocal": {
			Connection: "bq",
			Identifier: "stdlocal",
			Tables:     []string{"orders"},
		},
	}, bqConn("myproject"))

	nodes, _ := GenerateSourceCheckNodes(cfg)

	if len(nodes) != 1 {
		t.Fatalf("expected 1 check node, got %d: %v", len(nodes), nodeNameList(nodes))
	}

	if !strings.Contains(nodes[0].InlineSQL, `source "stdlocal" "orders"`) {
		t.Errorf("SQL missing source() call: %s", nodes[0].InlineSQL)
	}
}

func TestGenerateSourceCheckNodes_BareDatasetWithoutTablesSkipped(t *testing.T) {
	cfg := makeSourceCfg(map[string]config.SourceConfig{
		"stdlocal": {
			Connection: "bq",
			Identifier: "stdlocal",
		},
		"orders": {
			Connection: "bq",
			Identifier: "myproject.raw.orders",
		},
	}, bqConn("myproject"))

	nodes, _ := GenerateSourceCheckNodes(cfg)

	for _, n := range nodes {
		if strings.Contains(n.Name, "stdlocal") {
			t.Errorf("should not generate checks for bare dataset without tables, got %s", n.Name)
		}
	}

	names := nodeNames(nodes)
	if !names["check:source:orders:default:exists_not_empty"] {
		t.Error("missing exists_not_empty check for legacy orders source")
	}
}

func TestGenerateSourceCheckNodes_DepsPointToSource(t *testing.T) {
	cfg := makeSourceCfg(map[string]config.SourceConfig{
		"events": {
			Connection: "bq",
			Identifier: "raw",
			Tables:     []string{"events"},
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

func TestGenerateSourceCheckNodes_LegacyIdentifier(t *testing.T) {
	cfg := makeSourceCfg(map[string]config.SourceConfig{
		"orders": {
			Connection: "bq",
			Identifier: "myproject.raw.orders",
		},
	}, bqConn("myproject"))

	nodes, _ := GenerateSourceCheckNodes(cfg)

	if len(nodes) != 1 {
		t.Fatalf("expected 1 check node, got %d", len(nodes))
	}

	n := nodes[0]
	if !strings.Contains(n.InlineSQL, "myproject.raw.orders") {
		t.Errorf("legacy SQL missing identifier: %s", n.InlineSQL)
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
