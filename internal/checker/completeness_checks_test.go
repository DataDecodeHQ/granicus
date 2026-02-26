package checker

import (
	"strings"
	"testing"

	"github.com/analytehealth/granicus/internal/config"
)

func floatPtr(f float64) *float64 { return &f }

func makeCompleteCfg(assets []config.AssetConfig) *config.PipelineConfig {
	return &config.PipelineConfig{
		Pipeline: "test",
		Assets:   assets,
	}
}

func TestGenerateCompletenessCheckNodes_NoCompleteness(t *testing.T) {
	cfg := makeCompleteCfg([]config.AssetConfig{
		{Name: "ent_patient", Type: "shell", Source: "x.sh"},
	})

	nodes, deps := GenerateCompletenessCheckNodes(cfg)

	if len(nodes) != 0 {
		t.Errorf("expected 0 nodes for asset without completeness config, got %d", len(nodes))
	}
	if len(deps) != 0 {
		t.Errorf("expected 0 deps, got %d", len(deps))
	}
}

func TestGenerateCompletenessCheckNodes_SimpleSourceVsEntity(t *testing.T) {
	tolerance := 0.01
	cfg := makeCompleteCfg([]config.AssetConfig{
		{
			Name:                  "ent_patient",
			Type:                  "sql",
			Source:                "sql/ent_patient.sql",
			DestinationConnection: "bq",
			Grain:                 "patient_id",
			Completeness: &config.CompletenessConfig{
				SourceTable: "stg_patients",
				SourcePK:    "patient_id",
				Tolerance:   &tolerance,
			},
		},
	})

	nodes, deps := GenerateCompletenessCheckNodes(cfg)

	if len(nodes) != 1 {
		t.Fatalf("expected 1 check node, got %d", len(nodes))
	}

	n := nodes[0]
	if n.Name != "check:ent_patient:default:completeness" {
		t.Errorf("unexpected name: %s", n.Name)
	}
	if n.Type != "sql_check" {
		t.Errorf("expected sql_check, got %s", n.Type)
	}
	if n.DestinationConnection != "bq" {
		t.Errorf("expected connection bq, got %s", n.DestinationConnection)
	}
	if n.SourceAsset != "ent_patient" {
		t.Errorf("expected SourceAsset ent_patient, got %s", n.SourceAsset)
	}

	sql := n.InlineSQL
	if !strings.Contains(sql, "source_pks") {
		t.Errorf("SQL missing source_pks CTE: %s", sql)
	}
	if !strings.Contains(sql, "stg_patients") {
		t.Errorf("SQL missing source_table: %s", sql)
	}
	if !strings.Contains(sql, "patient_id AS pk") {
		t.Errorf("SQL missing source_pk select: %s", sql)
	}
	if !strings.Contains(sql, "entity_pks") {
		t.Errorf("SQL missing entity_pks CTE: %s", sql)
	}
	if !strings.Contains(sql, "ent_patient") {
		t.Errorf("SQL missing asset name: %s", sql)
	}
	if !strings.Contains(sql, "COMPLETENESS_IMBALANCE") {
		t.Errorf("SQL missing COMPLETENESS_IMBALANCE: %s", sql)
	}
	if !strings.Contains(sql, "drift_ratio") {
		t.Errorf("SQL missing drift_ratio: %s", sql)
	}
	if !strings.Contains(sql, "SAFE_DIVIDE") {
		t.Errorf("SQL missing SAFE_DIVIDE: %s", sql)
	}
	if !strings.Contains(sql, "> 0.01") {
		t.Errorf("SQL missing tolerance threshold (0.01): %s", sql)
	}

	// No exclusion/addition CTEs should be present
	if strings.Contains(sql, "exclusion_") {
		t.Errorf("SQL should not have exclusion CTEs: %s", sql)
	}
	if strings.Contains(sql, "addition_") {
		t.Errorf("SQL should not have addition CTEs: %s", sql)
	}
	// expected_pks should just be source_pks with no EXCEPT or UNION
	if strings.Contains(sql, "EXCEPT DISTINCT") {
		t.Errorf("SQL should not have EXCEPT DISTINCT without exclusions: %s", sql)
	}
	if strings.Contains(sql, "UNION DISTINCT") {
		t.Errorf("SQL should not have UNION DISTINCT without additions: %s", sql)
	}

	d := deps[n.Name]
	if len(d) != 1 || d[0] != "ent_patient" {
		t.Errorf("unexpected deps: %v", d)
	}
}

func TestGenerateCompletenessCheckNodes_SingleExclusion(t *testing.T) {
	tolerance := 0.01
	cfg := makeCompleteCfg([]config.AssetConfig{
		{
			Name:                  "ent_patient",
			Type:                  "sql",
			Source:                "sql/ent_patient.sql",
			DestinationConnection: "bq",
			Grain:                 "patient_id",
			Completeness: &config.CompletenessConfig{
				SourceTable: "stg_patients",
				SourcePK:    "patient_id",
				Tolerance:   &tolerance,
				Exclusions: []config.CompletenessExclusion{
					{Table: "stg_test_patients", PK: "patient_id"},
				},
			},
		},
	})

	nodes, _ := GenerateCompletenessCheckNodes(cfg)

	if len(nodes) != 1 {
		t.Fatalf("expected 1 check node, got %d", len(nodes))
	}

	sql := nodes[0].InlineSQL
	if !strings.Contains(sql, "exclusion_1") {
		t.Errorf("SQL missing exclusion_1 CTE: %s", sql)
	}
	if !strings.Contains(sql, "stg_test_patients") {
		t.Errorf("SQL missing exclusion table: %s", sql)
	}
	if !strings.Contains(sql, "EXCEPT DISTINCT") {
		t.Errorf("SQL missing EXCEPT DISTINCT: %s", sql)
	}
	if !strings.Contains(sql, "SELECT pk FROM exclusion_1") {
		t.Errorf("SQL missing exclusion reference in expected_pks: %s", sql)
	}
	// No additions
	if strings.Contains(sql, "addition_") {
		t.Errorf("SQL should not have addition CTEs: %s", sql)
	}
	if strings.Contains(sql, "UNION DISTINCT") {
		t.Errorf("SQL should not have UNION DISTINCT without additions: %s", sql)
	}
}

func TestGenerateCompletenessCheckNodes_MultipleExclusionsAndAdditions(t *testing.T) {
	tolerance := 0.01
	cfg := makeCompleteCfg([]config.AssetConfig{
		{
			Name:                  "ent_patient",
			Type:                  "sql",
			Source:                "sql/ent_patient.sql",
			DestinationConnection: "bq",
			Grain:                 "patient_id",
			Completeness: &config.CompletenessConfig{
				SourceTable: "stg_patients",
				SourcePK:    "patient_id",
				Tolerance:   &tolerance,
				Exclusions: []config.CompletenessExclusion{
					{Table: "stg_test_patients", PK: "patient_id"},
					{Table: "stg_inactive_patients", PK: "patient_id"},
				},
				Additions: []config.CompletenessExclusion{
					{Table: "stg_migrated_patients", PK: "legacy_patient_id"},
				},
			},
		},
	})

	nodes, _ := GenerateCompletenessCheckNodes(cfg)

	if len(nodes) != 1 {
		t.Fatalf("expected 1 check node, got %d", len(nodes))
	}

	sql := nodes[0].InlineSQL
	if !strings.Contains(sql, "exclusion_1") {
		t.Errorf("SQL missing exclusion_1: %s", sql)
	}
	if !strings.Contains(sql, "exclusion_2") {
		t.Errorf("SQL missing exclusion_2: %s", sql)
	}
	if !strings.Contains(sql, "stg_test_patients") {
		t.Errorf("SQL missing first exclusion table: %s", sql)
	}
	if !strings.Contains(sql, "stg_inactive_patients") {
		t.Errorf("SQL missing second exclusion table: %s", sql)
	}
	if !strings.Contains(sql, "addition_1") {
		t.Errorf("SQL missing addition_1: %s", sql)
	}
	if !strings.Contains(sql, "stg_migrated_patients") {
		t.Errorf("SQL missing addition table: %s", sql)
	}
	if !strings.Contains(sql, "legacy_patient_id AS pk") {
		t.Errorf("SQL missing addition pk: %s", sql)
	}
	if !strings.Contains(sql, "EXCEPT DISTINCT") {
		t.Errorf("SQL missing EXCEPT DISTINCT: %s", sql)
	}
	if !strings.Contains(sql, "UNION DISTINCT") {
		t.Errorf("SQL missing UNION DISTINCT: %s", sql)
	}
	if !strings.Contains(sql, "SELECT pk FROM exclusion_1") {
		t.Errorf("SQL missing exclusion_1 reference in expected_pks: %s", sql)
	}
	if !strings.Contains(sql, "SELECT pk FROM exclusion_2") {
		t.Errorf("SQL missing exclusion_2 reference in expected_pks: %s", sql)
	}
	if !strings.Contains(sql, "SELECT pk FROM addition_1") {
		t.Errorf("SQL missing addition_1 reference in expected_pks: %s", sql)
	}
}

func TestGenerateCompletenessCheckNodes_CustomTolerance(t *testing.T) {
	tolerance := 0.05
	cfg := makeCompleteCfg([]config.AssetConfig{
		{
			Name:                  "ent_patient",
			Type:                  "sql",
			Source:                "sql/ent_patient.sql",
			DestinationConnection: "bq",
			Grain:                 "patient_id",
			Completeness: &config.CompletenessConfig{
				SourceTable: "stg_patients",
				SourcePK:    "patient_id",
				Tolerance:   &tolerance,
			},
		},
	})

	nodes, _ := GenerateCompletenessCheckNodes(cfg)

	if len(nodes) != 1 {
		t.Fatalf("expected 1 check node, got %d", len(nodes))
	}

	sql := nodes[0].InlineSQL
	if !strings.Contains(sql, "> 0.05") {
		t.Errorf("SQL missing custom tolerance (0.05): %s", sql)
	}
}

func TestGenerateCompletenessCheckNodes_ExclusionWithFilter(t *testing.T) {
	tolerance := 0.01
	cfg := makeCompleteCfg([]config.AssetConfig{
		{
			Name:                  "ent_patient",
			Type:                  "sql",
			Source:                "sql/ent_patient.sql",
			DestinationConnection: "bq",
			Grain:                 "patient_id",
			Completeness: &config.CompletenessConfig{
				SourceTable: "stg_patients",
				SourcePK:    "patient_id",
				Tolerance:   &tolerance,
				Exclusions: []config.CompletenessExclusion{
					{Table: "stg_test_patients", PK: "patient_id", Filter: "is_test = TRUE"},
					{Table: "stg_inactive_patients", PK: "patient_id"},
				},
			},
		},
	})

	nodes, _ := GenerateCompletenessCheckNodes(cfg)

	if len(nodes) != 1 {
		t.Fatalf("expected 1 check node, got %d", len(nodes))
	}

	sql := nodes[0].InlineSQL

	// First exclusion has a filter
	if !strings.Contains(sql, "WHERE is_test = TRUE") {
		t.Errorf("SQL missing filter for first exclusion: %s", sql)
	}

	// Second exclusion has no filter — verify WHERE does not appear twice erroneously
	// Count occurrences: 1 WHERE for the filtered exclusion
	whereCount := strings.Count(sql, "WHERE is_test = TRUE")
	if whereCount != 1 {
		t.Errorf("expected 1 WHERE is_test = TRUE, got %d: %s", whereCount, sql)
	}

	// Second exclusion table should exist but without its own filter
	if !strings.Contains(sql, "stg_inactive_patients") {
		t.Errorf("SQL missing second exclusion table: %s", sql)
	}
}

func TestGenerateCompletenessCheckNodes_AdditionWithFilter(t *testing.T) {
	tolerance := 0.01
	cfg := makeCompleteCfg([]config.AssetConfig{
		{
			Name:                  "ent_patient",
			Type:                  "sql",
			Source:                "sql/ent_patient.sql",
			DestinationConnection: "bq",
			Grain:                 "patient_id",
			Completeness: &config.CompletenessConfig{
				SourceTable: "stg_patients",
				SourcePK:    "patient_id",
				Tolerance:   &tolerance,
				Additions: []config.CompletenessExclusion{
					{Table: "stg_migrated_patients", PK: "legacy_id", Filter: "migrated = TRUE"},
				},
			},
		},
	})

	nodes, _ := GenerateCompletenessCheckNodes(cfg)

	if len(nodes) != 1 {
		t.Fatalf("expected 1 check node, got %d", len(nodes))
	}

	sql := nodes[0].InlineSQL
	if !strings.Contains(sql, "WHERE migrated = TRUE") {
		t.Errorf("SQL missing filter for addition: %s", sql)
	}
	if !strings.Contains(sql, "legacy_id AS pk") {
		t.Errorf("SQL missing addition pk: %s", sql)
	}
}

func TestGenerateCompletenessCheckNodes_MultipleAssets(t *testing.T) {
	tolerance := 0.01
	cfg := makeCompleteCfg([]config.AssetConfig{
		{
			Name:  "ent_patient",
			Type:  "sql",
			Source: "sql/ent_patient.sql",
			Grain: "patient_id",
			Completeness: &config.CompletenessConfig{
				SourceTable: "stg_patients",
				SourcePK:    "patient_id",
				Tolerance:   &tolerance,
			},
		},
		{
			Name:  "ent_order",
			Type:  "shell",
			Source: "x.sh",
			// No completeness
		},
		{
			Name:  "ent_provider",
			Type:  "sql",
			Source: "sql/ent_provider.sql",
			Grain: "provider_id",
			Completeness: &config.CompletenessConfig{
				SourceTable: "stg_providers",
				SourcePK:    "provider_id",
				Tolerance:   &tolerance,
			},
		},
	})

	nodes, deps := GenerateCompletenessCheckNodes(cfg)

	if len(nodes) != 2 {
		t.Fatalf("expected 2 check nodes, got %d", len(nodes))
	}

	names := nodeNames(nodes)
	if !names["check:ent_patient:default:completeness"] {
		t.Error("missing ent_patient completeness check")
	}
	if !names["check:ent_provider:default:completeness"] {
		t.Error("missing ent_provider completeness check")
	}
	if names["check:ent_order:default:completeness"] {
		t.Error("ent_order should not have a completeness check")
	}

	for _, n := range nodes {
		d := deps[n.Name]
		if len(d) != 1 {
			t.Errorf("expected 1 dep for %s, got %v", n.Name, d)
		}
	}
}

func TestGenerateCompletenessCheckNodes_GrainUsedForEntityPK(t *testing.T) {
	tolerance := 0.01
	cfg := makeCompleteCfg([]config.AssetConfig{
		{
			Name:                  "ent_patient",
			Type:                  "sql",
			Source:                "sql/ent_patient.sql",
			DestinationConnection: "bq",
			Grain:                 "patient_uuid",
			Completeness: &config.CompletenessConfig{
				SourceTable: "stg_patients",
				SourcePK:    "patient_id",
				Tolerance:   &tolerance,
			},
		},
	})

	nodes, _ := GenerateCompletenessCheckNodes(cfg)

	if len(nodes) != 1 {
		t.Fatalf("expected 1 check node, got %d", len(nodes))
	}

	sql := nodes[0].InlineSQL
	// The grain (patient_uuid) is used for entity_pks, source_pk (patient_id) for source_pks
	if !strings.Contains(sql, "patient_uuid AS pk") {
		t.Errorf("SQL missing grain (patient_uuid) as entity pk: %s", sql)
	}
	if !strings.Contains(sql, "patient_id AS pk") {
		t.Errorf("SQL missing source_pk (patient_id) in source_pks: %s", sql)
	}
}

func TestGenerateCompletenessCheckNodes_SQLStructure(t *testing.T) {
	tolerance := 0.01
	cfg := makeCompleteCfg([]config.AssetConfig{
		{
			Name:                  "ent_patient",
			Type:                  "sql",
			Source:                "sql/ent_patient.sql",
			DestinationConnection: "bq",
			Grain:                 "patient_id",
			Completeness: &config.CompletenessConfig{
				SourceTable: "stg_patients",
				SourcePK:    "patient_id",
				Tolerance:   &tolerance,
			},
		},
	})

	nodes, _ := GenerateCompletenessCheckNodes(cfg)

	if len(nodes) != 1 {
		t.Fatalf("expected 1 check node, got %d", len(nodes))
	}

	sql := nodes[0].InlineSQL
	// Verify required CTEs exist in order
	sourcePKsPos := strings.Index(sql, "source_pks")
	expectedPKsPos := strings.Index(sql, "expected_pks")
	entityPKsPos := strings.Index(sql, "entity_pks")
	statsPos := strings.Index(sql, "stats")
	selectPos := strings.Index(sql, "SELECT\n  expected_count")

	if sourcePKsPos < 0 {
		t.Error("SQL missing source_pks CTE")
	}
	if expectedPKsPos < 0 {
		t.Error("SQL missing expected_pks CTE")
	}
	if entityPKsPos < 0 {
		t.Error("SQL missing entity_pks CTE")
	}
	if statsPos < 0 {
		t.Error("SQL missing stats CTE")
	}
	if selectPos < 0 {
		t.Error("SQL missing final SELECT")
	}

	// Verify ordering
	if sourcePKsPos > expectedPKsPos {
		t.Error("source_pks should appear before expected_pks")
	}
	if expectedPKsPos > entityPKsPos {
		t.Error("expected_pks should appear before entity_pks")
	}
	if entityPKsPos > statsPos {
		t.Error("entity_pks should appear before stats")
	}
	if statsPos > selectPos {
		t.Error("stats should appear before final SELECT")
	}

	// Verify stats columns
	if !strings.Contains(sql, "expected_count") {
		t.Errorf("SQL missing expected_count: %s", sql)
	}
	if !strings.Contains(sql, "entity_count") {
		t.Errorf("SQL missing entity_count: %s", sql)
	}
	if !strings.Contains(sql, "missing_count") {
		t.Errorf("SQL missing missing_count: %s", sql)
	}
	if !strings.Contains(sql, "unexpected_count") {
		t.Errorf("SQL missing unexpected_count: %s", sql)
	}

	// Verify template placeholders
	if !strings.Contains(sql, "{{.Project}}") {
		t.Errorf("SQL missing {{.Project}} template: %s", sql)
	}
	if !strings.Contains(sql, "{{.Dataset}}") {
		t.Errorf("SQL missing {{.Dataset}} template: %s", sql)
	}
}
