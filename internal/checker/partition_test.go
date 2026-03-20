package checker

import (
	"fmt"
	"testing"

	"github.com/DataDecodeHQ/granicus/internal/config"
)

type mockMetadataProvider struct {
	tables map[string]*TablePartitionInfo
}

func (m *mockMetadataProvider) GetPartitionInfo(project, dataset, table string) (*TablePartitionInfo, error) {
	key := fmt.Sprintf("%s.%s.%s", project, dataset, table)
	info, ok := m.tables[key]
	if !ok {
		return nil, fmt.Errorf("table not found: %s", key)
	}
	return info, nil
}

func TestValidatePartitions_Match(t *testing.T) {
	cfg := &config.PipelineConfig{
		Pipeline: "test",
		Resources: map[string]*config.ResourceConfig{
			"bq": {Type: "bigquery", Properties: map[string]string{"project": "proj", "dataset": "ds"}},
		},
		Assets: []config.AssetConfig{
			{Name: "events", DestinationResource: "bq", PartitionBy: "created_at", PartitionType: "DAY", ClusterBy: []string{"user_id"}},
		},
	}

	provider := &mockMetadataProvider{
		tables: map[string]*TablePartitionInfo{
			"proj.ds.events": {PartitionColumn: "created_at", PartitionType: "DAY", ClusterColumns: []string{"user_id"}},
		},
	}

	mismatches := ValidatePartitions(cfg, provider)
	if len(mismatches) != 0 {
		t.Errorf("expected no mismatches, got %v", mismatches)
	}
}

func TestValidatePartitions_Mismatch(t *testing.T) {
	cfg := &config.PipelineConfig{
		Pipeline: "test",
		Resources: map[string]*config.ResourceConfig{
			"bq": {Type: "bigquery", Properties: map[string]string{"project": "proj", "dataset": "ds"}},
		},
		Assets: []config.AssetConfig{
			{Name: "events", DestinationResource: "bq", PartitionBy: "created_at", PartitionType: "DAY", ClusterBy: []string{"user_id", "status"}},
		},
	}

	provider := &mockMetadataProvider{
		tables: map[string]*TablePartitionInfo{
			"proj.ds.events": {PartitionColumn: "updated_at", PartitionType: "MONTH", ClusterColumns: []string{"user_id"}},
		},
	}

	mismatches := ValidatePartitions(cfg, provider)
	if len(mismatches) != 3 {
		t.Fatalf("expected 3 mismatches, got %d: %v", len(mismatches), mismatches)
	}

	if mismatches[0].Field != "partition_by" {
		t.Errorf("expected partition_by mismatch, got %s", mismatches[0].Field)
	}
	if mismatches[1].Field != "partition_type" {
		t.Errorf("expected partition_type mismatch, got %s", mismatches[1].Field)
	}
	if mismatches[2].Field != "cluster_by" {
		t.Errorf("expected cluster_by mismatch, got %s", mismatches[2].Field)
	}
}

func TestValidatePartitions_SkipsNonPartitioned(t *testing.T) {
	cfg := &config.PipelineConfig{
		Pipeline: "test",
		Resources: map[string]*config.ResourceConfig{
			"bq": {Type: "bigquery", Properties: map[string]string{"project": "proj", "dataset": "ds"}},
		},
		Assets: []config.AssetConfig{
			{Name: "small_table", DestinationResource: "bq"},
		},
	}

	provider := &mockMetadataProvider{}
	mismatches := ValidatePartitions(cfg, provider)
	if len(mismatches) != 0 {
		t.Errorf("expected no mismatches for non-partitioned asset, got %v", mismatches)
	}
}

func TestValidatePartitions_TableNotFound(t *testing.T) {
	cfg := &config.PipelineConfig{
		Pipeline: "test",
		Resources: map[string]*config.ResourceConfig{
			"bq": {Type: "bigquery", Properties: map[string]string{"project": "proj", "dataset": "ds"}},
		},
		Assets: []config.AssetConfig{
			{Name: "missing", DestinationResource: "bq", PartitionBy: "created_at"},
		},
	}

	provider := &mockMetadataProvider{tables: map[string]*TablePartitionInfo{}}
	mismatches := ValidatePartitions(cfg, provider)
	if len(mismatches) != 0 {
		t.Errorf("expected no mismatches when table not found, got %v", mismatches)
	}
}

func TestPartitionMismatch_String(t *testing.T) {
	m := PartitionMismatch{Asset: "events", Field: "partition_by", Expect: "created_at", Actual: "updated_at"}
	s := m.String()
	if s != `events: expected partition_by="created_at", got "updated_at"` {
		t.Errorf("unexpected string: %s", s)
	}
}
