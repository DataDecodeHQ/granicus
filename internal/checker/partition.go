package checker

import (
	"fmt"
	"strings"

	"github.com/DataDecodeHQ/granicus/internal/config"
)

type TablePartitionInfo struct {
	PartitionColumn string
	PartitionType   string // DAY, HOUR, MONTH, YEAR, or "" if not partitioned
	ClusterColumns  []string
}

type TableMetadataProvider interface {
	GetPartitionInfo(project, dataset, table string) (*TablePartitionInfo, error)
}

type PartitionMismatch struct {
	Asset   string
	Field   string
	Expect  string
	Actual  string
}

// String returns a human-readable description of the partition mismatch.
func (m PartitionMismatch) String() string {
	return fmt.Sprintf("%s: expected %s=%q, got %q", m.Asset, m.Field, m.Expect, m.Actual)
}

// ValidatePartitions compares configured partition/cluster settings against actual BigQuery table metadata.
func ValidatePartitions(cfg *config.PipelineConfig, provider TableMetadataProvider) []PartitionMismatch {
	var mismatches []PartitionMismatch

	conn := cfg.Connections[cfg.Assets[0].DestinationConnection]
	if conn == nil {
		return nil
	}
	project := conn.Properties["project"]
	dataset := conn.Properties["dataset"]
	if project == "" || dataset == "" {
		return nil
	}

	for _, asset := range cfg.Assets {
		if asset.PartitionBy == "" && len(asset.ClusterBy) == 0 {
			continue
		}

		destConn := asset.DestinationConnection
		if destConn == "" {
			continue
		}
		c := cfg.Connections[destConn]
		if c == nil {
			continue
		}
		p := c.Properties["project"]
		d := c.Properties["dataset"]
		if p != "" {
			project = p
		}
		if d != "" {
			dataset = d
		}

		tableName := asset.Name
		if cfg.Prefix != "" {
			tableName = cfg.Prefix + tableName
		}

		info, err := provider.GetPartitionInfo(project, dataset, tableName)
		if err != nil {
			continue
		}

		if asset.PartitionBy != "" {
			if info.PartitionColumn != asset.PartitionBy {
				mismatches = append(mismatches, PartitionMismatch{
					Asset:  asset.Name,
					Field:  "partition_by",
					Expect: asset.PartitionBy,
					Actual: info.PartitionColumn,
				})
			}
		}

		if asset.PartitionType != "" {
			if !strings.EqualFold(info.PartitionType, asset.PartitionType) {
				mismatches = append(mismatches, PartitionMismatch{
					Asset:  asset.Name,
					Field:  "partition_type",
					Expect: asset.PartitionType,
					Actual: info.PartitionType,
				})
			}
		}

		if len(asset.ClusterBy) > 0 {
			expected := strings.Join(asset.ClusterBy, ",")
			actual := strings.Join(info.ClusterColumns, ",")
			if expected != actual {
				mismatches = append(mismatches, PartitionMismatch{
					Asset:  asset.Name,
					Field:  "cluster_by",
					Expect: expected,
					Actual: actual,
				})
			}
		}
	}

	return mismatches
}
