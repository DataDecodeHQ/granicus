package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type ForeignKeyConfig struct {
	Column     string `yaml:"column"`
	References string `yaml:"references"`
	Nullable   bool   `yaml:"nullable,omitempty"`
}

type CompletenessExclusion struct {
	Table  string `yaml:"table"`
	PK     string `yaml:"pk"`
	Filter string `yaml:"filter,omitempty"`
}

type CompletenessConfig struct {
	SourceTable     string                  `yaml:"source_table"`
	SourcePK        string                  `yaml:"source_pk"`
	Exclusions      []CompletenessExclusion `yaml:"exclusions,omitempty"`
	Additions       []CompletenessExclusion `yaml:"additions,omitempty"`
	Tolerance       *float64                `yaml:"tolerance,omitempty"`
	KnownExclusions []string                `yaml:"known_exclusions,omitempty"`
}

type ConnectionConfig struct {
	Name       string            `yaml:"-"`
	Type       string            `yaml:"type"`
	Properties map[string]string `yaml:",inline"`
}

type CheckConfig struct {
	Name     string `yaml:"name"`
	Type     string `yaml:"type"`
	Source   string `yaml:"source"`
	Blocking bool   `yaml:"blocking,omitempty"`
	Severity string `yaml:"severity,omitempty"`
}

type StandardsConfig struct {
	Email    []string `yaml:"email,omitempty"`
	Phone    []string `yaml:"phone,omitempty"`
	Currency []string `yaml:"currency,omitempty"`
}

type ContractConfig struct {
	PrimaryKey     string              `yaml:"primary_key,omitempty"`
	NotNull        []string            `yaml:"not_null,omitempty"`
	AcceptedValues map[string][]string `yaml:"accepted_values,omitempty"`
}

type AssetConfig struct {
	Name                  string              `yaml:"name"`
	Type                  string              `yaml:"type"`
	Source                string              `yaml:"source"`
	DestinationConnection string              `yaml:"destination_connection,omitempty"`
	SourceConnection      string              `yaml:"source_connection,omitempty"`
	Checks                []CheckConfig       `yaml:"checks,omitempty"`
	Pool                  string              `yaml:"pool,omitempty"`
	Layer                 string              `yaml:"layer,omitempty"`
	Grain                 string              `yaml:"grain,omitempty"`
	DefaultChecks         *bool               `yaml:"default_checks,omitempty"`
	DefaultChecksBlocking bool                `yaml:"default_checks_blocking,omitempty"`
	PartitionBy           string              `yaml:"partition_by,omitempty"`
	PartitionType         string              `yaml:"partition_type,omitempty"`
	ClusterBy             []string            `yaml:"cluster_by,omitempty"`
	ForeignKeys           []ForeignKeyConfig  `yaml:"foreign_keys,omitempty"`
	Upstream              []string            `yaml:"upstream,omitempty"`
	PrimaryUpstream       string              `yaml:"primary_upstream,omitempty"`
	MinRetentionRatio     *float64            `yaml:"min_retention_ratio,omitempty"`
	FanOutCheck           *bool               `yaml:"fan_out_check,omitempty"`
	Completeness          *CompletenessConfig `yaml:"completeness,omitempty"`
	Standards             *StandardsConfig    `yaml:"standards,omitempty"`
	StandardsBlocking     bool                `yaml:"standards_blocking,omitempty"`
	Timeout               string              `yaml:"timeout,omitempty"`
	DependsOn             []string            `yaml:"depends_on,omitempty"`
	Retry                 *RetryConfig        `yaml:"retry,omitempty"`
	SchemaCheck           string              `yaml:"schema_check,omitempty"`
	Contract              *ContractConfig     `yaml:"contract,omitempty"`
}

// RetryConfig configures per-asset retry behaviour.
type RetryConfig struct {
	MaxAttempts     int      `yaml:"max_attempts"`
	BackoffBase     string   `yaml:"backoff_base"`
	RetryableErrors []string `yaml:"retryable_errors"`
}

// validErrorCategories are the error taxonomy values accepted in retryable_errors.
var validErrorCategories = map[string]bool{
	"rate_limit": true,
	"quota":      true,
	"network":    true,
	"timeout":    true,
	"server":     true,
}

var defaultRetryableErrors = []string{"rate_limit", "quota", "network"}

var validPartitionTypes = map[string]bool{
	"":      true,
	"DAY":   true,
	"HOUR":  true,
	"MONTH": true,
	"YEAR":  true,
}

var validSchemaCheckValues = map[string]bool{
	"":       true,
	"warn":   true,
	"error":  true,
	"ignore": true,
}

var validLayers = map[string]bool{
	"":             true,
	"source":       true,
	"staging":      true,
	"intermediate": true,
	"analytics":    true,
	"entity":       true,
	"report":       true,
	"publish":      true,
}

var validSeverities = map[string]bool{
	"info":     true,
	"warning":  true,
	"error":    true,
	"critical": true,
}

type SourceConfig struct {
	Connection      string   `yaml:"connection"`
	Identifier      string   `yaml:"identifier"`
	Tables          []string `yaml:"tables,omitempty"`
	PrimaryKey      string   `yaml:"primary_key,omitempty"`
	ExpectedFresh   string   `yaml:"expected_freshness,omitempty"`
	ExpectedColumns []string `yaml:"expected_columns,omitempty"`
}

type PoolConfig struct {
	Slots      int    `yaml:"slots"`
	Timeout    string `yaml:"timeout,omitempty"`
	DefaultFor string `yaml:"default_for,omitempty"`
}

type PipelineConfig struct {
	Pipeline     string                       `yaml:"pipeline"`
	Schedule     string                       `yaml:"schedule,omitempty"`
	MaxParallel  int                          `yaml:"max_parallel"`
	Connections  map[string]*ConnectionConfig `yaml:"connections,omitempty"`
	Datasets     map[string]string            `yaml:"datasets,omitempty"`
	Sources      map[string]SourceConfig      `yaml:"sources,omitempty"`
	Pools        map[string]PoolConfig        `yaml:"pools,omitempty"`
	Assets       []AssetConfig                `yaml:"assets"`
	FunctionsDir string                       `yaml:"functions_dir,omitempty"`
	Prefix       string                       `yaml:"-"`
}

func (cfg *PipelineConfig) DatasetForAsset(asset AssetConfig, defaultDataset string) string {
	if asset.DestinationConnection != "" {
		if conn, ok := cfg.Connections[asset.DestinationConnection]; ok {
			if ds := conn.Properties["dataset"]; ds != "" {
				return ds
			}
		}
	}
	if asset.Layer != "" && cfg.Datasets != nil {
		if ds, ok := cfg.Datasets[asset.Layer]; ok {
			return ds
		}
	}
	return defaultDataset
}

func (cfg *PipelineConfig) OutputDatasets() []string {
	seen := make(map[string]bool)
	for _, asset := range cfg.Assets {
		connName := asset.DestinationConnection
		if connName == "" {
			continue
		}
		conn, ok := cfg.Connections[connName]
		if !ok {
			continue
		}
		defaultDS := conn.Properties["dataset"]
		ds := cfg.DatasetForAsset(asset, defaultDS)
		if ds != "" && !seen[ds] {
			seen[ds] = true
		}
	}
	result := make([]string, 0, len(seen))
	for ds := range seen {
		result = append(result, ds)
	}
	return result
}

var connectionRequirements = map[string][]string{
	"bigquery":   {"project", "dataset"},
	"postgres":   {"host", "database"},
	"mysql":      {"host", "database"},
	"snowflake":  {"account", "database"},
}

var validTypes = map[string]bool{
	"sql":    true,
	"python": true,
	"shell":  true,
	"dlt":    true,
}

// validateAndApplyRetryDefaults validates the retry block and fills in defaults.
// If no retry block is set, a default policy is applied.
func validateAndApplyRetryDefaults(a *AssetConfig) error {
	if a.Retry == nil {
		a.Retry = &RetryConfig{}
	}
	r := a.Retry

	if r.MaxAttempts == 0 {
		r.MaxAttempts = 3
	} else if r.MaxAttempts < 1 {
		return fmt.Errorf("retry.max_attempts must be >= 1")
	}

	if r.BackoffBase == "" {
		r.BackoffBase = "10s"
	} else {
		if _, err := time.ParseDuration(r.BackoffBase); err != nil {
			return fmt.Errorf("retry.backoff_base %q is not a valid duration: %w", r.BackoffBase, err)
		}
	}

	if len(r.RetryableErrors) == 0 {
		r.RetryableErrors = append([]string(nil), defaultRetryableErrors...)
	} else {
		for _, cat := range r.RetryableErrors {
			if !validErrorCategories[cat] {
				return fmt.Errorf("retry.retryable_errors contains unknown category %q (valid: rate_limit, quota, network, timeout, server)", cat)
			}
		}
	}

	return nil
}

func validateContract(assetName string, c *ContractConfig) error {
	for i, col := range c.NotNull {
		if col == "" {
			return fmt.Errorf("asset %q: contract.not_null[%d]: column name must not be empty", assetName, i)
		}
	}
	for col, vals := range c.AcceptedValues {
		if col == "" {
			return fmt.Errorf("asset %q: contract.accepted_values: column name must not be empty", assetName)
		}
		if len(vals) == 0 {
			return fmt.Errorf("asset %q: contract.accepted_values[%q]: must have at least one accepted value", assetName, col)
		}
	}
	return nil
}

func LoadConfig(path string) (*PipelineConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config: %w", err)
	}

	var cfg PipelineConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}

	if cfg.Pipeline == "" {
		return nil, fmt.Errorf("pipeline name is required")
	}

	if len(cfg.Assets) == 0 {
		return nil, fmt.Errorf("at least one asset is required")
	}

	if cfg.MaxParallel <= 0 {
		cfg.MaxParallel = 10
	}

	seen := make(map[string]bool)
	for i := range cfg.Assets {
		a := &cfg.Assets[i]

		if a.Source == "" {
			return nil, fmt.Errorf("asset at index %d: source is required", i)
		}

		if a.Name == "" {
			base := filepath.Base(a.Source)
			a.Name = strings.TrimSuffix(base, filepath.Ext(base))
		}

		if !validTypes[a.Type] {
			return nil, fmt.Errorf("asset %q: invalid type %q (must be sql, python, shell, or dlt)", a.Name, a.Type)
		}

		if !validLayers[a.Layer] {
			return nil, fmt.Errorf("asset %q: invalid layer %q (must be staging, intermediate, entity, report, or publish)", a.Name, a.Layer)
		}

		if !validPartitionTypes[a.PartitionType] {
			return nil, fmt.Errorf("asset %q: invalid partition_type %q (must be DAY, HOUR, MONTH, or YEAR)", a.Name, a.PartitionType)
		}

		if a.PartitionType != "" && a.PartitionBy == "" {
			return nil, fmt.Errorf("asset %q: partition_type requires partition_by", a.Name)
		}

		if !validSchemaCheckValues[a.SchemaCheck] {
			return nil, fmt.Errorf("asset %q: invalid schema_check %q (must be warn, error, or ignore)", a.Name, a.SchemaCheck)
		}

		if a.Timeout != "" {
			if _, err := time.ParseDuration(a.Timeout); err != nil {
				return nil, fmt.Errorf("asset %q: invalid timeout %q: %w", a.Name, a.Timeout, err)
			}
		}

		if err := validateAndApplyRetryDefaults(a); err != nil {
			return nil, fmt.Errorf("asset %q: %w", a.Name, err)
		}

		if seen[a.Name] {
			return nil, fmt.Errorf("duplicate asset name: %q", a.Name)
		}
		seen[a.Name] = true
	}

	// Populate connection names from map keys
	for name, conn := range cfg.Connections {
		conn.Name = name
	}

	// Validate connection properties
	if err := ValidateConnections(&cfg); err != nil {
		return nil, err
	}

	// Validate connection references
	for _, a := range cfg.Assets {
		if a.DestinationConnection != "" {
			if _, ok := cfg.Connections[a.DestinationConnection]; !ok {
				return nil, fmt.Errorf("asset %q references non-existent connection %q", a.Name, a.DestinationConnection)
			}
		}
		if a.SourceConnection != "" {
			if _, ok := cfg.Connections[a.SourceConnection]; !ok {
				return nil, fmt.Errorf("asset %q references non-existent connection %q", a.Name, a.SourceConnection)
			}
		}
		if a.Type == "sql" && a.DestinationConnection == "" {
			return nil, fmt.Errorf("sql asset %q must have destination_connection", a.Name)
		}
	}

	// Validate sources
	for name, src := range cfg.Sources {
		if src.Identifier == "" {
			return nil, fmt.Errorf("source %q: identifier is required", name)
		}
		if src.Connection != "" {
			if _, ok := cfg.Connections[src.Connection]; !ok {
				return nil, fmt.Errorf("source %q: references non-existent connection %q", name, src.Connection)
			}
		}
		if src.ExpectedFresh != "" {
			if _, err := time.ParseDuration(src.ExpectedFresh); err != nil {
				return nil, fmt.Errorf("source %q: invalid expected_freshness %q: %w", name, src.ExpectedFresh, err)
			}
		}
		// Check no collision with asset names
		for _, a := range cfg.Assets {
			if a.Name == name {
				return nil, fmt.Errorf("source %q: name collides with asset %q", name, a.Name)
			}
		}
	}

	// Validate asset structural check fields and apply defaults
	for i := range cfg.Assets {
		a := &cfg.Assets[i]
		for j := range a.Checks {
			check := &a.Checks[j]
			if check.Severity == "" {
				check.Severity = "error"
			} else if !validSeverities[check.Severity] {
				return nil, fmt.Errorf("asset %q: check %d: invalid severity %q (must be info, warning, error, or critical)", a.Name, j, check.Severity)
			}
		}
		for j, fk := range a.ForeignKeys {
			if fk.Column == "" {
				return nil, fmt.Errorf("asset %q: foreign_keys[%d]: column is required", a.Name, j)
			}
			if fk.References == "" {
				return nil, fmt.Errorf("asset %q: foreign_keys[%d]: references is required", a.Name, j)
			}
			if !strings.Contains(fk.References, ".") {
				return nil, fmt.Errorf("asset %q: foreign_keys[%d]: references must be in table.column format, got %q", a.Name, j, fk.References)
			}
		}
		if a.Completeness != nil {
			if a.Completeness.SourceTable == "" {
				return nil, fmt.Errorf("asset %q: completeness.source_table is required", a.Name)
			}
			if a.Completeness.SourcePK == "" {
				return nil, fmt.Errorf("asset %q: completeness.source_pk is required", a.Name)
			}
			if a.Completeness.Tolerance == nil {
				defaultTolerance := 0.01
				a.Completeness.Tolerance = &defaultTolerance
			}
		}
		if a.MinRetentionRatio == nil {
			defaultRatio := 0.5
			a.MinRetentionRatio = &defaultRatio
		}
		if a.Contract != nil {
			if err := validateContract(a.Name, a.Contract); err != nil {
				return nil, err
			}
		}
	}

	// Validate pools
	for name, pc := range cfg.Pools {
		if pc.Slots <= 0 {
			return nil, fmt.Errorf("pool %q: slots must be > 0", name)
		}
		if pc.Timeout != "" {
			if _, err := time.ParseDuration(pc.Timeout); err != nil {
				return nil, fmt.Errorf("pool %q: invalid timeout %q: %w", name, pc.Timeout, err)
			}
		}
	}

	// Validate pool references on assets
	for _, a := range cfg.Assets {
		if a.Pool != "" && a.Pool != "none" {
			if _, ok := cfg.Pools[a.Pool]; !ok {
				return nil, fmt.Errorf("asset %q references non-existent pool %q", a.Name, a.Pool)
			}
		}
	}

	return &cfg, nil
}

func ResolveAssetPool(asset AssetConfig, pools map[string]PoolConfig, connections map[string]*ConnectionConfig) string {
	if asset.Pool == "none" {
		return ""
	}
	if asset.Pool != "" {
		return asset.Pool
	}
	// Auto-assign based on default_for matching connection type
	connName := asset.DestinationConnection
	if connName == "" {
		return ""
	}
	conn, ok := connections[connName]
	if !ok {
		return ""
	}
	for poolName, pc := range pools {
		if pc.DefaultFor == conn.Type {
			return poolName
		}
	}
	return ""
}

func ValidateConnections(cfg *PipelineConfig) error {
	for name, conn := range cfg.Connections {
		required, known := connectionRequirements[conn.Type]
		if !known {
			continue
		}
		for _, prop := range required {
			if conn.Properties[prop] == "" {
				return fmt.Errorf("connection %q (type %s): missing required property %q", name, conn.Type, prop)
			}
		}
	}
	return nil
}
