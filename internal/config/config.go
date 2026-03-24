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

type ResourceConfig struct {
	Name        string            `yaml:"-"`
	Type        string            `yaml:"type"`
	Credentials string            `yaml:"credentials"`
	Properties  map[string]string `yaml:",inline"`
}

type CheckConfig struct {
	Name      string   `yaml:"name"`
	Type      string   `yaml:"type"`
	Source    string   `yaml:"source"`
	Blocking  bool     `yaml:"blocking,omitempty"`
	Severity  string   `yaml:"severity,omitempty"`
	DependsOn []string `yaml:"depends_on,omitempty"`
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
	DestinationResource string              `yaml:"destination_resource,omitempty"`
	SourceResource      string              `yaml:"source_resource,omitempty"`
	Checks                []CheckConfig       `yaml:"checks,omitempty"`
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
	Runner                string              `yaml:"runner,omitempty"`
	RunnerConfig          map[string]string   `yaml:"runner_config,omitempty"`
	Timeout               string              `yaml:"timeout,omitempty"`
	DependsOn             []string            `yaml:"depends_on,omitempty"`
	Retry                 *RetryConfig        `yaml:"retry,omitempty"`
	SchemaCheck           string              `yaml:"schema_check,omitempty"`
	Contract              *ContractConfig     `yaml:"contract,omitempty"`
	PollInterval          string              `yaml:"poll_interval,omitempty"`
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
	"config":       true,
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

// AlertSeverityConfig defines webhook routing for a specific severity level.
type AlertSeverityConfig struct {
	URL      string `yaml:"url"`
	Template string `yaml:"template,omitempty"`
}

// AlertRoutingConfig configures per-severity alert routing.
// Falls back to Default when no severity-specific config is set.
type AlertRoutingConfig struct {
	Critical *AlertSeverityConfig `yaml:"critical,omitempty"`
	Warning  *AlertSeverityConfig `yaml:"warning,omitempty"`
	Default  *AlertSeverityConfig `yaml:"default,omitempty"`
}

// Resolve returns the AlertSeverityConfig for the given severity, falling back to Default.
func (r *AlertRoutingConfig) Resolve(severity string) *AlertSeverityConfig {
	switch severity {
	case "critical":
		if r.Critical != nil {
			return r.Critical
		}
	case "warning":
		if r.Warning != nil {
			return r.Warning
		}
	}
	return r.Default
}

type SourceConfig struct {
	Resource        string   `yaml:"resource"`
	Identifier      string   `yaml:"identifier"`
	Tables          []string `yaml:"tables,omitempty"`
	PrimaryKey      string   `yaml:"primary_key,omitempty"`
	ExpectedFresh   string   `yaml:"expected_freshness,omitempty"`
	ExpectedColumns []string `yaml:"expected_columns,omitempty"`
}

type PipelineConfig struct {
	Pipeline     string                       `yaml:"pipeline"`
	Schedule     string                       `yaml:"schedule,omitempty"`
	Resources    map[string]*ResourceConfig `yaml:"resources,omitempty"`
	Datasets     map[string]string            `yaml:"datasets,omitempty"`
	Sources      map[string]SourceConfig      `yaml:"sources,omitempty"`
	Assets       []AssetConfig                `yaml:"assets"`
	FunctionsDir string                       `yaml:"functions_dir,omitempty"`
	Alerts       *AlertRoutingConfig          `yaml:"alerts,omitempty"`
	Prefix       string                       `yaml:"-"`
	ConfigDir    string                       `yaml:"-"` // directory containing this config file
}

// DatasetForAsset returns the output dataset for an asset, resolving from resource, layer mapping, or the provided default.
func (cfg *PipelineConfig) DatasetForAsset(asset AssetConfig, defaultDataset string) string {
	if asset.DestinationResource != "" {
		if conn, ok := cfg.Resources[asset.DestinationResource]; ok {
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

// OutputDatasets returns the deduplicated list of BigQuery datasets written to by this pipeline's assets.
func (cfg *PipelineConfig) OutputDatasets() []string {
	seen := make(map[string]bool)
	for _, asset := range cfg.Assets {
		connName := asset.DestinationResource
		if connName == "" {
			continue
		}
		conn, ok := cfg.Resources[connName]
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

var resourceRequirements = map[string][]string{
	"bigquery":   {"project", "dataset"},
	"gcs":        {"bucket"},
	"postgres":   {"host", "database"},
	"mysql":      {"host", "database"},
	"snowflake":  {"account", "database"},
}

var validTypes = map[string]bool{
	"sql":        true,
	"python":     true,
	"shell":      true,
	"dlt":        true,
	"gcs_export": true,
	"gcs_ingest": true,
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

func validateAlertRouting(r *AlertRoutingConfig) error {
	severities := []struct {
		name string
		cfg  *AlertSeverityConfig
	}{
		{"critical", r.Critical},
		{"warning", r.Warning},
		{"default", r.Default},
	}
	for _, s := range severities {
		if s.cfg != nil && s.cfg.URL == "" {
			return fmt.Errorf("alerts.%s: url is required when severity block is set", s.name)
		}
	}
	return nil
}

func validateAssetFields(cfg *PipelineConfig) error {
	seen := make(map[string]bool)
	for i := range cfg.Assets {
		a := &cfg.Assets[i]

		if a.Source == "" {
			return fmt.Errorf("asset at index %d: source is required", i)
		}

		if a.Name == "" {
			base := filepath.Base(a.Source)
			a.Name = strings.TrimSuffix(base, filepath.Ext(base))
		}

		if !validTypes[a.Type] {
			return fmt.Errorf("asset %q: invalid type %q (must be sql, python, shell, or dlt)", a.Name, a.Type)
		}

		if !validLayers[a.Layer] {
			return fmt.Errorf("asset %q: invalid layer %q (must be config, staging, intermediate, entity, report, or publish)", a.Name, a.Layer)
		}

		if !validPartitionTypes[a.PartitionType] {
			return fmt.Errorf("asset %q: invalid partition_type %q (must be DAY, HOUR, MONTH, or YEAR)", a.Name, a.PartitionType)
		}

		if a.PartitionType != "" && a.PartitionBy == "" {
			return fmt.Errorf("asset %q: partition_type requires partition_by", a.Name)
		}

		if !validSchemaCheckValues[a.SchemaCheck] {
			return fmt.Errorf("asset %q: invalid schema_check %q (must be warn, error, or ignore)", a.Name, a.SchemaCheck)
		}

		if a.Timeout != "" {
			if _, err := time.ParseDuration(a.Timeout); err != nil {
				return fmt.Errorf("asset %q: invalid timeout %q: %w", a.Name, a.Timeout, err)
			}
		}

		if a.Runner == "" {
			a.Runner = "local"
		}

		if err := validateAndApplyRetryDefaults(a); err != nil {
			return fmt.Errorf("asset %q: %w", a.Name, err)
		}

		if seen[a.Name] {
			return fmt.Errorf("duplicate asset name: %q", a.Name)
		}
		seen[a.Name] = true
	}
	return nil
}

func validateResourceRefs(cfg *PipelineConfig) error {
	for _, a := range cfg.Assets {
		if a.DestinationResource != "" {
			if _, ok := cfg.Resources[a.DestinationResource]; !ok {
				return fmt.Errorf("asset %q references non-existent resource %q", a.Name, a.DestinationResource)
			}
		}
		if a.SourceResource != "" {
			if _, ok := cfg.Resources[a.SourceResource]; !ok {
				return fmt.Errorf("asset %q references non-existent resource %q", a.Name, a.SourceResource)
			}
		}
		if a.Type == "sql" && a.DestinationResource == "" {
			return fmt.Errorf("sql asset %q must have destination_resource", a.Name)
		}
		if (a.Type == "gcs_export" || a.Type == "gcs_ingest") && a.SourceResource == "" {
			return fmt.Errorf("%s asset %q must have source_resource", a.Type, a.Name)
		}
		if a.PollInterval != "" && a.Type != "gcs_ingest" {
			return fmt.Errorf("asset %q: poll_interval is only valid for gcs_ingest assets", a.Name)
		}
		if a.Type == "gcs_ingest" {
			if srcConn, ok := cfg.Resources[a.SourceResource]; ok && srcConn.Type != "gcs" {
				return fmt.Errorf("gcs_ingest asset %q: source_resource must be type gcs, got %q", a.Name, srcConn.Type)
			}
			if a.DestinationResource != "" {
				if destConn, ok := cfg.Resources[a.DestinationResource]; ok && destConn.Type != "bigquery" {
					return fmt.Errorf("gcs_ingest asset %q: destination_resource must be type bigquery, got %q", a.Name, destConn.Type)
				}
			}
		}
	}
	return nil
}

func validateSourceDefs(cfg *PipelineConfig) error {
	for name, src := range cfg.Sources {
		if src.Identifier == "" && src.Resource == "" {
			return fmt.Errorf("source %q: identifier is required when no resource is set", name)
		}
		if src.Resource != "" {
			if _, ok := cfg.Resources[src.Resource]; !ok {
				return fmt.Errorf("source %q: references non-existent resource %q", name, src.Resource)
			}
		}
		if src.ExpectedFresh != "" {
			if _, err := time.ParseDuration(src.ExpectedFresh); err != nil {
				return fmt.Errorf("source %q: invalid expected_freshness %q: %w", name, src.ExpectedFresh, err)
			}
		}
		// Check no collision with asset names
		for _, a := range cfg.Assets {
			if a.Name == name {
				return fmt.Errorf("source %q: name collides with asset %q", name, a.Name)
			}
		}
	}
	return nil
}

func validateCheckFields(cfg *PipelineConfig) error {
	for i := range cfg.Assets {
		a := &cfg.Assets[i]
		for j := range a.Checks {
			check := &a.Checks[j]
			if check.Severity == "" {
				check.Severity = "error"
			} else if !validSeverities[check.Severity] {
				return fmt.Errorf("asset %q: check %d: invalid severity %q (must be info, warning, error, or critical)", a.Name, j, check.Severity)
			}
		}
		for j, fk := range a.ForeignKeys {
			if fk.Column == "" {
				return fmt.Errorf("asset %q: foreign_keys[%d]: column is required", a.Name, j)
			}
			if fk.References == "" {
				return fmt.Errorf("asset %q: foreign_keys[%d]: references is required", a.Name, j)
			}
			if !strings.Contains(fk.References, ".") {
				return fmt.Errorf("asset %q: foreign_keys[%d]: references must be in table.column format, got %q", a.Name, j, fk.References)
			}
		}
		if a.Completeness != nil {
			if a.Completeness.SourceTable == "" {
				return fmt.Errorf("asset %q: completeness.source_table is required", a.Name)
			}
			if a.Completeness.SourcePK == "" {
				return fmt.Errorf("asset %q: completeness.source_pk is required", a.Name)
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
				return err
			}
		}
	}
	return nil
}


// LoadConfig reads a pipeline YAML file, validates all fields, and returns the parsed configuration.
func LoadConfig(path string) (*PipelineConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config: %w", err)
	}

	var cfg PipelineConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}

	cfg.ConfigDir = filepath.Dir(path)

	if cfg.Pipeline == "" {
		return nil, fmt.Errorf("pipeline name is required")
	}

	if len(cfg.Assets) == 0 {
		return nil, fmt.Errorf("at least one asset is required")
	}

	if err := validateAssetFields(&cfg); err != nil {
		return nil, err
	}

	// Populate resource names from map keys
	for name, conn := range cfg.Resources {
		conn.Name = name
	}

	// Validate resource properties
	if err := ValidateResources(&cfg); err != nil {
		return nil, err
	}

	if err := validateResourceRefs(&cfg); err != nil {
		return nil, err
	}

	if err := validateSourceDefs(&cfg); err != nil {
		return nil, err
	}

	if err := validateCheckFields(&cfg); err != nil {
		return nil, err
	}

	// Validate alerts routing config
	if cfg.Alerts != nil {
		if err := validateAlertRouting(cfg.Alerts); err != nil {
			return nil, err
		}
	}

	return &cfg, nil
}


// ValidateResources checks that all resources have the required properties for their type.
func ValidateResources(cfg *PipelineConfig) error {
	for name, conn := range cfg.Resources {
		required, known := resourceRequirements[conn.Type]
		if !known {
			continue
		}
		for _, prop := range required {
			if conn.Properties[prop] == "" {
				return fmt.Errorf("resource %q (type %s): missing required property %q", name, conn.Type, prop)
			}
		}
	}
	return nil
}
