package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

type ConnectionConfig struct {
	Name       string            `yaml:"-"`
	Type       string            `yaml:"type"`
	Properties map[string]string `yaml:",inline"`
}

type CheckConfig struct {
	Name   string `yaml:"name"`
	Type   string `yaml:"type"`
	Source string `yaml:"source"`
}

type AssetConfig struct {
	Name                  string        `yaml:"name"`
	Type                  string        `yaml:"type"`
	Source                string        `yaml:"source"`
	DestinationConnection string        `yaml:"destination_connection,omitempty"`
	SourceConnection      string        `yaml:"source_connection,omitempty"`
	Checks                []CheckConfig `yaml:"checks,omitempty"`
}

type PipelineConfig struct {
	Pipeline    string                       `yaml:"pipeline"`
	MaxParallel int                          `yaml:"max_parallel"`
	Connections map[string]*ConnectionConfig `yaml:"connections,omitempty"`
	Assets      []AssetConfig                `yaml:"assets"`
}

var validTypes = map[string]bool{
	"sql":    true,
	"python": true,
	"shell":  true,
	"dlt":    true,
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

		if seen[a.Name] {
			return nil, fmt.Errorf("duplicate asset name: %q", a.Name)
		}
		seen[a.Name] = true
	}

	// Populate connection names from map keys
	for name, conn := range cfg.Connections {
		conn.Name = name
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

	return &cfg, nil
}
