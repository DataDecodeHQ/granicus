package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

type AssetConfig struct {
	Name   string `yaml:"name"`
	Type   string `yaml:"type"`
	Source string `yaml:"source"`
}

type PipelineConfig struct {
	Pipeline    string        `yaml:"pipeline"`
	MaxParallel int           `yaml:"max_parallel"`
	Assets      []AssetConfig `yaml:"assets"`
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

	return &cfg, nil
}
