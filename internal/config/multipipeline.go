package config

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

type MultiPipelineConfig struct {
	Pipelines          []PipelineRef       `yaml:"pipelines"`
	CrossDependencies  []CrossDependency   `yaml:"cross_dependencies,omitempty"`
}

type PipelineRef struct {
	Config string `yaml:"config"`
}

type CrossDependency struct {
	Upstream   string `yaml:"upstream"`
	Downstream string `yaml:"downstream"`
	Type       string `yaml:"type"` // "blocks" or "freshness"
}

// LoadMultiPipelineConfig reads and validates a multi-pipeline YAML config that references individual pipeline configs.
func LoadMultiPipelineConfig(path string) (*MultiPipelineConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading multi-pipeline config: %w", err)
	}

	var cfg MultiPipelineConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing multi-pipeline config: %w", err)
	}

	if len(cfg.Pipelines) == 0 {
		return nil, fmt.Errorf("at least one pipeline is required")
	}

	return &cfg, nil
}

// LoadAllPipelines loads a multi-pipeline config and returns the fully parsed PipelineConfig for each referenced pipeline.
func LoadAllPipelines(multiCfgPath string) ([]*PipelineConfig, error) {
	multiCfg, err := LoadMultiPipelineConfig(multiCfgPath)
	if err != nil {
		return nil, err
	}

	baseDir := filepath.Dir(multiCfgPath)
	var pipelines []*PipelineConfig

	for _, ref := range multiCfg.Pipelines {
		cfgPath := filepath.Join(baseDir, ref.Config)
		cfg, err := LoadConfig(cfgPath)
		if err != nil {
			return nil, fmt.Errorf("loading pipeline %s: %w", ref.Config, err)
		}
		pipelines = append(pipelines, cfg)
	}

	return pipelines, nil
}
