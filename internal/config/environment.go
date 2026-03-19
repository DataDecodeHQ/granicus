package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type EnvironmentOverride struct {
	Prefix      string                       `yaml:"prefix"`
	Connections map[string]*ConnectionConfig `yaml:"connections,omitempty"`
}

type EnvironmentConfig struct {
	Environments map[string]*EnvironmentOverride `yaml:"environments"`
}

// LoadEnvironmentConfig reads and parses an environment override YAML file.
func LoadEnvironmentConfig(path string) (*EnvironmentConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading env config: %w", err)
	}

	var cfg EnvironmentConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing env config: %w", err)
	}

	// Populate connection names
	for _, env := range cfg.Environments {
		for name, conn := range env.Connections {
			conn.Name = name
		}
	}

	return &cfg, nil
}

// MergeEnvironment applies environment-specific connection overrides and prefix to a copy of the base pipeline config.
func MergeEnvironment(base *PipelineConfig, envCfg *EnvironmentConfig, envName string) (*PipelineConfig, error) {
	env, ok := envCfg.Environments[envName]
	if !ok {
		return nil, fmt.Errorf("environment %q not found in config", envName)
	}

	// Deep copy base config
	merged := *base
	merged.Assets = make([]AssetConfig, len(base.Assets))
	copy(merged.Assets, base.Assets)

	// Copy connections map
	merged.Connections = make(map[string]*ConnectionConfig)
	for k, v := range base.Connections {
		cp := *v
		cp.Properties = make(map[string]string)
		for pk, pv := range v.Properties {
			cp.Properties[pk] = pv
		}
		merged.Connections[k] = &cp
	}

	// Override connection properties from environment
	for name, envConn := range env.Connections {
		if existing, ok := merged.Connections[name]; ok {
			for k, v := range envConn.Properties {
				existing.Properties[k] = v
			}
			if envConn.Type != "" {
				existing.Type = envConn.Type
			}
		} else {
			cp := *envConn
			cp.Name = name
			cp.Properties = make(map[string]string)
			for k, v := range envConn.Properties {
				cp.Properties[k] = v
			}
			merged.Connections[name] = &cp
		}
	}

	// Store prefix for template access
	merged.Prefix = env.Prefix

	return &merged, nil
}

// StateDBPath returns the filesystem path to the state database for the given environment.
func StateDBPath(projectRoot, envName string) string {
	if envName == "" {
		envName = "dev"
	}
	return projectRoot + "/.granicus/" + envName + "-state.db"
}
