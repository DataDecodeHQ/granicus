package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type ServerConfig struct {
	Server ServerSettings `yaml:"server"`
}

type ServerSettings struct {
	Port    int            `yaml:"port"`
	APIKeys []ServerAPIKey `yaml:"api_keys"`
}

type ServerAPIKey struct {
	Name string `yaml:"name"`
	Key  string `yaml:"key"`
}

// LoadServerConfig reads and parses a server YAML config file, defaulting the port to 8080 if unset.
func LoadServerConfig(path string) (*ServerConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading server config: %w", err)
	}

	var cfg ServerConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing server config: %w", err)
	}

	if cfg.Server.Port <= 0 {
		cfg.Server.Port = 8080
	}

	return &cfg, nil
}
