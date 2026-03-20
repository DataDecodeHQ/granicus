package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// CLIConfig represents the merged CLI configuration from all layers.
type CLIConfig struct {
	Endpoint string `json:"endpoint,omitempty"`
	APIKey   string `json:"api_key,omitempty"`
	Org      string `json:"org,omitempty"`
	Pipeline string `json:"default_pipeline,omitempty"`
}

// UserConfig is the user-global config at ~/.granicus/config.json.
// Contains auth credentials — never committed.
type UserConfig struct {
	Endpoint string `json:"endpoint,omitempty"`
	APIKey   string `json:"api_key,omitempty"`
	Org      string `json:"default_org,omitempty"`
}

// ProjectConfig is the project-level config at .granicus/config.json.
// Safe to commit — no secrets.
type ProjectConfig struct {
	Org      string `json:"org,omitempty"`
	Pipeline string `json:"default_pipeline,omitempty"`
}

// UserConfigPath returns the path to ~/.granicus/config.json.
func UserConfigPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolving home directory: %w", err)
	}
	return filepath.Join(home, ".granicus", "config.json"), nil
}

// ProjectConfigPath returns the path to .granicus/config.json relative to the given root.
func ProjectConfigPath(projectRoot string) string {
	return filepath.Join(projectRoot, ".granicus", "config.json")
}

// LoadUserConfig reads the user-global config file.
func LoadUserConfig() (*UserConfig, error) {
	path, err := UserConfigPath()
	if err != nil {
		return nil, err
	}
	return loadJSON[UserConfig](path)
}

// LoadProjectConfig reads the project-level config file.
func LoadProjectConfig(projectRoot string) (*ProjectConfig, error) {
	path := ProjectConfigPath(projectRoot)
	return loadJSON[ProjectConfig](path)
}

// WriteUserConfig writes the user-global config file, creating the directory if needed.
func WriteUserConfig(cfg *UserConfig) error {
	path, err := UserConfigPath()
	if err != nil {
		return err
	}
	return writeJSON(path, cfg)
}

// ResolveCLIConfig merges configuration from all layers:
// CLI flags > env vars > project config > user config > defaults.
func ResolveCLIConfig(projectRoot string, flagEndpoint, flagAPIKey string) (*CLIConfig, error) {
	result := &CLIConfig{}

	// Layer 1: User config (lowest priority)
	if uc, err := LoadUserConfig(); err == nil && uc != nil {
		result.Endpoint = uc.Endpoint
		result.APIKey = uc.APIKey
		result.Org = uc.Org
	}

	// Layer 2: Project config
	if projectRoot != "" {
		if pc, err := LoadProjectConfig(projectRoot); err == nil && pc != nil {
			if pc.Org != "" {
				result.Org = pc.Org
			}
			if pc.Pipeline != "" {
				result.Pipeline = pc.Pipeline
			}
		}
	}

	// Layer 3: Env vars
	if v := os.Getenv("GRANICUS_API_KEY"); v != "" {
		result.APIKey = v
	}
	if v := os.Getenv("GRANICUS_ENDPOINT"); v != "" {
		result.Endpoint = v
	}

	// Layer 4: CLI flags (highest priority)
	if flagEndpoint != "" {
		result.Endpoint = flagEndpoint
	}
	if flagAPIKey != "" {
		result.APIKey = flagAPIKey
	}

	return result, nil
}

func loadJSON[T any](path string) (*T, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var v T
	if err := json.Unmarshal(data, &v); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", path, err)
	}
	return &v, nil
}

func writeJSON(path string, v any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return fmt.Errorf("creating config directory: %w", err)
	}
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0600)
}
