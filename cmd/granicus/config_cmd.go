package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

const configFileName = ".granicus-config.json"

type granulusConfig struct {
	Cloud struct {
		APIKey   string `json:"api_key,omitempty"`
		Endpoint string `json:"endpoint,omitempty"`
	} `json:"cloud"`
}

func newConfigCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Manage Granicus CLI configuration",
	}

	setCmd := &cobra.Command{
		Use:   "set <key> <value>",
		Short: "Set a config value",
		Args:  cobra.ExactArgs(2),
		RunE:  runConfigSet,
	}

	getCmd := &cobra.Command{
		Use:   "get <key>",
		Short: "Get a config value",
		Args:  cobra.ExactArgs(1),
		RunE:  runConfigGet,
	}

	showCmd := &cobra.Command{
		Use:   "show",
		Short: "Show all configuration",
		RunE:  runConfigShow,
	}

	cmd.AddCommand(setCmd, getCmd, showCmd)
	return cmd
}

func configPath() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, configFileName)
}

func loadConfig() (*granulusConfig, error) {
	cfg := &granulusConfig{}
	data, err := os.ReadFile(configPath())
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return nil, err
	}
	if err := json.Unmarshal(data, cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

func saveConfig(cfg *granulusConfig) error {
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(configPath(), data, 0600)
}

func runConfigSet(cmd *cobra.Command, args []string) error {
	key, value := args[0], args[1]

	cfg, err := loadConfig()
	if err != nil {
		return err
	}

	switch key {
	case "cloud.api_key":
		cfg.Cloud.APIKey = value
	case "cloud.endpoint":
		cfg.Cloud.Endpoint = value
	default:
		return fmt.Errorf("unknown config key: %s (valid: cloud.api_key, cloud.endpoint)", key)
	}

	if err := saveConfig(cfg); err != nil {
		return err
	}
	fmt.Printf("Set %s\n", key)
	return nil
}

func runConfigGet(cmd *cobra.Command, args []string) error {
	key := args[0]

	cfg, err := loadConfig()
	if err != nil {
		return err
	}

	switch key {
	case "cloud.api_key":
		if cfg.Cloud.APIKey != "" {
			fmt.Printf("%s...%s\n", cfg.Cloud.APIKey[:4], cfg.Cloud.APIKey[len(cfg.Cloud.APIKey)-4:])
		} else {
			fmt.Println("(not set)")
		}
	case "cloud.endpoint":
		if cfg.Cloud.Endpoint != "" {
			fmt.Println(cfg.Cloud.Endpoint)
		} else {
			fmt.Println("(not set)")
		}
	default:
		return fmt.Errorf("unknown config key: %s", key)
	}
	return nil
}

func runConfigShow(cmd *cobra.Command, args []string) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}

	mode := "local"
	if cfg.Cloud.APIKey != "" && cfg.Cloud.Endpoint != "" {
		mode = "cloud"
	}

	fmt.Printf("Mode: %s\n", mode)
	fmt.Printf("Endpoint: %s\n", cfg.Cloud.Endpoint)
	if cfg.Cloud.APIKey != "" {
		fmt.Printf("API Key: %s...%s\n", cfg.Cloud.APIKey[:4], cfg.Cloud.APIKey[len(cfg.Cloud.APIKey)-4:])
	}
	return nil
}

// IsCloudMode returns true if the CLI has cloud credentials configured.
func IsCloudMode() bool {
	// Check env vars first (set by Cloud Run or explicitly)
	if os.Getenv("GRANICUS_API_URL") != "" && os.Getenv("GRANICUS_API_KEY") != "" {
		return true
	}
	// Fall back to config file
	cfg, err := loadConfig()
	if err != nil {
		return false
	}
	return cfg.Cloud.APIKey != "" && cfg.Cloud.Endpoint != ""
}

// CloudEndpoint returns the configured engine endpoint.
func CloudEndpoint() string {
	if url := os.Getenv("GRANICUS_API_URL"); url != "" {
		return url
	}
	cfg, _ := loadConfig()
	return cfg.Cloud.Endpoint
}

// CloudAPIKey returns the configured API key.
func CloudAPIKey() string {
	if key := os.Getenv("GRANICUS_API_KEY"); key != "" {
		return key
	}
	cfg, _ := loadConfig()
	return cfg.Cloud.APIKey
}
