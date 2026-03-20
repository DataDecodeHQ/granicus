package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/DataDecodeHQ/granicus/internal/config"
)

// Mode represents the CLI execution mode.
type Mode string

const (
	ModeLocal Mode = "local"
	ModeCloud Mode = "cloud"
)

// resolveMode determines the execution mode from resolved config and flags.
// Cloud mode activates when an API key is resolvable.
// If API key is present but endpoint is missing, returns an error via panic-free path.
func resolveMode(cfg *config.CLIConfig, forceLocal bool) Mode {
	if forceLocal {
		return ModeLocal
	}
	if cfg.APIKey != "" {
		return ModeCloud
	}
	return ModeLocal
}

// validateMode checks that cloud mode has all required config.
// Returns an error if API key is set but endpoint is missing.
func validateMode(cfg *config.CLIConfig, mode Mode) error {
	if mode == ModeCloud && cfg.Endpoint == "" {
		return fmt.Errorf("cloud mode requires an endpoint; run 'granicus login' or set GRANICUS_ENDPOINT")
	}
	return nil
}

// requireLocal returns an error if the current mode is not local.
// Used by cloud-only commands to gate with a clear message.
func requireCloud(mode Mode, command string) error {
	if mode != ModeCloud {
		return fmt.Errorf("%s is a cloud-only command; run 'granicus login' to configure cloud mode", command)
	}
	return nil
}

// CloudEndpoint returns the resolved cloud endpoint.
func CloudEndpoint() string {
	cfg, _ := config.ResolveCLIConfig("", "", "")
	return cfg.Endpoint
}

// CloudAPIKey returns the resolved API key.
func CloudAPIKey() string {
	if key := os.Getenv("GRANICUS_API_KEY"); key != "" {
		return key
	}
	cfg, _ := config.ResolveCLIConfig("", "", "")
	return cfg.APIKey
}

// cloudGate wraps a cobra RunE function with requireCloud gating.
func cloudGate(fn func(cmd *cobra.Command, args []string) error) func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		cfg, _ := config.ResolveCLIConfig("", "", "")
		mode := resolveMode(cfg, false)
		if err := requireCloud(mode, cmd.Name()); err != nil {
			return err
		}
		return fn(cmd, args)
	}
}

// requireLocal returns an error if we need local mode but are in cloud mode.
func requireLocal(mode Mode, command string) error {
	if mode != ModeLocal {
		return fmt.Errorf("%s requires local mode; use --local flag to override", command)
	}
	return nil
}
