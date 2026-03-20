package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/DataDecodeHQ/granicus/internal/config"
	"github.com/spf13/cobra"
)

func newConfigCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Manage Granicus CLI configuration",
	}

	showCmd := &cobra.Command{
		Use:   "show",
		Short: "Show resolved configuration from all layers",
		RunE:  runConfigShow,
	}
	addJSONFlag(showCmd)

	cmd.AddCommand(showCmd)
	return cmd
}

func newLoginCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "login",
		Short: "Configure API credentials for cloud mode",
		Long:  "Prompts for endpoint and API key, then writes to ~/.granicus/config.json.",
		RunE:  runLogin,
	}
	cmd.Flags().String("endpoint", "", "API endpoint URL")
	cmd.Flags().String("api-key", "", "API key")
	return cmd
}

func runLogin(cmd *cobra.Command, args []string) error {
	endpoint, _ := cmd.Flags().GetString("endpoint")
	apiKey, _ := cmd.Flags().GetString("api-key")

	reader := bufio.NewReader(os.Stdin)

	if endpoint == "" {
		fmt.Print("Endpoint (e.g. https://api.granicus.io): ")
		line, _ := reader.ReadString('\n')
		endpoint = strings.TrimSpace(line)
	}
	if endpoint == "" {
		return fmt.Errorf("endpoint is required")
	}

	if apiKey == "" {
		fmt.Print("API Key: ")
		line, _ := reader.ReadString('\n')
		apiKey = strings.TrimSpace(line)
	}
	if apiKey == "" {
		return fmt.Errorf("API key is required")
	}

	uc := &config.UserConfig{
		Endpoint: endpoint,
		APIKey:   apiKey,
	}

	// Preserve existing org if set
	existing, _ := config.LoadUserConfig()
	if existing != nil && existing.Org != "" {
		uc.Org = existing.Org
	}

	if err := config.WriteUserConfig(uc); err != nil {
		return fmt.Errorf("writing config: %w", err)
	}

	path, _ := config.UserConfigPath()
	fmt.Printf("Credentials saved to %s\n", path)
	return nil
}

func runConfigShow(cmd *cobra.Command, args []string) error {
	cliCfg, err := config.ResolveCLIConfig(".", "", "")
	if err != nil {
		return err
	}

	mode := resolveMode(cliCfg, false)

	if wantJSON(cmd) {
		out := map[string]string{"mode": string(mode)}
		if cliCfg.Endpoint != "" {
			out["endpoint"] = cliCfg.Endpoint
		}
		if cliCfg.APIKey != "" {
			masked := cliCfg.APIKey
			if len(masked) > 8 {
				masked = masked[:4] + "..." + masked[len(masked)-4:]
			}
			out["api_key"] = masked
		}
		if cliCfg.Org != "" {
			out["org"] = cliCfg.Org
		}
		if cliCfg.Pipeline != "" {
			out["pipeline"] = cliCfg.Pipeline
		}
		return outputJSON(out)
	}

	fmt.Printf("Mode:     %s\n", mode)
	if cliCfg.Endpoint != "" {
		fmt.Printf("Endpoint: %s\n", cliCfg.Endpoint)
	}
	if cliCfg.APIKey != "" {
		masked := cliCfg.APIKey
		if len(masked) > 8 {
			masked = masked[:4] + "..." + masked[len(masked)-4:]
		}
		fmt.Printf("API Key:  %s\n", masked)
	}
	if cliCfg.Org != "" {
		fmt.Printf("Org:      %s\n", cliCfg.Org)
	}
	if cliCfg.Pipeline != "" {
		fmt.Printf("Pipeline: %s\n", cliCfg.Pipeline)
	}
	return nil
}
