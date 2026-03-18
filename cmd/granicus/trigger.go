package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/spf13/cobra"
)

func newTriggerCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "trigger <pipeline>",
		Short: "Trigger a pipeline run via the engine API",
		Args:  cobra.ExactArgs(1),
		RunE:  runTrigger,
	}
	cmd.Flags().StringSlice("assets", nil, "Run only these assets")
	cmd.Flags().Bool("downstream-only", false, "Run downstream of specified assets only")
	cmd.Flags().String("from-date", "", "Override start date")
	cmd.Flags().String("to-date", "", "Override end date")
	cmd.Flags().String("from-failure", "", "Re-run from a failed run (parent run ID)")
	cmd.Flags().Int("version", 0, "Run a specific version (without changing active)")
	cmd.Flags().Bool("test", false, "Test mode")
	cmd.Flags().String("test-window", "", "Test window (e.g. 7d, 4w)")
	cmd.Flags().Bool("dry-run", false, "Show execution plan without running")
	cmd.Flags().Bool("json", false, "JSON output")
	return cmd
}

func runTrigger(cmd *cobra.Command, args []string) error {
	pipeline := args[0]
	assets, _ := cmd.Flags().GetStringSlice("assets")
	downstreamOnly, _ := cmd.Flags().GetBool("downstream-only")
	fromDate, _ := cmd.Flags().GetString("from-date")
	toDate, _ := cmd.Flags().GetString("to-date")
	fromFailure, _ := cmd.Flags().GetString("from-failure")
	version, _ := cmd.Flags().GetInt("version")
	testMode, _ := cmd.Flags().GetBool("test")
	testWindow, _ := cmd.Flags().GetString("test-window")
	dryRun, _ := cmd.Flags().GetBool("dry-run")
	jsonOutput, _ := cmd.Flags().GetBool("json")

	apiURL := os.Getenv("GRANICUS_API_URL")
	apiKey := os.Getenv("GRANICUS_API_KEY")

	if apiURL == "" {
		return fmt.Errorf("GRANICUS_API_URL not set")
	}

	body := map[string]any{
		"pipeline": pipeline,
	}
	if len(assets) > 0 {
		body["assets"] = assets
	}
	if downstreamOnly {
		body["downstream_only"] = true
	}
	if fromDate != "" {
		body["from_date"] = fromDate
	}
	if toDate != "" {
		body["to_date"] = toDate
	}
	if fromFailure != "" {
		body["from_failure"] = fromFailure
	}
	if version > 0 {
		body["version"] = version
	}
	if testMode {
		body["test"] = true
	}
	if testWindow != "" {
		body["test_window"] = testWindow
	}
	if dryRun {
		body["dry_run"] = true
	}

	data, _ := json.Marshal(body)
	url := apiURL + "/api/v1/pipelines/" + pipeline + "/trigger"

	req, err := http.NewRequest("POST", url, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("API request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("API error %d: %s", resp.StatusCode, string(respBody))
	}

	if jsonOutput {
		fmt.Println(string(respBody))
	} else {
		var result map[string]any
		json.Unmarshal(respBody, &result)
		if runID, ok := result["run_id"].(string); ok {
			fmt.Printf("Triggered %s: run_id=%s\n", pipeline, runID)
		} else {
			fmt.Println(string(respBody))
		}
	}

	return nil
}
