package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// addJSONFlag adds a --json boolean flag to the command.
func addJSONFlag(cmd *cobra.Command) {
	cmd.Flags().Bool("json", false, "Output as JSON")
}

// wantJSON returns true if the --json flag is set on the command.
func wantJSON(cmd *cobra.Command) bool {
	v, _ := cmd.Flags().GetBool("json")
	return v
}

// outputJSON marshals v as indented JSON to stdout.
func outputJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return fmt.Errorf("json output: %w", err)
	}
	return nil
}
