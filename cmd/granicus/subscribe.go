package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"cloud.google.com/go/pubsub/v2"
	"github.com/spf13/cobra"

	"github.com/DataDecodeHQ/granicus/internal/logging"
)

func newSubscribeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "subscribe",
		Short: "Subscribe to Pub/Sub events and store locally",
		RunE:  runSubscribe,
	}
	cmd.Flags().String("project", "", "GCP project for Pub/Sub")
	cmd.Flags().String("subscription", "granicus-events-vm", "Pub/Sub subscription name")
	cmd.Flags().String("data-dir", ".granicus", "Local data directory")
	return cmd
}

func runSubscribe(cmd *cobra.Command, args []string) error {
	logging.Init(true)

	project, _ := cmd.Flags().GetString("project")
	subscription, _ := cmd.Flags().GetString("subscription")
	dataDir, _ := cmd.Flags().GetString("data-dir")

	if project == "" {
		project = os.Getenv("GRANICUS_PUBSUB_PROJECT")
		if project == "" {
			project = os.Getenv("GRANICUS_FIRESTORE_PROJECT")
		}
	}
	if project == "" {
		return fmt.Errorf("project required (--project or GRANICUS_PUBSUB_PROJECT)")
	}

	// Create local directories
	for _, dir := range []string{"runs", "failures", "state"} {
		os.MkdirAll(filepath.Join(dataDir, dir), 0755)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := pubsub.NewClient(ctx, project)
	if err != nil {
		return fmt.Errorf("creating Pub/Sub client: %w", err)
	}
	defer client.Close()

	sub := client.Subscriber(subscription)
	sub.ReceiveSettings.MaxOutstandingMessages = 100

	slog.Info("subscribing to events", "subscription", subscription, "project", project)

	// Handle shutdown
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
		<-sigCh
		slog.Info("shutting down subscriber")
		cancel()
	}()

	err = sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		var event map[string]any
		if err := json.Unmarshal(msg.Data, &event); err != nil {
			slog.Warn("malformed event message", "error", err)
			msg.Ack()
			return
		}

		pipeline, _ := event["pipeline"].(string)
		runID, _ := event["run_id"].(string)
		eventType, _ := event["event_type"].(string)

		slog.Debug("received event", "pipeline", pipeline, "run_id", runID, "type", eventType)

		// Store run summaries
		if eventType == "run_completed" || eventType == "run_started" {
			runDir := filepath.Join(dataDir, "runs", pipeline)
			os.MkdirAll(runDir, 0755)
			path := filepath.Join(runDir, runID+".json")
			data, _ := json.MarshalIndent(event, "", "  ")
			os.WriteFile(path, data, 0644)
		}

		// Store failures
		if eventType == "asset_failed" || eventType == "node_failed" || eventType == "run_completed" {
			status, _ := event["status"].(string)
			if eventType == "asset_failed" || eventType == "node_failed" || status == "completed_with_failures" || status == "failed" {
				failDir := filepath.Join(dataDir, "failures")
				path := filepath.Join(failDir, runID+"_"+eventType+".json")
				data, _ := json.MarshalIndent(event, "", "  ")
				os.WriteFile(path, data, 0644)
			}
		}

		msg.Ack()
	})

	if err != nil && err != context.Canceled {
		return fmt.Errorf("subscription error: %w", err)
	}

	return nil
}
