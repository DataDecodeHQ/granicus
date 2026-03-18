#!/usr/bin/env bash
set -euo pipefail

# Set up Pub/Sub topics for Granicus event streaming and result reporting.
# Ordering keys: pipeline + run_id (ensures events for a run arrive in order).

PROJECT="${GCP_PROJECT:-datadecode-platform}"

echo "Creating Pub/Sub topics in project: $PROJECT"

# Events topic: engine publishes all run/node lifecycle events
gcloud pubsub topics create granicus-events \
  --project="$PROJECT" \
  --message-ordering \
  --labels=service=granicus,env=dev \
  2>/dev/null && echo "Created topic: granicus-events" \
  || echo "Topic granicus-events already exists"

# Results topic: Cloud Run Jobs publish ResultEnvelope on completion
gcloud pubsub topics create granicus-results \
  --project="$PROJECT" \
  --message-ordering \
  --labels=service=granicus,env=dev \
  2>/dev/null && echo "Created topic: granicus-results" \
  || echo "Topic granicus-results already exists"

# Default subscriptions for the VM subscriber
gcloud pubsub subscriptions create granicus-events-vm \
  --project="$PROJECT" \
  --topic=granicus-events \
  --enable-message-ordering \
  --ack-deadline=60 \
  --message-retention-duration=7d \
  --labels=service=granicus,env=dev \
  2>/dev/null && echo "Created subscription: granicus-events-vm" \
  || echo "Subscription granicus-events-vm already exists"

gcloud pubsub subscriptions create granicus-results-engine \
  --project="$PROJECT" \
  --topic=granicus-results \
  --enable-message-ordering \
  --ack-deadline=120 \
  --message-retention-duration=1d \
  --labels=service=granicus,env=dev \
  2>/dev/null && echo "Created subscription: granicus-results-engine" \
  || echo "Subscription granicus-results-engine already exists"

echo "Pub/Sub setup complete"
