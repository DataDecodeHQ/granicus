#!/usr/bin/env bash
set -euo pipefail

# Enable Firestore Native mode and deploy composite indexes for Granicus.

PROJECT="${GCP_PROJECT:-datadecode-platform}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "Setting up Firestore in project: $PROJECT"

# Enable Firestore API
gcloud services enable firestore.googleapis.com --project="$PROJECT"

# Create Firestore database in Native mode (idempotent — errors if already exists)
gcloud firestore databases create \
  --project="$PROJECT" \
  --location=us-central1 \
  --type=firestore-native \
  2>/dev/null && echo "Created Firestore database (Native mode)" \
  || echo "Firestore database already exists"

# Deploy composite indexes
gcloud firestore indexes composite create \
  --project="$PROJECT" \
  --collection-group=runs \
  --field-config field-path=pipeline,order=ascending \
  --field-config field-path=status,order=ascending \
  --field-config field-path=started_at,order=descending \
  2>/dev/null && echo "Created index: runs (pipeline+status+started_at)" \
  || echo "Index runs (pipeline+status+started_at) already exists"

gcloud firestore indexes composite create \
  --project="$PROJECT" \
  --collection-group=runs \
  --field-config field-path=pipeline,order=ascending \
  --field-config field-path=status,order=ascending \
  2>/dev/null && echo "Created index: runs (pipeline+status)" \
  || echo "Index runs (pipeline+status) already exists"

gcloud firestore indexes composite create \
  --project="$PROJECT" \
  --collection-group=events \
  --query-scope=COLLECTION_GROUP \
  --field-config field-path=event_type,order=ascending \
  --field-config field-path=timestamp,order=descending \
  2>/dev/null && echo "Created index: events (event_type+timestamp)" \
  || echo "Index events (event_type+timestamp) already exists"

echo "Firestore setup complete"
