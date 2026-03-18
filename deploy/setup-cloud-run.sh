#!/usr/bin/env bash
set -euo pipefail

PROJECT="${GCP_PROJECT:-datadecode-platform}"
REGION="${GCP_REGION:-us-central1}"
REPO="granicus"
IMAGE="engine"

echo "Setting up Cloud Run for Granicus in project: $PROJECT"

# Create Artifact Registry repository
gcloud artifacts repositories create "$REPO" \
  --project="$PROJECT" \
  --location="$REGION" \
  --repository-format=docker \
  --labels=service=granicus \
  2>/dev/null && echo "Created Artifact Registry: $REPO" \
  || echo "Artifact Registry $REPO already exists"

# Build and push image
IMAGE_TAG="${REGION}-docker.pkg.dev/${PROJECT}/${REPO}/${IMAGE}:latest"
echo "Building and pushing: $IMAGE_TAG"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
GRANICUS_DIR="$(dirname "$SCRIPT_DIR")"

docker build -t "$IMAGE_TAG" "$GRANICUS_DIR"
docker push "$IMAGE_TAG"

# Deploy Cloud Run service
gcloud run deploy granicus-engine \
  --project="$PROJECT" \
  --region="$REGION" \
  --image="$IMAGE_TAG" \
  --platform=managed \
  --memory=1Gi \
  --cpu=1 \
  --min-instances=0 \
  --max-instances=3 \
  --timeout=3600 \
  --cpu-boost \
  --no-cpu-throttling \
  --port=8080 \
  --set-env-vars="GRANICUS_FIRESTORE_PROJECT=${PROJECT},GRANICUS_PIPELINES_BUCKET=granicus-pipelines,GRANICUS_OPS_BUCKET=granicus-ops,GRANICUS_PIPELINE_SOURCE=gcs" \
  --labels=service=granicus,env=dev \
  --no-allow-unauthenticated

echo "Cloud Run setup complete"
echo "Service URL:"
gcloud run services describe granicus-engine \
  --project="$PROJECT" \
  --region="$REGION" \
  --format="value(status.url)"
