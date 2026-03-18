#!/usr/bin/env bash
set -euo pipefail

PROJECT="${GCP_PROJECT:-datadecode-platform}"
REGION="${GCP_REGION:-us-central1}"

echo "Setting up Secret Manager secrets in project: $PROJECT"

# Enable Secret Manager API
gcloud services enable secretmanager.googleapis.com --project="$PROJECT"

# Create secrets for BQ service accounts
for SA_NAME in analytehealth paternitylabs; do
  SECRET_ID="granicus-sa-${SA_NAME}"

  gcloud secrets create "$SECRET_ID" \
    --project="$PROJECT" \
    --replication-policy=automatic \
    --labels=service=granicus \
    2>/dev/null && echo "Created secret: $SECRET_ID" \
    || echo "Secret $SECRET_ID already exists"

  echo "Upload SA key with: gcloud secrets versions add $SECRET_ID --data-file=path/to/${SA_NAME}.json --project=$PROJECT"
done

# Update Cloud Run to mount secrets
echo ""
echo "After uploading keys, update Cloud Run with:"
echo "gcloud run services update granicus-engine \\"
echo "  --project=$PROJECT --region=$REGION \\"
echo "  --update-secrets=/secrets/analytehealth.json=granicus-sa-analytehealth:latest \\"
echo "  --update-secrets=/secrets/paternitylabs.json=granicus-sa-paternitylabs:latest"

echo ""
echo "Secrets setup complete"
