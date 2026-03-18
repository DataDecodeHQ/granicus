#!/usr/bin/env bash
set -euo pipefail

PROJECT="${GCP_PROJECT:-datadecode-platform}"
REGION="${GCP_REGION:-us-central1}"

SERVICE_URL=$(gcloud run services describe granicus-engine \
  --project="$PROJECT" --region="$REGION" --format="value(status.url)" 2>/dev/null)

if [ -z "$SERVICE_URL" ]; then
  echo "Error: granicus-engine Cloud Run service not found"
  exit 1
fi

SA_EMAIL="granicus-scheduler@${PROJECT}.iam.gserviceaccount.com"

# Create scheduler service account if not exists
gcloud iam service-accounts create granicus-scheduler \
  --project="$PROJECT" \
  --display-name="Granicus Cloud Scheduler" \
  2>/dev/null || true

# Grant invoker role
gcloud run services add-iam-policy-binding granicus-engine \
  --project="$PROJECT" --region="$REGION" \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/run.invoker" \
  2>/dev/null || true

echo "Setting up Cloud Scheduler jobs in project: $PROJECT"

# analyte_health: daily at 7:15 AM UTC
gcloud scheduler jobs create http granicus-analyte-health \
  --project="$PROJECT" --location="$REGION" \
  --schedule="15 7 * * *" --time-zone="UTC" \
  --uri="${SERVICE_URL}/api/v1/pipelines/analyte_health/trigger" \
  --http-method=POST \
  --headers="Content-Type=application/json" \
  --message-body='{"pipeline":"analyte_health"}' \
  --oidc-service-account-email="$SA_EMAIL" \
  --oidc-token-audience="$SERVICE_URL" \
  2>/dev/null && echo "Created: granicus-analyte-health" \
  || echo "Job granicus-analyte-health already exists"

# legacy_dbt: daily at 7:00 AM UTC
gcloud scheduler jobs create http granicus-legacy-dbt \
  --project="$PROJECT" --location="$REGION" \
  --schedule="0 7 * * *" --time-zone="UTC" \
  --uri="${SERVICE_URL}/api/v1/pipelines/legacy_dbt/trigger" \
  --http-method=POST \
  --headers="Content-Type=application/json" \
  --message-body='{"pipeline":"legacy_dbt"}' \
  --oidc-service-account-email="$SA_EMAIL" \
  --oidc-token-audience="$SERVICE_URL" \
  2>/dev/null && echo "Created: granicus-legacy-dbt" \
  || echo "Job granicus-legacy-dbt already exists"

# paternity_labs: daily at 6:00 AM UTC
gcloud scheduler jobs create http granicus-paternity-labs \
  --project="$PROJECT" --location="$REGION" \
  --schedule="0 6 * * *" --time-zone="UTC" \
  --uri="${SERVICE_URL}/api/v1/pipelines/paternity_labs/trigger" \
  --http-method=POST \
  --headers="Content-Type=application/json" \
  --message-body='{"pipeline":"paternity_labs"}' \
  --oidc-service-account-email="$SA_EMAIL" \
  --oidc-token-audience="$SERVICE_URL" \
  2>/dev/null && echo "Created: granicus-paternity-labs" \
  || echo "Job granicus-paternity-labs already exists"

# Nightly pruning: 2:00 AM UTC
gcloud scheduler jobs create http granicus-prune \
  --project="$PROJECT" --location="$REGION" \
  --schedule="0 2 * * *" --time-zone="UTC" \
  --uri="${SERVICE_URL}/api/v1/admin/prune" \
  --http-method=POST \
  --oidc-service-account-email="$SA_EMAIL" \
  --oidc-token-audience="$SERVICE_URL" \
  2>/dev/null && echo "Created: granicus-prune" \
  || echo "Job granicus-prune already exists"

echo "Cloud Scheduler setup complete"
