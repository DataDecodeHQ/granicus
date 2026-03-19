#!/usr/bin/env bash
set -euo pipefail

PROJECT="${GCP_PROJECT:-datadecode-platform}"
REGION="${GCP_REGION:-us-central1}"
ENGINE_SA="granicus-engine@${PROJECT}.iam.gserviceaccount.com"

# Target BQ projects
ANALYTEHEALTH_PROJECT="${ANALYTEHEALTH_PROJECT:-api-project-178709533099}"
PATERNITYLABS_PROJECT="${PATERNITYLABS_PROJECT:-paternitylab}"

echo "Configuring Workload Identity for Granicus in project: $PROJECT"

# Create engine service account
gcloud iam service-accounts create granicus-engine \
  --project="$PROJECT" \
  --display-name="Granicus Engine (Cloud Run)" \
  2>/dev/null && echo "Created SA: granicus-engine" \
  || echo "SA granicus-engine already exists"

# Update Cloud Run service to use engine SA (replaces mounted secrets)
gcloud run services update granicus-engine \
  --project="$PROJECT" --region="$REGION" \
  --service-account="$ENGINE_SA" \
  --remove-secrets=/secrets/analytehealth.json \
  --remove-secrets=/secrets/paternitylabs.json \
  2>/dev/null || true

echo "Granting BQ access to $ENGINE_SA"

# Grant BQ access in AnalyteHealth project
for ROLE in roles/bigquery.dataEditor roles/bigquery.jobUser; do
  gcloud projects add-iam-policy-binding "$ANALYTEHEALTH_PROJECT" \
    --member="serviceAccount:${ENGINE_SA}" \
    --role="$ROLE" \
    --condition=None \
    2>/dev/null && echo "  Granted $ROLE in $ANALYTEHEALTH_PROJECT" \
    || echo "  $ROLE already granted in $ANALYTEHEALTH_PROJECT"
done

# Grant BQ access in PaternityLabs project
for ROLE in roles/bigquery.dataEditor roles/bigquery.jobUser; do
  gcloud projects add-iam-policy-binding "$PATERNITYLABS_PROJECT" \
    --member="serviceAccount:${ENGINE_SA}" \
    --role="$ROLE" \
    --condition=None \
    2>/dev/null && echo "  Granted $ROLE in $PATERNITYLABS_PROJECT" \
    || echo "  $ROLE already granted in $PATERNITYLABS_PROJECT"
done

# Grant Firestore access
gcloud projects add-iam-policy-binding "$PROJECT" \
  --member="serviceAccount:${ENGINE_SA}" \
  --role="roles/datastore.user" \
  --condition=None \
  2>/dev/null || true

# Grant GCS access
gcloud projects add-iam-policy-binding "$PROJECT" \
  --member="serviceAccount:${ENGINE_SA}" \
  --role="roles/storage.objectAdmin" \
  --condition=None \
  2>/dev/null || true

# Grant Pub/Sub access
gcloud projects add-iam-policy-binding "$PROJECT" \
  --member="serviceAccount:${ENGINE_SA}" \
  --role="roles/pubsub.publisher" \
  --condition=None \
  2>/dev/null || true

echo "Workload Identity setup complete"
echo "Engine SA: $ENGINE_SA"
echo "No JSON files needed — BQ access via attached service account"
