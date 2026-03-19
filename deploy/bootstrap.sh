#!/usr/bin/env bash
set -euo pipefail

# Bootstrap script for Granicus deployment on datadecode-platform.
# Run this ONCE from a machine with gcloud + gh CLI access.
#
# Prerequisites:
#   - gcloud authenticated with datadecode-platform project owner access
#   - gh CLI authenticated with DataDecodeHQ org access
#   - WIF pool already exists (created during Tanit setup)

PROJECT="datadecode-platform"
REGION="us-central1"
REPO="DataDecodeHQ/granicus"
SA_NAME="granicus-deploy"
SA_EMAIL="${SA_NAME}@${PROJECT}.iam.gserviceaccount.com"

echo "=== Granicus Bootstrap ==="
echo "Project: $PROJECT"
echo "Region:  $REGION"
echo "Repo:    $REPO"
echo ""

# --- Step 1: Service account ---
echo "--- Step 1: Create service account ---"
gcloud iam service-accounts create "$SA_NAME" \
  --project="$PROJECT" \
  --display-name="Granicus Deploy (GitHub Actions)" \
  2>/dev/null && echo "Created SA: $SA_EMAIL" \
  || echo "SA already exists: $SA_EMAIL"

# Grant roles for CI/CD
for ROLE in \
  roles/artifactregistry.admin \
  roles/run.developer \
  roles/iam.serviceAccountUser \
  roles/storage.admin \
  roles/secretmanager.secretAccessor \
  roles/datastore.user \
  roles/pubsub.admin \
  roles/cloudscheduler.admin; do
  gcloud projects add-iam-policy-binding "$PROJECT" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="$ROLE" \
    --condition=None \
    --quiet 2>/dev/null
  echo "  Granted $ROLE"
done

# --- Step 2: Artifact Registry ---
echo ""
echo "--- Step 2: Create Artifact Registry repo ---"
gcloud artifacts repositories create granicus \
  --project="$PROJECT" \
  --location="$REGION" \
  --repository-format=docker \
  2>/dev/null && echo "Created repo: granicus" \
  || echo "Repo already exists: granicus"

# --- Step 3: WIF binding ---
echo ""
echo "--- Step 3: Bind WIF to granicus-deploy SA ---"

# Get the existing WIF pool
POOL_NAME=$(gcloud iam workload-identity-pools list \
  --project="$PROJECT" --location=global \
  --format="value(name)" --limit=1)

if [ -z "$POOL_NAME" ]; then
  echo "ERROR: No WIF pool found. Was Tanit set up on this project?"
  exit 1
fi

POOL_ID=$(basename "$POOL_NAME")
echo "Found WIF pool: $POOL_ID"

# Get the provider
PROVIDER_NAME=$(gcloud iam workload-identity-pools providers list \
  --project="$PROJECT" --location=global \
  --workload-identity-pool="$POOL_ID" \
  --format="value(name)" --limit=1)

if [ -z "$PROVIDER_NAME" ]; then
  echo "ERROR: No WIF provider found in pool $POOL_ID"
  exit 1
fi
echo "Found WIF provider: $(basename "$PROVIDER_NAME")"

# Allow the granicus repo to impersonate this SA
gcloud iam service-accounts add-iam-policy-binding "$SA_EMAIL" \
  --project="$PROJECT" \
  --role="roles/iam.workloadIdentityUser" \
  --member="principalSet://iam.googleapis.com/${POOL_NAME}/attribute.repository/${REPO}" \
  --quiet 2>/dev/null
echo "Bound WIF for repo $REPO -> $SA_EMAIL"

# --- Step 4: GitHub secrets ---
echo ""
echo "--- Step 4: Set GitHub secrets ---"

# The full provider resource name
FULL_PROVIDER="projects/$(gcloud projects describe $PROJECT --format='value(projectNumber)')/locations/global/workloadIdentityPools/${POOL_ID}/providers/$(basename "$PROVIDER_NAME")"

gh secret set GCP_WORKLOAD_IDENTITY_PROVIDER \
  --repo="$REPO" --body="$FULL_PROVIDER"
echo "Set secret: GCP_WORKLOAD_IDENTITY_PROVIDER"

gh secret set GCP_SERVICE_ACCOUNT \
  --repo="$REPO" --body="$SA_EMAIL"
echo "Set secret: GCP_SERVICE_ACCOUNT"

# --- Step 5: GitHub variables ---
echo ""
echo "--- Step 5: Set GitHub variables ---"

gh variable set GCP_PROJECT_ID \
  --repo="$REPO" --body="$PROJECT"
echo "Set variable: GCP_PROJECT_ID"

gh variable set CLOUD_RUN_REGION \
  --repo="$REPO" --body="$REGION"
echo "Set variable: CLOUD_RUN_REGION"

gh variable set ARTIFACT_REGISTRY_REPO \
  --repo="$REPO" --body="granicus"
echo "Set variable: ARTIFACT_REGISTRY_REPO"

echo ""
echo "=== Bootstrap complete ==="
echo ""
echo "Next steps:"
echo "  1. Push this branch to GitHub"
echo "  2. Go to: https://github.com/$REPO/actions/workflows/deploy.yml"
echo "  3. Click 'Run workflow' -> environment=dev, skip_infra=false"
echo "  4. First deploy provisions Firestore, Pub/Sub, GCS buckets, then deploys"
echo ""
echo "After first deploy:"
echo "  - Run setup-scheduler.sh to create Cloud Scheduler jobs"
echo "  - Run setup-workload-identity.sh to grant BQ cross-project access"
echo "  - Push your first pipeline: granicus push <pipeline-dir> --pipeline <name> --activate"
