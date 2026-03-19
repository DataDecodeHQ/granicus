# Granicus Cloud Deployment

Deploy Granicus as a Cloud Run service on GCP with Firestore state, GCS archival, and Pub/Sub event streaming.

## Prerequisites

- `gcloud` CLI authenticated with `datadecode-platform` project
- Docker installed locally
- APIs enabled: Cloud Run, Firestore, Pub/Sub, Cloud Scheduler, Secret Manager, Artifact Registry

```bash
gcloud services enable \
  run.googleapis.com \
  firestore.googleapis.com \
  pubsub.googleapis.com \
  cloudscheduler.googleapis.com \
  secretmanager.googleapis.com \
  artifactregistry.googleapis.com \
  --project=datadecode-platform
```

## Step 1: Provision Infrastructure

Run these scripts in order. Each is idempotent -- safe to re-run.

```bash
cd granicus/deploy

# 1. Firestore database + composite indexes
bash setup-firestore.sh

# 2. Pub/Sub topics and subscriptions
bash setup-pubsub.sh

# 3. Create GCS buckets (manual -- not scripted)
gsutil mb -l us-central1 gs://granicus-pipelines
gsutil mb -l us-central1 gs://granicus-ops
```

## Step 2: Build and Deploy the Engine

```bash
# Build container, push to Artifact Registry, deploy Cloud Run service
bash setup-cloud-run.sh
```

This creates:
- Artifact Registry repo `granicus` in `us-central1`
- Cloud Run service `granicus-engine` (0-3 instances, 1Gi/1CPU, 60min timeout)

The service URL is printed at the end. Save it -- you'll need it for scheduler and CLI config.

## Step 3: Set Up Auth

**Option A: SA keys (quick start, Phase 2)**

```bash
bash setup-secrets.sh

# Upload your existing SA keys to Secret Manager
gcloud secrets versions add granicus-sa-analytehealth \
  --data-file=.credentials/bigquery/analytehealth.json \
  --project=datadecode-platform

gcloud secrets versions add granicus-sa-paternitylabs \
  --data-file=.credentials/bigquery/paternitylabs.json \
  --project=datadecode-platform

# Mount secrets on Cloud Run
gcloud run services update granicus-engine \
  --project=datadecode-platform --region=us-central1 \
  --update-secrets=/secrets/analytehealth.json=granicus-sa-analytehealth:latest \
  --update-secrets=/secrets/paternitylabs.json=granicus-sa-paternitylabs:latest
```

**Option B: Workload Identity (production, Phase 5)**

```bash
bash setup-workload-identity.sh
```

No SA JSON files needed. Cloud Run gets BQ access via its attached service account.

## Step 4: Push Your First Pipeline

```bash
# From the project root
granicus push project/granicus_pipeline/paternity_labs/ --pipeline paternity_labs --activate
```

This packages the pipeline directory as a versioned archive, uploads to GCS, registers in Firestore, and sets it as the active version.

Verify:

```bash
granicus versions paternity_labs
```

## Step 5: Set Up Scheduled Runs

```bash
bash setup-scheduler.sh
```

Creates Cloud Scheduler jobs:
- `paternity_labs` at 6:00 AM UTC
- `legacy_dbt` at 7:00 AM UTC
- `analyte_health` at 7:15 AM UTC
- Nightly Firestore prune at 2:00 AM UTC

## Step 6: Configure Local CLI

```bash
# Get the service URL
SERVICE_URL=$(gcloud run services describe granicus-engine \
  --project=datadecode-platform --region=us-central1 \
  --format="value(status.url)")

# Set up cloud mode
granicus config set cloud.endpoint "$SERVICE_URL"
granicus config set cloud.api_key "your-api-key-here"

# Verify
granicus config show
```

## Usage

### Trigger a Run Manually

```bash
# Full pipeline run
granicus trigger paternity_labs

# Run specific assets
granicus trigger analyte_health --assets stg_stdlocal__orders,ent_order_line

# Test mode (temporary dataset)
granicus trigger paternity_labs --test --test-window 7d

# Dry run (show plan without executing)
granicus trigger paternity_labs --dry-run
```

### Monitor Runs

```bash
# Current status
granicus cloud-status

# Recent run history
granicus history --pipeline paternity_labs --since 7d

# Events for a specific run
granicus cloud-events --run-id run_20260319_060000

# Recent failures
granicus failures --since 7d

# Node reliability stats
granicus stats --pipeline analyte_health --node map_patient_identity --since 30d

# Interval state
granicus intervals --pipeline analyte_health --asset stg_stdlocal__orders
```

All commands support `--json` for machine-readable output.

### Pipeline Versioning

```bash
# Push a new version
granicus push project/granicus_pipeline/analyte_health/ --pipeline analyte_health

# List versions
granicus versions analyte_health

# Compare versions
granicus diff analyte_health 3 4

# Activate a version
granicus activate analyte_health 4

# Push and activate in one step
granicus push project/granicus_pipeline/analyte_health/ --pipeline analyte_health --activate
```

### Re-run from Failure

```bash
# Find the failed run
granicus failures --json --since 1d

# Re-run from where it failed
granicus trigger analyte_health --from-failure run_20260319_071500
```

### VM Event Subscriber

On the VM, run the subscriber to pull events and maintain local state:

```bash
granicus subscribe --project datadecode-platform --data-dir .granicus
```

This stores run summaries and failure records locally in `.granicus/runs/` and `.granicus/failures/`.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `GRANICUS_STATE_BACKEND` | `sqlite` | State backend: `sqlite` or `firestore` |
| `GRANICUS_FIRESTORE_PROJECT` | -- | GCP project for Firestore |
| `GRANICUS_PIPELINES_BUCKET` | `granicus-pipelines` | GCS bucket for pipeline archives |
| `GRANICUS_OPS_BUCKET` | `granicus-ops` | GCS bucket for run archives |
| `GRANICUS_PIPELINE_SOURCE` | `local` | Pipeline source: `local` or `gcs` |
| `GRANICUS_PUBSUB_PROJECT` | (from FIRESTORE_PROJECT) | GCP project for Pub/Sub |
| `GRANICUS_EVENTS_TOPIC` | `granicus-events` | Pub/Sub topic for events |
| `GRANICUS_RESULT_TOPIC` | `granicus-results` | Pub/Sub topic for job results |
| `GRANICUS_RETENTION_DAYS` | `30` | Days before Firestore data is pruned |
| `GRANICUS_API_URL` | -- | Engine URL (for CLI cloud mode) |
| `GRANICUS_API_KEY` | -- | API key (for CLI cloud mode) |
| `GCP_PROJECT` | `datadecode-platform` | GCP project (for deploy scripts) |
| `GCP_REGION` | `us-central1` | GCP region (for deploy scripts) |

## Architecture

```
                    Cloud Scheduler (cron)
                           |
                           v
                  Cloud Run Service (granicus-engine)
                   /        |         \
                  v         v          v
            Firestore    GCS        Pub/Sub
            (state,    (pipeline   (events,
             runs,     archives,    results)
             events)   run JSONL)      |
                                       v
                                  VM Subscriber
                                  (local state)
```

**Control plane**: Cloud Run Service handles scheduling, API, state management.
**Compute plane**: BigQuery (SQL assets) + Cloud Run Jobs (Python assets).
**Storage**: Firestore for hot operational data (30 days), GCS for permanent archives.

## Deploy Scripts Reference

| Script | What it does |
|--------|-------------|
| `setup-firestore.sh` | Enable Firestore Native mode, deploy composite indexes |
| `setup-pubsub.sh` | Create topics (events, results) and subscriptions |
| `setup-cloud-run.sh` | Build Docker image, push to Artifact Registry, deploy Cloud Run |
| `setup-secrets.sh` | Create Secret Manager secrets for BQ service accounts |
| `setup-scheduler.sh` | Create Cloud Scheduler jobs for all pipelines + nightly prune |
| `setup-workload-identity.sh` | Set up engine SA with cross-project BQ access (replaces secrets) |

## Testing the Deployment

Quick smoke test after deployment:

```bash
# 1. Check the service is healthy
curl -s "$SERVICE_URL/api/v1/health" | jq

# 2. Push a pipeline
granicus push project/granicus_pipeline/paternity_labs/ \
  --pipeline paternity_labs --activate

# 3. Trigger a test run
granicus trigger paternity_labs --test --test-window 7d

# 4. Watch the run
granicus cloud-status --json

# 5. Check results
granicus history --pipeline paternity_labs --since 1d --json
```
