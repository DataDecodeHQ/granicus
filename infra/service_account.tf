# Engine SA — attached to Cloud Run, needs Firestore/GCS/Secret Manager access
resource "google_service_account" "engine" {
  # description: Granicus engine Cloud Run service account
  account_id   = "granicus-engine"
  display_name = "Granicus Engine (Cloud Run)"
  project      = var.project_id
}

# Deploy SA — used by GitHub Actions via WIF for CI/CD
resource "google_service_account" "deploy" {
  # description: Granicus deploy service account for GitHub Actions CI/CD
  account_id   = "granicus-deploy"
  display_name = "Granicus Deploy (GitHub Actions)"
  project      = var.project_id
}

# --- Engine IAM: platform project ---

resource "google_project_iam_member" "engine_firestore" {
  # description: engine SA Firestore access in platform project
  project = var.project_id
  role    = "roles/datastore.user"
  member  = "serviceAccount:${google_service_account.engine.email}"
}

resource "google_project_iam_member" "engine_storage" {
  # description: engine SA GCS access in platform project
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.engine.email}"
}

# engine_pubsub removed — Pub/Sub resources not yet provisioned (pubsub.tf is commented out).
# Re-add when Pub/Sub topics are uncommented and applied.

# --- Cross-project BQ access ---
# BQ on api-project-178709533099 (AnalyteHealth) and paternitylab is accessed
# via mounted SA credential files (Secret Manager), not cross-project IAM.
# When we migrate to cross-project IAM, add google_project_iam_member resources here.

# --- Deploy SA IAM: project-level roles for CI/CD ---

resource "google_project_iam_member" "deploy_artifact_registry" {
  # description: deploy SA Artifact Registry writer for pushing images
  project = var.project_id
  role    = "roles/artifactregistry.writer"
  member  = "serviceAccount:${google_service_account.deploy.email}"
}

resource "google_project_iam_member" "deploy_cloud_run" {
  # description: deploy SA Cloud Run developer for deploying services
  project = var.project_id
  role    = "roles/run.developer"
  member  = "serviceAccount:${google_service_account.deploy.email}"
}

resource "google_service_account_iam_member" "deploy_sa_user" {
  # description: deploy SA can act as engine SA (needed to attach engine SA to Cloud Run)
  service_account_id = google_service_account.engine.id
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.deploy.email}"
}

resource "google_project_iam_member" "deploy_storage" {
  # description: deploy SA storage admin for managing GCS buckets
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.deploy.email}"
}

resource "google_project_iam_member" "deploy_firestore" {
  # description: deploy SA Firestore access for provisioning
  project = var.project_id
  role    = "roles/datastore.user"
  member  = "serviceAccount:${google_service_account.deploy.email}"
}

# deploy_pubsub removed — Pub/Sub resources not yet provisioned (pubsub.tf is commented out).
# Re-add when Pub/Sub topics are uncommented and applied.

resource "google_storage_bucket_iam_member" "deploy_tf_state" {
  # description: deploy SA access to Terraform state bucket for CI/CD
  bucket = "granicus-terraform-state"
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.deploy.email}"
}

resource "google_project_iam_member" "deploy_scheduler" {
  # description: deploy SA Cloud Scheduler admin for provisioning jobs
  project = var.project_id
  role    = "roles/cloudscheduler.admin"
  member  = "serviceAccount:${google_service_account.deploy.email}"
}
