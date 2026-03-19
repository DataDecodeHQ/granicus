# Platform-level scheduler: SA, invoker binding, and prune job.
# Per-pipeline jobs are in clients.tf, driven by var.pipeline_schedules.

resource "google_service_account" "scheduler" {
  # description: Granicus Cloud Scheduler service account for invoking Cloud Run
  account_id   = "granicus-scheduler"
  display_name = "Granicus Cloud Scheduler"
  project      = var.project_id
}

resource "google_cloud_run_v2_service_iam_member" "scheduler_invoker" {
  # description: scheduler SA can invoke the granicus-engine Cloud Run service
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_service.engine.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.scheduler.email}"
}

resource "google_cloud_scheduler_job" "prune" {
  # description: nightly cleanup of stale runs and old data
  name      = "granicus-prune"
  project   = var.project_id
  region    = var.region
  schedule  = var.schedule_prune
  time_zone = "UTC"

  http_target {
    uri         = "${google_cloud_run_v2_service.engine.uri}/api/v1/admin/prune"
    http_method = "POST"

    oidc_token {
      service_account_email = google_service_account.scheduler.email
      audience              = google_cloud_run_v2_service.engine.uri
    }
  }
}
