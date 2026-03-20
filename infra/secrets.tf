# Platform-level secrets (client secrets are in clients.tf)

resource "google_secret_manager_secret" "api_key" {
  secret_id = "granicus-api-key"
  project   = var.project_id

  replication {
    auto {}
  }

  labels = {
    service = "granicus"
    purpose = "api-auth"
  }
}

resource "google_secret_manager_secret_iam_member" "engine_api_key" {
  secret_id = google_secret_manager_secret.api_key.secret_id
  project   = var.project_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.engine.email}"
}
