output "service_url" {
  description = "Cloud Run service URL for the Granicus engine"
  value       = google_cloud_run_v2_service.engine.uri
}

output "engine_sa_email" {
  description = "Engine service account email (attached to Cloud Run)"
  value       = google_service_account.engine.email
}

output "deploy_sa_email" {
  description = "Deploy service account email (used by GitHub Actions via WIF)"
  value       = google_service_account.deploy.email
}

output "image_uri" {
  description = "Artifact Registry image URI for the engine container"
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.granicus.repository_id}/engine"
}
