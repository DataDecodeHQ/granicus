resource "google_artifact_registry_repository" "granicus" {
  # description: Docker image repository for Granicus engine builds
  repository_id = var.ar_repository
  project       = var.project_id
  location      = var.region
  format        = "DOCKER"
}
