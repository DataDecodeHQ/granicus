resource "google_storage_bucket" "pipelines" {
  # description: GCS bucket for Granicus pipeline definitions and configuration
  name     = var.pipelines_bucket
  project  = var.project_id
  location = "US"

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
}
