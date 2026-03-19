# Workload Identity Federation — pool and provider owned by Tanit.
# Granicus binds its deploy SA to the existing pool.

locals {
  # Pool name constructed from project number — pool itself is managed in tanit/infra
  wif_pool_name = "projects/${var.project_number}/locations/global/workloadIdentityPools/github-actions"
}

# Allow GitHub Actions for the granicus repo to impersonate the deploy SA
resource "google_service_account_iam_member" "wif_deploy_binding" {
  # description: WIF binding lets GitHub Actions impersonate deploy SA for CI/CD
  service_account_id = google_service_account.deploy.id
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/${local.wif_pool_name}/attribute.repository/${var.github_org}/${var.github_repo}"
}
