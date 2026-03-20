terraform {
  required_version = ">= 1.5"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }

  # Local state for now. Migrate to GCS backend once stable:
  #
  # backend "gcs" {
  #   bucket = "granicus-terraform-state"
  #   prefix = "granicus"
  # }
}

provider "google" {
  project = var.project_id
  region  = var.region
}
