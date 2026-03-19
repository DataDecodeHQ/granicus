resource "google_cloud_run_v2_service" "engine" {
  # description: Granicus pipeline engine — receives triggers, orchestrates BigQuery pipelines
  name     = var.engine_service_name
  project  = var.project_id
  location = var.region
  ingress  = "INGRESS_TRAFFIC_ALL"

  template {
    scaling {
      min_instance_count = var.cloud_run_min_instances
      max_instance_count = var.cloud_run_max_instances
    }

    max_instance_request_concurrency = 80
    timeout                          = var.cloud_run_timeout
    service_account                  = google_service_account.engine.email

    labels = {
      service = "granicus"
      env     = "dev"
    }

    containers {
      image   = "${var.region}-docker.pkg.dev/${var.project_id}/${var.ar_repository}/engine:latest"
      command = ["granicus"]
      args    = ["serve", "--env", "dev"]

      ports {
        container_port = 8080
        name           = "http1"
      }

      resources {
        limits = {
          cpu    = var.cloud_run_cpu
          memory = var.cloud_run_memory
        }
        cpu_idle          = false
        startup_cpu_boost = true
      }

      startup_probe {
        tcp_socket {
          port = 8080
        }
        period_seconds    = 240
        timeout_seconds   = 240
        failure_threshold = 1
      }

      env {
        name  = "GRANICUS_FIRESTORE_PROJECT"
        value = var.project_id
      }
      env {
        name  = "GRANICUS_PIPELINES_BUCKET"
        value = var.pipelines_bucket
      }
      env {
        name  = "GRANICUS_OPS_BUCKET"
        value = "granicus-ops"
      }
      env {
        name  = "GRANICUS_PIPELINE_SOURCE"
        value = "gcs"
      }
      env {
        name  = "GRANICUS_STATE_BACKEND"
        value = "firestore"
      }
    }
  }

  lifecycle {
    ignore_changes = [
      template[0].containers[0].image,
      template[0].revision,
      client,
      client_version,
    ]
  }
}
