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

    max_instance_request_concurrency = 1
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
        http_get {
          path = "/api/v1/health"
          port = 8080
        }
        initial_delay_seconds = 5
        period_seconds        = 5
        failure_threshold     = 6
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

resource "google_cloud_run_v2_job" "python_runner" {
  # description: Cloud Run Job for dispatched Python pipeline tasks
  name     = "granicus-python-runner"
  project  = var.project_id
  location = var.region

  template {
    template {
      service_account = google_service_account.engine.email
      max_retries     = 1
      timeout         = "1800s"

      containers {
        image = "${var.region}-docker.pkg.dev/${var.project_id}/${var.ar_repository}/python-runner:latest"

        resources {
          limits = {
            cpu    = "1"
            memory = "2Gi"
          }
        }

        env {
          name  = "GRANICUS_RESULT_TOPIC"
          value = "granicus-results"
        }
        env {
          name  = "GRANICUS_PUBSUB_PROJECT"
          value = var.project_id
        }
      }
    }
  }

  lifecycle {
    ignore_changes = [
      template[0].template[0].containers[0].image,
      client,
      client_version,
    ]
  }
}
