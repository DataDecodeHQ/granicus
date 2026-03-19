resource "google_firestore_database" "default" {
  # description: Firestore Native database for Granicus run state and events
  project     = var.project_id
  name        = "(default)"
  location_id = var.region
  type        = "FIRESTORE_NATIVE"
}

# Composite index: query runs by pipeline + status, ordered by start time
resource "google_firestore_index" "runs_pipeline_status_started" {
  # description: composite index for querying runs by pipeline, status, and recency
  project    = var.project_id
  database   = google_firestore_database.default.name
  collection = "runs"

  fields {
    field_path = "pipeline"
    order      = "ASCENDING"
  }
  fields {
    field_path = "status"
    order      = "ASCENDING"
  }
  fields {
    field_path = "started_at"
    order      = "DESCENDING"
  }
}

# Composite index: query runs by pipeline + status (no time ordering)
resource "google_firestore_index" "runs_pipeline_status" {
  # description: composite index for filtering runs by pipeline and status
  project    = var.project_id
  database   = google_firestore_database.default.name
  collection = "runs"

  fields {
    field_path = "pipeline"
    order      = "ASCENDING"
  }
  fields {
    field_path = "status"
    order      = "ASCENDING"
  }
}

# Composite index: query events by type, ordered by timestamp (collection group scope)
resource "google_firestore_index" "events_type_timestamp" {
  # description: collection group index for querying events by type and recency
  project     = var.project_id
  database    = google_firestore_database.default.name
  collection  = "events"
  query_scope = "COLLECTION_GROUP"

  fields {
    field_path = "event_type"
    order      = "ASCENDING"
  }
  fields {
    field_path = "timestamp"
    order      = "DESCENDING"
  }
}
