# Pub/Sub topics and subscriptions for Granicus event streaming.
# These have not been provisioned yet. Uncomment and apply when ready.

# resource "google_pubsub_topic" "events" {
#   # description: Pub/Sub topic for Granicus run and node lifecycle events
#   name    = "granicus-events"
#   project = var.project_id
#
#   message_storage_policy {
#     allowed_persistence_regions = [var.region]
#   }
#
#   labels = {
#     service = "granicus"
#     env     = "dev"
#   }
# }
#
# resource "google_pubsub_topic" "results" {
#   # description: Pub/Sub topic for Granicus pipeline result envelopes
#   name    = "granicus-results"
#   project = var.project_id
#
#   message_storage_policy {
#     allowed_persistence_regions = [var.region]
#   }
#
#   labels = {
#     service = "granicus"
#     env     = "dev"
#   }
# }
#
# resource "google_pubsub_subscription" "events_vm" {
#   # description: VM-side subscription for consuming Granicus lifecycle events
#   name    = "granicus-events-vm"
#   project = var.project_id
#   topic   = google_pubsub_topic.events.id
#
#   ack_deadline_seconds       = 60
#   message_retention_duration = "604800s" # 7 days
#   enable_message_ordering    = true
#
#   labels = {
#     service = "granicus"
#     env     = "dev"
#   }
# }
#
# resource "google_pubsub_subscription" "results_engine" {
#   # description: Engine-side subscription for consuming pipeline result envelopes
#   name    = "granicus-results-engine"
#   project = var.project_id
#   topic   = google_pubsub_topic.results.id
#
#   ack_deadline_seconds       = 120
#   message_retention_duration = "86400s" # 1 day
#   enable_message_ordering    = true
#
#   labels = {
#     service = "granicus"
#     env     = "dev"
#   }
# }
