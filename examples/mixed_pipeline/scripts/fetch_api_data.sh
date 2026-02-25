#!/bin/bash
# granicus:
#   produces: [raw_api_events]

echo "Fetching events from API..."
mkdir -p /tmp/mixed_pipeline
cat > /tmp/mixed_pipeline/raw_events.json <<'JSON'
[
  {"event_id": "e1", "type": "click", "ts": "2026-02-25T10:00:00Z", "user": "u1"},
  {"event_id": "e2", "type": "view",  "ts": "2026-02-25T10:05:00Z", "user": "u2"},
  {"event_id": "e3", "type": "click", "ts": null, "user": "u1"}
]
JSON
echo "Fetched 3 events to /tmp/mixed_pipeline/raw_events.json"
