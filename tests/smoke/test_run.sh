#!/bin/bash
# Smoke test: granicus run --test with small asset subset
# Requires BQ credentials (must run as bqservice or with GOOGLE_APPLICATION_CREDENTIALS)
set -euo pipefail

ANALYTEHEALTH_ROOT="${ANALYTEHEALTH_ROOT:-/opt/projects/AnalyteHealth}"
PIPELINE_DIR="$ANALYTEHEALTH_ROOT/project/granicus_pipeline/analyte_health"
ASSETS="stg_orders"

echo "=== Smoke test: granicus run --test ==="
echo "ANALYTEHEALTH_ROOT=$ANALYTEHEALTH_ROOT"
echo "PIPELINE_DIR=$PIPELINE_DIR"
echo "ASSETS=$ASSETS"

if [ ! -f "$PIPELINE_DIR/pipeline.yaml" ]; then
    echo "FAIL: pipeline.yaml not found at $PIPELINE_DIR"
    exit 1
fi

granicus run "$PIPELINE_DIR/pipeline.yaml" \
    --project-root "$PIPELINE_DIR" \
    --test \
    --test-window 7d \
    --assets "$ASSETS"
status=$?

if [ $status -eq 0 ]; then
    echo "PASS: granicus run --test exited 0"
else
    echo "FAIL: granicus run --test exited $status"
    exit $status
fi

# Verify state was recorded
STATE_DB="$PIPELINE_DIR/.granicus/test-state.db"
if [ -f "$STATE_DB" ]; then
    echo "PASS: test-state.db exists at $STATE_DB"
else
    echo "WARN: test-state.db not found (may use different path)"
fi
