#!/usr/bin/env bash
# GCS export script — receives env vars from Granicus GCS runner:
#   GRANICUS_GCS_BUCKET, GRANICUS_GCS_PREFIX, GRANICUS_GCS_FORMAT,
#   GRANICUS_GCS_PARTITION_PREFIX, GRANICUS_INTERVAL_START, GRANICUS_INTERVAL_END
set -euo pipefail

DEST="gs://${GRANICUS_GCS_BUCKET}/${GRANICUS_GCS_PREFIX}"

if [ "${GRANICUS_GCS_PARTITION_PREFIX:-}" = "true" ] && [ -n "${GRANICUS_INTERVAL_START:-}" ]; then
    DATE_PART=$(echo "$GRANICUS_INTERVAL_START" | cut -d'T' -f1)
    DEST="${DEST}/dt=${DATE_PART}"
fi

FORMAT="${GRANICUS_GCS_FORMAT:-parquet}"

echo "Exporting to ${DEST} as ${FORMAT}"

bq extract \
    --destination_format="${FORMAT^^}" \
    "my-gcp-project:analytics.stg_orders" \
    "${DEST}/stg_orders_*.${FORMAT}"

echo "Export complete"
