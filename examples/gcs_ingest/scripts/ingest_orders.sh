#!/usr/bin/env bash
# GCS ingest script — receives env vars from Granicus GCS ingest runner:
#   GRANICUS_GCS_BUCKET, GRANICUS_GCS_PREFIX, GRANICUS_GCS_FILE_PATTERN,
#   GRANICUS_GCS_FORMAT, GRANICUS_GCS_LOAD_METHOD, GRANICUS_GCS_ARCHIVE_PREFIX,
#   GRANICUS_DEST_PROJECT, GRANICUS_DEST_DATASET
set -euo pipefail

BUCKET="${GRANICUS_GCS_BUCKET}"
PREFIX="${GRANICUS_GCS_PREFIX}"
PATTERN="${GRANICUS_GCS_FILE_PATTERN:-*.csv}"
FORMAT="${GRANICUS_GCS_FORMAT:-csv}"
LOAD_METHOD="${GRANICUS_GCS_LOAD_METHOD:-append}"
ARCHIVE_PREFIX="${GRANICUS_GCS_ARCHIVE_PREFIX:-}"
DEST_PROJECT="${GRANICUS_DEST_PROJECT}"
DEST_DATASET="${GRANICUS_DEST_DATASET}"

# List matching files
FILES=$(gsutil ls "gs://${BUCKET}/${PREFIX}/${PATTERN}" 2>/dev/null || true)
FILE_COUNT=$(echo "$FILES" | grep -c "^gs://" || true)

if [ "$FILE_COUNT" -eq 0 ]; then
    echo "No files matching ${PATTERN} in gs://${BUCKET}/${PREFIX}"
    echo "GRANICUS_META:file_count=0"
    echo "GRANICUS_META:row_count=0"
    exit 0
fi

echo "Found ${FILE_COUNT} file(s) to ingest"

WRITE_DISPOSITION="WRITE_APPEND"
if [ "$LOAD_METHOD" = "replace" ]; then
    WRITE_DISPOSITION="WRITE_TRUNCATE"
fi

TOTAL_ROWS=0
for FILE in $FILES; do
    echo "Loading ${FILE}..."
    bq load \
        --source_format="${FORMAT^^}" \
        --write_disposition="${WRITE_DISPOSITION}" \
        "${DEST_PROJECT}:${DEST_DATASET}.orders" \
        "${FILE}"

    if [ -n "$ARCHIVE_PREFIX" ]; then
        ARCHIVE_DEST="gs://${BUCKET}/${ARCHIVE_PREFIX}/$(basename "$FILE")"
        gsutil mv "$FILE" "$ARCHIVE_DEST"
        echo "Archived to ${ARCHIVE_DEST}"
    fi
done

echo "GRANICUS_META:file_count=${FILE_COUNT}"
echo "GRANICUS_META:load_method=${LOAD_METHOD}"
echo "Ingest complete"
