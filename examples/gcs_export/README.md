# GCS Export Example

Demonstrates exporting BigQuery data to Google Cloud Storage using the `gcs_export` asset type.

## Pipeline

1. `stg_orders` — SQL asset that stages order data in BigQuery
2. `export_daily_orders` — GCS export that runs a shell script to extract data to GCS

## GCS Connection Properties

| Property | Description |
|----------|-------------|
| `bucket` | GCS bucket name (required) |
| `prefix` | Path prefix within the bucket |
| `format` | Output format: `parquet`, `csv`, or `json` |
| `partition_prefix` | When `"true"`, appends `dt=YYYY-MM-DD` to the output path |
| `credentials` | Path to service account JSON (optional, falls back to ADC) |

## Environment Variables

The GCS runner passes these to your export script:

- `GRANICUS_GCS_BUCKET` — bucket name
- `GRANICUS_GCS_PREFIX` — path prefix
- `GRANICUS_GCS_FORMAT` — output format
- `GRANICUS_GCS_PARTITION_PREFIX` — partition prefix setting
- `GRANICUS_INTERVAL_START` / `GRANICUS_INTERVAL_END` — interval bounds (if incremental)

## Running

```bash
granicus run pipeline.yaml
```
