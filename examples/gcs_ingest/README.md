# GCS Ingest Example

Demonstrates loading files from Google Cloud Storage into BigQuery using the `gcs_ingest` asset type.

## Pipeline

`ingest_order_files` — Watches a GCS bucket for CSV files, loads them into BigQuery, and archives processed files.

## GCS Connection Properties (Source)

| Property | Description |
|----------|-------------|
| `bucket` | GCS bucket name (required) |
| `prefix` | Path prefix to watch |
| `file_pattern` | Glob pattern for matching files (e.g., `*.csv`) |
| `format` | File format: `csv`, `parquet`, or `json` |
| `load_method` | How to load: `append`, `replace`, or `merge` |
| `archive_prefix` | Move processed files here (omit to leave in place) |
| `credentials` | Path to service account JSON (optional, falls back to ADC) |

## Polling

Set `poll_interval` on the asset (cron syntax) to have the scheduler check for new files automatically in server mode:

```yaml
poll_interval: "*/15 * * * *"  # every 15 minutes
```

Manual CLI trigger works like any other asset:

```bash
granicus run pipeline.yaml --assets ingest_order_files
```

## Metadata

The ingest script outputs metadata via stdout lines prefixed with `GRANICUS_META:`:

```
GRANICUS_META:file_count=5
GRANICUS_META:load_method=append
```

These are captured in the run result metadata.

## Running

```bash
granicus run pipeline.yaml
```
