# Granicus

A lightweight Go pipeline orchestrator for SQL and script-based data pipelines. Granicus builds a DAG from dependency directives embedded in SQL file comments, runs assets with configurable parallelism, and tracks incremental state in SQLite.

## Install

Requires Go 1.25+.

```bash
go build -o granicus ./cmd/granicus
```

## Quickstart

1. Create a pipeline config (`pipeline.yaml`):

```yaml
pipeline: my_pipeline
max_parallel: 4

connections:
  warehouse:
    type: bigquery
    project: my-gcp-project
    dataset: my_dataset
    credentials_file: /path/to/service-account.json

assets:
  - name: raw_orders
    type: sql
    source: sql/raw_orders.sql
    destination_connection: warehouse

  - name: order_summary
    type: sql
    source: sql/order_summary.sql
    destination_connection: warehouse
```

2. Add directives to your SQL files as comment blocks. Granicus parses a `granicus:` YAML block from the leading comments of each file:

```sql
-- granicus:
--   depends_on:
--     - raw_orders
--   layer: intermediate

SELECT
  order_id,
  SUM(amount) AS total
FROM raw_orders
GROUP BY order_id
```

3. Validate and run:

```bash
granicus validate pipeline.yaml --project-root .
granicus run pipeline.yaml --project-root .
```

## Architecture

```
SQL/Python files          pipeline.yaml
       |                       |
       v                       v
   directives parser      config loader
       |                       |
       +----------+------------+
                  |
                  v
          graph (DAG builder)
                  |
                  v
         executor (parallel DAG walk)
                  |
                  v
     runners (sql, python, shell, dlt, sql_check)
                  |
                  v
         state store (SQLite)
```

- **Graph**: Parses directives from SQL/Python file comments, resolves dependencies, detects cycles, and builds a DAG with topological ordering.
- **Executor**: Walks the DAG with bounded parallelism (semaphore). On failure, all downstream nodes are skipped. Supports incremental execution with interval tracking and automatic retries for rate-limit errors.
- **Runners**: Pluggable execution backends -- `sql` (BigQuery, Postgres, etc.), `python`, `shell`, `dlt`, `sql_check`. Registered per connection type.
- **State store**: SQLite database (`.granicus/state.db`) tracking interval completion for incremental assets. Supports mark-in-progress, mark-complete, mark-failed transitions.

## Directives

Directives are embedded in SQL or Python file comments as a YAML block under the `granicus:` key. SQL uses `--` prefixes, Python/shell uses `#` prefixes.

```sql
-- granicus:
--   depends_on:
--     - upstream_asset
--   time_column: created_at
--   interval_unit: day
--   start_date: "2024-01-01"
--   lookback: 3
--   batch_size: 30
--   layer: staging
--   grain: order
--   produces:
--     - output_a
--     - output_b
--   default_checks: true
```

| Directive | Description |
|---|---|
| `depends_on` | List of upstream asset names this asset depends on |
| `time_column` | Column used for incremental processing |
| `interval_unit` | Interval granularity: `day`, `month`, etc. |
| `start_date` | Earliest date for incremental backfill |
| `lookback` | Number of past intervals to re-process each run |
| `batch_size` | Max intervals per execution |
| `layer` | Asset layer: `staging`, `intermediate`, `entity`, `report` |
| `grain` | Entity grain (used for default check generation) |
| `produces` | Multi-output: list of assets produced by one source file |
| `default_checks` | Enable/disable auto-generated checks for this asset |

## Config Reference

Pipeline config is a YAML file with the following fields:

```yaml
pipeline: string           # Required. Pipeline name.
schedule: string           # Optional. Cron expression for scheduled execution.
max_parallel: int          # Optional. Max concurrent assets (default: 10).

connections:               # Connection definitions, keyed by name.
  <name>:
    type: string           # bigquery, postgres, mysql, snowflake, gcs, s3, iceberg
    project: string        # Connection-specific properties (inline).
    dataset: string
    credentials_file: string
    # ... other properties depending on type

assets:                    # Required. At least one asset.
  - name: string           # Optional. Defaults to source filename without extension.
    type: string           # Required. sql, python, shell, or dlt.
    source: string         # Required. Path to source file (relative to project root).
    destination_connection: string  # Required for sql assets. Connection name for output.
    source_connection: string       # Optional. Connection name for input.
    layer: string          # Optional. staging, intermediate, entity, or report.
    grain: string          # Optional. Entity grain.
    partition_by: string   # Optional. Column to partition by.
    partition_type: string # Optional. DAY, HOUR, MONTH, or YEAR.
    cluster_by: [string]   # Optional. Columns to cluster by.
    default_checks: bool   # Optional. Enable/disable default checks.
    checks:                # Optional. Explicit check definitions.
      - name: string
        type: string       # sql_check or python_check
        source: string     # Path to check SQL/Python file.
```

## CLI Reference

### `run <config.yaml>`

Execute a pipeline.

```
--project-root string    Project root directory (default ".")
--max-parallel int       Override max_parallel from config
--assets string          Run only these assets and their dependencies (comma-separated)
--from-failure string    Re-run from a failed run ID (skips already-succeeded assets)
--from-date string       Override start_date for incremental assets (YYYY-MM-DD)
--to-date string         Override end date for incremental assets (YYYY-MM-DD)
--full-refresh           Invalidate interval state and reprocess from start
--test                   Run in test mode (creates temporary dataset)
--test-window string     Test window duration (e.g., 7d, 4w, 3m)
--keep-test-data         Preserve test dataset after run
```

### `validate <config.yaml>`

Validate pipeline config, dependency graph, and source file existence.

```
--project-root string    Project root directory (default ".")
```

### `status [run_id]`

Show status of a run. Defaults to the most recent run if no ID is given.

```
--project-root string    Project root directory (default ".")
```

### `history`

List recent pipeline runs.

```
--limit int              Number of runs to show (default 10)
--project-root string    Project root directory (default ".")
```

### `serve`

Start the scheduler and HTTP trigger server. Runs pipelines on their configured cron schedules and accepts HTTP trigger requests.

```
--config-dir string      Directory containing pipeline YAML configs (required)
--server-config string   Path to granicus-server.yaml
--env-config string      Path to granicus-env.yaml
--env string             Environment name (default "dev")
--project-root string    Project root directory (default ".")
```

The server exposes `/api/v1/health`, `/metrics` (Prometheus), and pipeline trigger endpoints. Supports API key authentication and pipeline concurrency locking.

### `gc`

Clean up old run logs and test artifacts.

```
--retention-days int     Delete runs older than this many days (default 30)
--project-root string    Project root directory (default ".")
```

### `backup`

Backup the state store (`state.db`).

```
--output string          Output path (default: alongside state.db)
--keep int               Number of backups to retain (default 7)
--project-root string    Project root directory (default ".")
```

### `version`

Print the current version.

## Multi-Pipeline

Use a `granicus.yaml` file to manage multiple pipelines together:

```yaml
pipelines:
  - config: pipelines/ingest.yaml
  - config: pipelines/transform.yaml
  - config: pipelines/reports.yaml

cross_dependencies:
  - upstream: ingest
    downstream: transform
    type: blocks
  - upstream: transform
    downstream: reports
    type: freshness
```

Pipeline config paths are resolved relative to the `granicus.yaml` file. Cross-dependencies define ordering between pipelines: `blocks` prevents the downstream pipeline from starting until the upstream completes, while `freshness` is advisory.

## Test Mode

The `--test` flag creates a temporary BigQuery dataset, runs the pipeline against it, and tears it down afterward.

```bash
# Run with a 7-day data window
granicus run pipeline.yaml --test --test-window 7d

# Keep the test dataset for inspection
granicus run pipeline.yaml --test --test-window 4w --keep-test-data
```

Test mode uses a separate state database (`test-state.db`) and limits the number of incremental intervals processed to reduce cost. The test window controls how far back data is loaded (`7d` = 7 days, `4w` = 4 weeks, `3m` = 3 months).
