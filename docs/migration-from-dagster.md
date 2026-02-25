# Migrating from Dagster to Granicus

This guide covers how to move existing Dagster pipelines to Granicus. Granicus is a lightweight, file-based pipeline orchestrator -- assets are SQL/Python/Shell files configured through YAML, not Python decorator graphs.

## Concept Mapping

| Dagster | Granicus | Notes |
|---------|----------|-------|
| `@asset` function | SQL/Python/Shell file + YAML entry | Each asset is a standalone file registered in `pipeline.yaml` |
| `@op` | N/A | Assets are the unit of work. There are no sub-asset operations. |
| Resource | Connection | Defined in `pipeline.yaml` under `connections:`. Supports `bigquery`, `postgres`, `mysql`, `snowflake`, `gcs`, `s3`, `iceberg`. |
| `IOManager` | N/A | Runners handle I/O automatically based on connection type. SQL runner reads/writes via the destination connection. |
| Sensor | Schedule (cron) or HTTP trigger | Set `schedule:` in `pipeline.yaml` for cron, or use `granicus serve` and hit the HTTP trigger endpoint. |
| Partition (time-based) | Interval directives | Set `time_column`, `interval_unit`, `start_date`, and optionally `lookback` / `batch_size` in the SQL directive block. |
| `@asset_check` | Check (`sql_check` or `python_check`) | Defined per-asset in `pipeline.yaml` under `checks:`, or auto-generated from `layer` and `grain`. |
| Job | Pipeline | A `pipeline.yaml` file is one pipeline. Run it with `granicus run`. |
| Code location | `pipeline.yaml` | One config file declares the full graph: connections, assets, checks, schedule. |
| Dagster UI | CLI + logs + `/api/v1/health` | `granicus status`, `granicus history`, log files in `.granicus/runs/`. |
| `dagster asset materialize` | `granicus run` | Use `--assets` to filter to specific assets. |
| Backfill | `granicus run --from-date / --to-date` | Or `--full-refresh` to reprocess from `start_date`. |

## Step-by-Step Migration

### 1. Extract SQL into a file

Dagster assets typically embed SQL in Python. Pull the SQL out into a standalone `.sql` file.

**Before** -- Dagster asset with inline SQL:

```python
from dagster import asset

@asset(
    deps=["raw_orders"],
    metadata={"schema": "staging"},
)
def stg_orders(context, bq: BigQueryResource):
    sql = """
        SELECT
            order_id,
            customer_id,
            order_date,
            status,
            total_amount
        FROM `project.raw.orders`
        WHERE order_date >= @start_date
          AND order_date < @end_date
    """
    bq.execute(sql, {"start_date": context.partition_key, ...})
```

**After** -- `sql/staging/stg_orders.sql`:

```sql
-- granicus:
--   depends_on:
--     - raw_orders
--   time_column: order_date
--   interval_unit: day
--   start_date: "2023-01-01"
--   layer: staging

SELECT
    order_id,
    customer_id,
    order_date,
    status,
    total_amount
FROM `project.raw.orders`
WHERE order_date >= @interval_start
  AND order_date < @interval_end
```

Key changes:
- Dependencies move from `deps=[]` to `-- depends_on:` in the directive block.
- Partition logic becomes `time_column` + `interval_unit`. The runner injects `@interval_start` and `@interval_end` parameters automatically.
- Layer metadata moves into the directive block or into `pipeline.yaml`.

### 2. Add the Granicus directive block

The directive block is a YAML structure embedded in SQL comments at the top of the file (first 50 lines). It must start with `-- granicus:` and use `--` prefixed lines for the body.

Available directives:

| Directive | Type | Purpose |
|-----------|------|---------|
| `depends_on` | list | Upstream asset names |
| `time_column` | string | Column used for incremental processing |
| `interval_unit` | string | `day`, `hour`, `month`, `year` |
| `start_date` | string | Earliest date to process (`YYYY-MM-DD`) |
| `lookback` | int | Number of extra intervals to re-process |
| `batch_size` | int | Intervals per execution batch |
| `layer` | string | `staging`, `intermediate`, `entity`, or `report` |
| `grain` | string | Entity grain (enables default uniqueness checks) |
| `default_checks` | bool | Enable/disable auto-generated checks |
| `partition_by` | string | BigQuery table partitioning column |
| `partition_type` | string | `DAY`, `HOUR`, `MONTH`, or `YEAR` |
| `cluster_by` | list | BigQuery clustering columns |
| `produces` | list | Output names if asset produces multiple tables |

### 3. Create pipeline.yaml

Register the asset and its connection in a pipeline config file.

```yaml
pipeline: my_pipeline
schedule: "0 6 * * *"    # optional: cron for daily 6am run
max_parallel: 4

connections:
  warehouse:
    type: bigquery
    project: my-gcp-project
    dataset: analytics

assets:
  - name: stg_orders
    type: sql
    source: sql/staging/stg_orders.sql
    destination_connection: warehouse
    layer: staging
```

Connection properties vary by type:

| Connection type | Required properties |
|----------------|-------------------|
| `bigquery` | `project`, `dataset` |
| `postgres` | `host`, `database` |
| `mysql` | `host`, `database` |
| `snowflake` | `account`, `database` |

Asset types: `sql`, `python`, `shell`, `dlt`.

SQL assets must have a `destination_connection`. Python, shell, and dlt assets can optionally reference `source_connection` and `destination_connection` which are passed as environment variables.

### 4. Add checks (optional)

Checks run after an asset completes. Define them inline in `pipeline.yaml`:

```yaml
assets:
  - name: stg_orders
    type: sql
    source: sql/staging/stg_orders.sql
    destination_connection: warehouse
    layer: staging
    grain: order_id
    checks:
      - name: stg_orders_no_nulls
        type: sql_check
        source: checks/stg_orders_no_nulls.sql
```

If you set `layer` and `grain`, Granicus auto-generates default checks (e.g., uniqueness on the grain column). Disable with `default_checks: false`.

### 5. Validate

```bash
granicus validate pipeline.yaml
```

This checks:
- Config structure and required fields
- All source files exist
- Dependency graph has no cycles
- All `depends_on` references resolve to known assets
- Connection references are valid

### 6. Run

```bash
# Run the full pipeline
granicus run pipeline.yaml

# Run specific assets (and their dependencies)
granicus run pipeline.yaml --assets stg_orders,stg_customers

# Backfill a date range
granicus run pipeline.yaml --from-date 2024-01-01 --to-date 2024-06-01

# Full refresh (reprocess from start_date)
granicus run pipeline.yaml --full-refresh

# Re-run from a previous failure
granicus run pipeline.yaml --from-failure <run_id>

# Test mode (creates temporary dataset, processes recent window)
granicus run pipeline.yaml --test --test-window 7d
```

## What Dagster Has That Granicus Replaces

### Web UI -> CLI + logs

Dagster has a full web UI for browsing assets, viewing runs, and launching materializations. Granicus replaces this with:

- `granicus status [run_id]` -- show run results, failed/skipped nodes
- `granicus history --limit 20` -- list recent runs
- `granicus serve` -- starts an HTTP server with `/api/v1/health` and trigger endpoints
- Run logs stored in `.granicus/runs/<run_id>/` as structured JSON

### Complex Python logic -> Python runner

If your Dagster asset does more than run SQL (API calls, file processing, custom transformations), use the `python` asset type:

```yaml
assets:
  - name: fetch_api_data
    type: python
    source: scripts/fetch_api_data.py
    destination_connection: warehouse
```

The Python script receives connection details as environment variables and is responsible for its own I/O.

### Dagster schedules/sensors -> cron + HTTP triggers

Dagster sensors that poll for new data have no direct equivalent. Options:
- Use cron schedules in `pipeline.yaml` with `granicus serve` running as a daemon.
- Use external cron/systemd timers to call `granicus run`.
- Hit the HTTP trigger endpoint from an external webhook or monitoring system.

### Dagster partitions -> interval state

Dagster's partition system tracks which partitions have been materialized. Granicus tracks this in `.granicus/state.db` -- a SQLite database that records the last-processed interval per asset. The `--full-refresh` flag resets this state.

## Worked Example

Migrating a Dagster asset that deduplicates patient records from a raw table.

### Before: Dagster

```python
from dagster import asset, DailyPartitionsDefinition

@asset(
    deps=["raw_patients"],
    partitions_def=DailyPartitionsDefinition(start_date="2023-06-01"),
    metadata={"schema": "intermediate"},
)
def int_patients_deduped(context, bq: BigQueryResource):
    start = context.partition_key
    end = context.partition_time_window.end.strftime("%Y-%m-%d")

    sql = f"""
        SELECT
            patient_id,
            first_name,
            last_name,
            date_of_birth,
            created_at,
            ROW_NUMBER() OVER (
                PARTITION BY patient_id ORDER BY created_at DESC
            ) AS rn
        FROM `myproject.raw.patients`
        WHERE created_at >= '{start}'
          AND created_at < '{end}'
        QUALIFY rn = 1
    """
    result = bq.query(sql)
    bq.load_table(result, "myproject.intermediate.int_patients_deduped",
                   write_disposition="WRITE_TRUNCATE",
                   time_partitioning={"field": "created_at", "type": "DAY"})
```

### After: Granicus

**`sql/intermediate/int_patients_deduped.sql`**:

```sql
-- granicus:
--   depends_on:
--     - raw_patients
--   time_column: created_at
--   interval_unit: day
--   start_date: "2023-06-01"
--   layer: intermediate
--   partition_by: created_at
--   partition_type: DAY

SELECT
    patient_id,
    first_name,
    last_name,
    date_of_birth,
    created_at,
    ROW_NUMBER() OVER (
        PARTITION BY patient_id ORDER BY created_at DESC
    ) AS rn
FROM `myproject.raw.patients`
WHERE created_at >= @interval_start
  AND created_at < @interval_end
QUALIFY rn = 1
```

**`pipeline.yaml`** (relevant section):

```yaml
pipeline: patient_pipeline
max_parallel: 4

connections:
  warehouse:
    type: bigquery
    project: myproject
    dataset: intermediate

assets:
  - name: int_patients_deduped
    type: sql
    source: sql/intermediate/int_patients_deduped.sql
    destination_connection: warehouse
```

**Validate and run**:

```bash
granicus validate pipeline.yaml
granicus run pipeline.yaml --assets int_patients_deduped
```

The SQL runner handles:
- Replacing `@interval_start` and `@interval_end` with the correct timestamps
- Writing results to the destination table
- Applying BigQuery partitioning and clustering settings from the directives
- Tracking interval state so the next run picks up where it left off
