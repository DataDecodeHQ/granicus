# Lightweight Asset-Oriented Orchestrator

## The Problem

Dagster's execution model introduces significant overhead per step (10-15 seconds of framework boot, process spin-up, event logging) and limited parallelism. Basic pipelines that should run in 3-5 minutes take 30-40 minutes. As automated checks increase — especially LLM-driven validation — this problem gets exponentially worse.

The data engineering ecosystem has a gap: heavy frameworks (Dagster, Airflow) that do everything slowly, and lightweight tools (dbt, Hamilton) that only do one thing. Nobody built the thin, fast, asset-aware orchestration layer in the middle.

## Core Design Principles

- **Asset-oriented, not workflow-oriented.** The primary objects are datasets (nouns), not tasks (verbs). The system tracks what data exists, when it was last updated, and whether it's correct. The process that produces the data is secondary.
- **Maximum parallelism by default.** Run everything whose dependencies are satisfied, immediately. Concurrency is bounded by external resources (DB connections, API rate limits), not the orchestrator.
- **Independent failure.** If a node fails, mark it failed, skip its descendants, keep running everything else. No partial failure hacks, no "catch and pretend it succeeded."
- **Near-zero orchestrator overhead.** The executor adds milliseconds, not seconds. No process-per-step. No framework boot per node. No daemon ticking every 30 seconds.
- **Heterogeneous execution.** SQL, Python, API calls — the orchestrator doesn't care. It dispatches work and waits for results.

## Asset vs. Workflow: Why It Matters

**Workflow/task-oriented** systems are *process first*. They track whether steps executed correctly, in the right order, with the right inputs. The data is a side effect. Good for: agent orchestration, payment flows, business process automation — anywhere the *process completing correctly* is the point.

**Asset-oriented** systems are *state first*. They track what data exists, whether it's current, whether it's correct, and what produced it. When data is the primary object, you can attach checks, track history, detect staleness, and trace lineage natively. Data quality becomes a natural consequence of the model. Good for: data pipelines, analytics, ML feature stores — anywhere the *data being right* is the point.

## What to Build

### 1. Graph Definition Layer

Assets declare their dependencies in the source files themselves — not in the config JSON. SQL files use a comment convention (`-- depends_on: raw_transactions`), Python files use a similar comment or decorator. The orchestrator parses source files to build the dependency graph automatically. This prevents config drift — the graph is always derived from the code.

The config JSON handles only orchestrator metadata: asset name (or inferred from filename), type (SQL/Python/dlt — or inferred from file extension), destination, layer, and check overrides. Dependencies come from the code.

### 2. DAG Executor

Walks the graph, resolves dependencies, dispatches everything that's ready immediately. High concurrency bounded by external resources, not the orchestrator. Independent failure — node fails, descendants are skipped, everything else keeps going. Checks run automatically after their asset materializes, same execution model.

### 3. Node Runners

Execute the actual work: run SQL against a warehouse, call a Python function, hit an API. Capture: start time, end time, success/fail, error message, metadata (row count, whatever the node wants to report). The executor doesn't care what kind of work it is — it dispatches and waits for a result.

### 4. Structured Log Store

Every execution writes a consistent record: asset ID, run ID, timestamp, status, duration, metadata, error. Queryable — answer "what failed in the last run," "when did this asset last succeed," "show me everything that ran today." This is the observability layer. Dashboards, alerting, and debugging all read from this.

### 5. Trigger System

- **Cron schedules** — run this subgraph on a schedule
- **Webhooks/listeners** — new file landed, API event received, kick off a subgraph
- **Manual** — CLI command to run a subgraph or re-run from a failed node

### 6. CLI

- Submit a run (full graph or subgraph)
- Re-run from failure (reads last run from log store, starts from failed nodes)
- Query status (what's running, what failed, asset history)
- Validate graph (check for cycles, missing dependencies)

## What's Not Needed

### Type System
Dagster lets you define types for data flowing between assets (e.g., "this outputs a DataFrame with columns X, Y, Z"). In practice almost nobody uses it — checks do the same job better, and most validation happens at the warehouse level. The checks layer covers data validation without the framework overhead.

### Auto-Materialization Policies
Dagster's declarative scheduling: "materialize this asset whenever its parents are fresh" or "never be more than 2 hours stale." Cool idea, hard to predict, hard to debug, sluggish due to daemon tick rates. Explicit triggers (cron + webhooks + event listeners) are easier to reason about than "run when the framework decides conditions are met."

### Resource Framework
Dagster's dependency injection system for DB connections, API clients, etc. with per-environment config swapping. It's just dependency injection — solvable with environment variables, a config file, and a factory function. A simple config loader is all that's needed.

### Heavy UI
Good logs and structured data in a queryable store are sufficient. Grafana, a simple CLI, or a lightweight custom page over the log store replaces Dagit. Build a UI later if needed.

### IO Managers
Nodes read and write their own data. No framework abstraction over data persistence.

## Incremental Loading and Interval Tracking

Borrowed from SQLMesh's approach, which handles incrementals more cleanly than dbt.

**How it works:**

- Assets declare their `time_column` and optionally their grain/partition key in the source file or config.
- The executor automatically injects time range filters (`WHERE time_column BETWEEN @start AND @end`) at runtime. The model SQL always looks the same whether it's a backfill or an incremental run — no `is_incremental()` branching.
- A **state store tracks which intervals have been processed** per asset. The executor knows "I've processed Jan 1-15 but not Jan 16" and can detect and fill gaps, not just "grab everything after the last timestamp."
- **Automatic partitioning**: if the destination supports it (BigQuery, Databricks), tables are partitioned on the time column automatically.
- **Lookback parameter**: assets can declare a lookback (e.g., `lookback: 3`) meaning "always reprocess the last N intervals" to catch late-arriving data.

```sql
-- model: stg_transactions
-- depends_on: raw_transactions
-- time_column: created_at
-- grain: transaction_id
-- lookback: 2

SELECT
    transaction_id,
    customer_id,
    amount,
    created_at
FROM raw_transactions
WHERE created_at BETWEEN @start AND @end
```

The executor manages the interval logic. The node runner injects `@start` and `@end` values. The model author just writes clean SQL with a WHERE clause.

## Multi-Output Assets

An asset can produce multiple outputs. A Python script might write 3 tables, or a dlt pipeline might create several destinations.

Dependencies declared in the source file:

```python
# -- produces: table_a, table_b, table_c
# -- depends_on: raw_events
```

The graph tracks each output as a separate node that happens to be produced by the same execution unit. Downstream assets can depend on any individual output. If `table_a` fails but `table_b` and `table_c` succeed, that's the independent failure model working — dependents of `table_b` and `table_c` keep running.

## Language Agnosticism

Since the orchestrator reads a config and dispatches work to node runners, the actual code can be written in any language. The contract is simple: take data in, push data out, return a status.

- **SQL** → send to warehouse, capture row count and status
- **Python** → run the script, capture stdout/stderr and exit code
- **Go / Rust / Java** → run the binary, same contract
- **dlt** → invoke the dlt pipeline, capture its output
- **R / Julia / whatever** → same pattern

Dagster can't do this because everything must be wrapped in Python decorators. dbt can't because everything must be SQL/Jinja. This orchestrator is language-agnostic by design because the execution boundary is the config + the file, not a framework API. If it can read data and write data, it can be an asset.

## Connectors (Non-Default, Opt-In)

The orchestrator doesn't natively embed connectors. Node runners handle execution against external systems based on config. Supported targets:

- **BigQuery** — SQL execution and write destinations
- **GCS / Hetzner Cloud Storage** — File read/write for extracts, loads, intermediate artifacts
- **Apache Iceberg** — Read and write Iceberg tables as assets
- **dlt** — Fire dlt pipelines as a node type (the orchestrator triggers dlt, dlt handles the extraction/loading)

A node declares its type and destination in config. The node runner knows how to execute against that target. Adding a new connector means adding a new node runner — the orchestrator itself doesn't change.

### Source and Destination Connections

Assets can read from one system and write to another. The config supports separate `source_connection` and `destination_connection` fields for cross-system data movement (e.g., Iceberg → BigQuery, Postgres → GCS, API → DuckDB).

Connections are defined in a connections config:

```yaml
connections:
  iceberg_lakehouse:
    type: iceberg
    catalog: my_catalog
    warehouse: s3://my-bucket/warehouse

  bigquery_prod:
    type: bigquery
    project: my-project
    dataset: analytics_prod

  hetzner_storage:
    type: gcs_compatible
    endpoint: https://fsn1.your-objectstorage.com
    bucket: data-lake
```

Assets reference connections by name:

```yaml
assets:
  - name: customer_sync
    type: python
    source: scripts/sync_customers.py
    source_connection: iceberg_lakehouse
    destination_connection: bigquery_prod
    layer: source
```

The node runner injects both connection configs as environment variables (or a config file path) so the script can read from one and write to the other. The orchestrator doesn't need to understand data formats or connection protocols — it just passes connection info to the node runner.

For simple read-from-A-write-to-B patterns, dlt is the ideal node type — a dlt script that reads Iceberg and writes BigQuery is about 10 lines of Python. For more complex transforms, Python or SQL nodes handle the logic with both connections available.

If only `destination_connection` is specified (or just `destination`), the asset reads from whatever its code references and writes to the declared destination — this is the common case for SQL transforms that read from the same warehouse they write to.

## Default Tests by Layer

Assets declare their layer (source, staging, intermediate, mart, etc.). Each layer gets default checks that run automatically after materialization:

- **Source** — not null on key columns, row count > 0, freshness (data arrived recently)
- **Staging** — unique keys, accepted values on known columns, schema matches expected
- **Intermediate / Mart** — referential integrity, row count ratios vs. upstream, no unexpected nulls

Defaults can be:

- **Turned off** per asset in config (`"default_checks": false`)
- **Extended** with custom checks stored in a standard `tests/` folder, referenced by asset name or layer

Custom checks follow the same interface as any check node: they receive asset metadata, run a validation, return pass/fail with a message.

## Pipeline Configuration

Each pipeline is defined by a YAML config file. Multiple pipelines = multiple config files.

Connections are defined at the top level or in a separate connections file, and referenced by name in assets:

```yaml
pipeline: revenue_daily
schedule: "0 3 * * *"
environment: prod
max_parallel: 10

connections:
  iceberg_lakehouse:
    type: iceberg
    catalog: my_catalog
    warehouse: s3://my-bucket/warehouse

  bigquery_prod:
    type: bigquery
    project: my-project
    dataset: analytics_prod

assets:
  - name: raw_transactions
    type: dlt
    source: scripts/extract_transactions.py
    source_connection: iceberg_lakehouse
    destination_connection: bigquery_prod
    layer: source
    default_checks: true

  - name: stg_transactions
    type: sql
    source: sql/stg_transactions.sql
    destination_connection: bigquery_prod
    layer: staging

  - name: revenue_summary
    type: python
    source: scripts/revenue_summary.py
    destination_connection: bigquery_prod
    layer: mart
    custom_checks: ["tests/revenue_bounds.py"]
```

Notes:
- `destination_connection` is the common case for SQL transforms that read and write within the same warehouse.
- `source_connection` + `destination_connection` is for cross-system movement (Iceberg → BigQuery, API → GCS, etc.).
- Dependencies are parsed from source files, not declared in config.
- `layer` determines which default checks apply.

A top-level config can reference multiple pipeline configs to run them together or define cross-pipeline dependencies.

## Environment-Based Destinations and Virtual Environments

Environments are defined in a separate config (or environment variables) that the pipeline config references:

```yaml
environments:
  dev:
    prefix: "dev_"
    database: analytics_dev
    destination_override: bigquery

  prod:
    prefix: ""
    database: analytics_prod
    destination_override: bigquery

  test:
    prefix: "test_"
    database: ":local:"
    destination_override: duckdb
```

Run with `--env dev` and all destinations get the dev prefix and database. No code changes, just config.

**Virtual Environments (borrowed from SQLMesh):** Rather than fully materializing separate copies of every table for dev and prod, use views as pointers to physical tables. Production is a set of views pointing to the current physical tables. Dev creates new physical tables only for assets that changed, and points everything else at the existing prod tables. Promoting to prod = swapping view pointers, no data movement. Rolling back = swapping them back. This keeps dev environments cheap and fast to spin up, and ensures what you tested in dev is exactly what runs in prod.

## Test Mode (DuckDB Diversion)

Run any pipeline or subgraph with a `--test` flag. The executor:

1. **Swaps all destinations to a local DuckDB.** The pipeline runs the same logic but writes locally instead of to the real destination.
2. **Runs the full pipeline (or specified subset).** You can test one asset or many: `orchestrator run --test --assets stg_transactions,revenue_summary`
3. **Executes all checks** against the DuckDB output — default layer checks plus any custom checks.
4. **Saves metadata** — what was tested, what passed/failed, row counts, schema diffs, value distributions, timing.
5. **Cleans up** — if all checks pass, deletes the DuckDB data. Metadata persists for audit.

The orchestrator's responsibility ends here. It handles destination swapping, execution, check running, and metadata capture. The *decision logic* about what constitutes an acceptable change (e.g., "row count shifted by less than 5%", "no new nulls in critical columns", "schema didn't change unexpectedly") lives outside the orchestrator — in custom checks, AI validation agents, or whatever testing framework wraps around it.

This enables a workflow where an AI agent modifies a model, triggers a `--test` run, reviews the metadata, and decides whether to promote the change — without the orchestrator needing to know anything about AI.

## Existing Tools and Their Gaps

| Tool | What it does well | Where it falls short |
|------|------------------|---------------------|
| **Dagster** | Asset model, checks, DX | Slow execution, high overhead per step, poor parallelism |
| **Prefect** | Lightweight execution, async | Workflow-oriented, no native asset graph or checks |
| **Temporal** | Incredible execution engine, durable, parallel | Workflow/activity model, no concept of assets or data lineage |
| **dbt** | Fast parallel SQL execution | SQL only — no Python in the graph |
| **Airflow** | Mature DAG executor | Task-oriented, similar overhead to Dagster |
| **Hamilton** | Lightweight Python DAG | Function-level DAG inside a single process, not an orchestration layer |
| **Dagu** | Go single-binary, YAML DAGs, parallel steps, no DB required | Workflow/task-oriented, no asset model, no data-awareness |
| **SQLMesh** | Incremental intervals, virtual environments, smart state tracking | SQL-focused transformation framework, not a general orchestrator |

The gap: a fast, parallel DAG executor that thinks in assets, supports heterogeneous execution (SQL + Python + whatever), has native checks, event-driven triggers, and good structured logging — without the framework bloat.

## Reference Implementations: What to Steal

### DAG Executor and Parallel Dispatch
- **Dagu** (Go): Single-binary DAG engine with YAML definitions, parallel step execution, `maxActiveSteps` concurrency control, lifecycle hooks (onSuccess, onFailure), retry with exponential backoff. Architecture: Scheduler → Agent → Executors → Storage Layer. File-based state with JSONL status files. Closest existing tool to the execution model we want. ([github.com/dagu-org/dagu](https://github.com/dagu-org/dagu))
- **go-dag** (Go): Minimal library — just graph + concurrent scheduler. Good reference for the core algorithm: topological sort, goroutine dispatch, dependency tracking via channels. ([github.com/AkihiroSuda/go-dag](https://github.com/AkihiroSuda/go-dag))
- **Goflow** (Go): Simple DAG scheduler with custom Operators (implement a `Run()` method), retry strategies, SSE streaming. Shows how to keep the operator interface minimal. ([github.com/fieldryand/goflow](https://github.com/fieldryand/goflow))

### Incremental Loading and State Tracking
- **SQLMesh**: The gold standard for interval-based incremental loading. Tracks which time intervals have been processed in a state store. Automatically injects `@start` / `@end` macro variables. Supports lookback for late-arriving data, batch_size for chunking large backfills, and gap detection. State stored in DB tables (Postgres, DuckDB, etc). ([sqlmesh.readthedocs.io](https://sqlmesh.readthedocs.io/en/stable/guides/incremental_time/))

### Virtual Environments and Dev/Prod Isolation
- **SQLMesh Virtual Data Environments**: Environments are collections of views pointing to physical snapshot tables. Creating a dev environment = cloning the view pointers. Promoting to prod = swapping view pointers (zero data movement). Each model version gets a fingerprinted physical table. Changed models get new snapshots; unchanged models reuse existing prod tables. ([tobikodata.com/blog/virtual-data-environments](https://www.tobikodata.com/blog/virtual-data-environments))

### Data Quality Checks
- **Soda Core / SodaCL**: YAML-based checks, pushes computation to the warehouse via SQL, lightweight CLI. Check syntax like `row_count > 0`, `missing_count(email) = 0`, `duplicate_count(id) = 0`. Closest to the "default checks by layer" pattern we want — simple, declarative, SQL-native. ([docs.soda.io](https://docs.soda.io))
- **dbt tests**: Convention-based: `unique`, `not_null`, `accepted_values`, `relationships` built-in. Custom tests as SQL queries returning failing rows. Shows how default checks per model type can work.
- **Great Expectations**: More powerful but heavier. Worth referencing for advanced statistical checks (distribution shifts, anomaly detection) that LLM agents might want to run later.

### Data Loading (EL)
- **dlt (data load tool)**: Python library for extract-load. Handles schema inference, incremental loading, normalization. Lightweight — no server, runs as a Python script. Supports 60+ sources out of the box. Our orchestrator fires dlt as a node type and captures its output. ([dlthub.com](https://dlthub.com))

### Structured Logging
- **Dagu's approach**: JSONL status files per run attempt, separate stdout/stderr logs per step, hierarchical directory structure by date. Simple, queryable, no database required for basic operation. Can be upgraded to Postgres/BigQuery for more complex querying.

## Internal Dependency Map

How the features we've outlined depend on each other:

```
Graph Definition Layer ─────────────────┐
  (must exist first, everything depends  │
   on knowing the graph)                 │
         │                               │
         ▼                               │
    DAG Executor ◄───────────────────────┘
    (walks graph, dispatches work)
         │
         ├──► Node Runners
         │    (SQL, Python, dlt, Go, etc.)
         │    Depends on: Graph Definition (to know what to run)
         │    Depends on: Environment Config (to know where to connect)
         │
         ├──► Structured Log Store
         │    (receives events from executor and node runners)
         │    Depends on: nothing — can start as JSONL files
         │
         ├──► Checks
         │    Depends on: Node Runners (checks ARE nodes, same execution model)
         │    Depends on: Graph Definition (checks attach to assets)
         │    Depends on: Default Check Definitions (layer-based defaults)
         │
         ├──► Incremental Loading / Interval Tracking
         │    Depends on: State Store (to track processed intervals)
         │    Depends on: Node Runners (to inject @start/@end)
         │    Depends on: Graph Definition (time_column, lookback metadata)
         │
         ├──► Trigger System
         │    Depends on: DAG Executor (to submit runs)
         │    Depends on: Graph Definition (to know which subgraph to run)
         │    Cron: just a scheduler that calls the executor
         │    Webhooks: HTTP server that calls the executor
         │
         ├──► CLI
         │    Depends on: DAG Executor (to submit/query runs)
         │    Depends on: Structured Log Store (to query status)
         │    Depends on: Graph Definition (to validate graphs)
         │
         ├──► Environment Config / Virtual Environments
         │    Depends on: Graph Definition (to know which assets exist)
         │    Depends on: Node Runners (to swap destinations)
         │
         └──► Test Mode (DuckDB Diversion)
              Depends on: Environment Config (test env swaps to DuckDB)
              Depends on: Checks (to validate test output)
              Depends on: Structured Log Store (to save test metadata)
```

**Critical path**: Graph Definition → DAG Executor → Node Runners → Structured Logs → Everything else layers on top.
