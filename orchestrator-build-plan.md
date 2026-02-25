# Build Plan: Lightweight Asset-Oriented Orchestrator

## Phase Overview

```
Phase 0: Foundation (Graph + Executor + Logs)
    │
Phase 1: Make It Useful (Node Runners + CLI + Basic Checks)
    │
Phase 2: Make It Smart (Incremental Loading + Interval Tracking)
    │
Phase 3: Make It Operational (Triggers + Environments + Connectors)
    │
Phase 4: Make It Testable (DuckDB Test Mode + Default Checks by Layer)
    │
Phase 5: Make It Production-Ready (Virtual Environments + Multi-Pipeline + Polish)
```

---

## Phase 0: Foundation

**Goal:** A Go binary that can read a graph definition, execute nodes in parallel with independent failure, and log what happened.

### What to Build

**0a. Graph Definition Parser**
- Read a JSON config file that declares assets with names, types (SQL/Python/shell), and source file paths
- Parse source files for `-- depends_on:` comments to extract dependencies
- Build an in-memory DAG from the parsed config + dependencies
- Validate: detect cycles, missing dependencies, duplicate asset names
- Reference: Dagu's YAML parser for structure, but our dependency extraction from source files is novel

**0b. DAG Executor**
- Topological sort of the graph
- Worker pool (goroutines) with configurable concurrency (`max_parallel`)
- Walk the graph: find all nodes with zero unresolved dependencies, dispatch them
- When a node completes: mark it done, check if any dependents are now unblocked, dispatch those
- Independent failure: if a node fails, mark all descendants as `skipped`, keep running everything else
- Reference: `go-dag` library for the core algorithm, Dagu's agent/executor architecture for the dispatch model

**0c. Stub Node Runner**
- For now, just execute shell commands (`sh -c "..."`)
- Capture: exit code, stdout, stderr, start time, end time, duration
- This proves the executor works without needing real SQL/Python runners yet

**0d. Structured Log Store (v1: JSONL files)**
- Each run gets a directory: `runs/{run_id}/`
- Each node writes a JSONL entry: `{asset, run_id, status, start, end, duration, error, stdout_path, stderr_path}`
- Run-level summary file: `{run_id, start, end, total_nodes, succeeded, failed, skipped}`
- Reference: Dagu's file-based storage model (JSONL status files, separate stdout/stderr per step)

### How to Test Phase 0

- **Unit tests for graph parser**: cycles detected, missing deps caught, valid graphs parse correctly
- **Unit tests for executor**: 
  - Linear chain (A→B→C) runs in order
  - Independent nodes (A, B, C with no deps) run in parallel
  - Failure propagation: if B fails, C (depends on B) is skipped, A (no dep on B) succeeds
  - Concurrency limit respected: with `max_parallel=2`, at most 2 nodes run simultaneously
- **Integration test**: create 20 shell commands (`echo` + `sleep` with varying durations), define dependencies, run, verify correct ordering and parallelism from log timestamps
- **Benchmark test**: 100 no-op nodes with complex dependencies. Measure total overhead. Target: <1 second for graph resolution and dispatch of 100 nodes.

### Unknowns

- **Dependency parsing from source files**: How robust does this need to be? Simple regex for `-- depends_on:` in SQL, `# depends_on:` in Python? Or do we need a proper parser? Start with regex, see if it breaks.
- **Concurrency model**: Goroutines are cheap but we need to handle process execution (for Python/SQL nodes that shell out). How do we handle hundreds of concurrent subprocesses? Probably fine on a VM with reasonable `max_parallel`, but test it.
- **Graph definition format**: JSON works for now, but as complexity grows we may want YAML or TOML. Don't over-design — start with JSON, swap later if needed.

### Definition of Done
Run 50 shell-command assets with dependencies in <2 seconds total overhead. All logs captured. Failures propagate correctly.

---

## Phase 1: Make It Useful

**Goal:** Real node runners (SQL, Python, dlt), a CLI for humans, and basic checks.

**Depends on:** Phase 0 (graph, executor, logs)

### What to Build

**1a. SQL Node Runner**
- Accept a SQL file path and a connection config (from `destination_connection` in pipeline config)
- Execute the SQL against the target warehouse
- Capture: row count affected, execution time, any errors
- Handle connection pooling: the executor manages a pool of DB connections, node runners borrow/return
- Reference: Dagu's `postgres` and `sqlite` step types for the basic pattern

**1b. Python Node Runner**
- Execute a Python script as a subprocess
- Inject connection configs as environment variables: `GRANICUS_SOURCE_CONNECTION` and `GRANICUS_DEST_CONNECTION` (JSON-serialized connection details)
- Capture stdout/stderr, exit code
- Convention: the script writes structured output (JSON) to stdout for metadata (row counts, etc.)
- Or: the script writes to a known metadata file path that the runner reads after execution

**1c. dlt Node Runner**
- Execute a dlt pipeline script as a Python subprocess
- Inject source and destination connection configs as environment variables
- dlt is the ideal node type for cross-system data movement (e.g., Iceberg → BigQuery) since dlt handles schema inference, incremental loading, and normalization natively
- Capture dlt's load_info output for metadata
- Reference: dlt's CLI interface (`dlt run`) or just invoke the Python script directly

**1d. CLI**
- `orchestrator run <pipeline_config>` — run a full pipeline
- `orchestrator run <pipeline_config> --assets asset_a,asset_b` — run a subgraph
- `orchestrator run <pipeline_config> --from-failure <run_id>` — re-run from last failure (query log store for failed nodes, use them as starting points)
- `orchestrator status <run_id>` — show run status from log store
- `orchestrator validate <pipeline_config>` — parse and validate graph, report issues
- `orchestrator history` — show recent runs
- Reference: Dagu's CLI for UX patterns

**1e. Basic Checks**
- Checks are just another node type that runs after its target asset
- Define checks in the config or in a `tests/` folder
- Check runner: executes a SQL query or Python script, expects a pass/fail result
- Checks that fail mark the check node as failed but don't block other assets (independent failure applies)
- Reference: Soda Core's SodaCL for the check definition syntax, dbt tests for the "checks are just SQL returning failing rows" pattern

### How to Test Phase 1

- **SQL runner**: Create a test table in BigQuery/DuckDB (local for testing), run a SQL transform, verify the output table exists and has expected rows
- **Python runner**: Write a Python script that reads a CSV and writes a Parquet file. Run it, verify output exists.
- **dlt runner**: Write a simple dlt pipeline (API → DuckDB), run it, verify data loaded
- **CLI tests**: End-to-end: define a 5-asset pipeline with SQL + Python + checks, run via CLI, verify logs and status output
- **Re-run from failure**: Run a pipeline where one asset deliberately fails, then re-run from failure, verify only the failed asset and its descendants re-run
- **Checks**: Define a check that validates row count > 0. Run after a successful asset. Run after an asset that produces 0 rows. Verify check pass/fail.

### Unknowns

- **BigQuery connection management**: How many concurrent connections can we open? Rate limits? Need to test with real workloads.
- **Python subprocess overhead**: How fast is it to spin up a Python subprocess? If it's 500ms+, that's our per-node overhead for Python assets. May want a long-running Python worker process that accepts tasks instead.
- **Check syntax**: Do we invent our own, use SodaCL-compatible YAML, or just use raw SQL? Start with raw SQL (a query that returns 0 rows = pass, >0 rows = fail) and layer nicer syntax later.
- **dlt integration**: dlt has its own state tracking and incremental loading. Do we let dlt manage its own state, or does our orchestrator wrap it? Start with letting dlt own its state — we just trigger it and capture results.

### Definition of Done
A real pipeline: dlt extracts data from an API → SQL transforms it in BigQuery → Python generates a summary → checks validate the output. All via CLI. Runs in under 5 minutes for a moderate dataset. Logs show everything that happened.

---

## Phase 2: Make It Smart

**Goal:** Incremental loading with interval tracking, so pipelines only process new data.

**Depends on:** Phase 1 (working node runners, log store)

### What to Build

**2a. State Store (upgrade from JSONL)**
- Postgres table for interval tracking: `{asset_id, interval_start, interval_end, status, run_id, processed_at}`
- On each run: query state store for unprocessed intervals, process only those
- Record processed intervals after successful execution
- Reference: SQLMesh's interval tracking model — divide time into disjoint intervals based on cron/interval_unit, record which have been processed, identify gaps

**2b. Interval Injection**
- Assets declare `time_column`, `interval_unit` (day/hour), `lookback` in their source file or config
- The executor calculates which intervals need processing
- The node runner injects `@start` and `@end` values into SQL queries (string substitution)
- For Python nodes: pass start/end as environment variables or CLI args
- Reference: SQLMesh's `@start_ds` / `@end_ds` macro substitution

**2c. Lookback and Gap Detection**
- Lookback: always reprocess the last N intervals to catch late-arriving data
- Gap detection: compare all expected intervals against processed intervals, identify and fill gaps
- Batch size: if backfilling a large range, chunk into batches of N intervals per execution

**2d. Multi-Output Assets**
- Support `-- produces: table_a, table_b, table_c` declarations in source files
- Each output is a separate node in the graph for dependency purposes
- But they're produced by a single execution unit
- If the execution fails, all outputs are marked failed. If it succeeds, all outputs are marked succeeded.
- Downstream assets can depend on individual outputs

### How to Test Phase 2

- **Interval tracking**: Run a daily asset for 5 days. On day 6, verify only day 6 is processed. Manually delete day 3's record from state store. Run again. Verify day 3 and day 6 are both processed (gap fill).
- **Lookback**: Set lookback=2 on a daily asset. Run on day 5. Verify days 3, 4, and 5 are all processed.
- **Batch size**: Backfill 30 days with batch_size=7. Verify 5 batches run (4×7 + 1×2).
- **Multi-output**: Define a Python script that writes 3 tables. Define downstream assets that depend on individual outputs. Verify graph resolution is correct.
- **Idempotency test**: Run the same interval twice. Verify the data is identical (no duplicates).

### Unknowns

- **State store schema**: How granular do we track? Per-asset per-interval? Per-asset per-interval per-run? Start simple.
- **Partial interval handling**: What if a run crashes mid-interval? Do we mark it as processed or not? Need a transaction model — mark as "in progress", then "complete" on success. On restart, "in progress" intervals get reprocessed.
- **Cross-asset interval dependencies**: If asset B depends on asset A, and A has been processed through day 5 but B only through day 3, should the executor automatically backfill B for days 4-5? This is where it gets complex. Start with manual backfill, add automatic later.
- **Non-time-based incrementals**: What about incremental by unique key (upserts)? Not needed in Phase 2, but design the state store to be extensible.

### Definition of Done
A daily pipeline that runs incrementally. On first run, backfills from start date. On subsequent runs, only processes new intervals. Lookback catches late-arriving data. Gaps can be detected and filled.

---

## Phase 3: Make It Operational

**Goal:** The orchestrator runs on a schedule, responds to events, and handles dev/prod separation.

**Depends on:** Phase 2 (incremental loading, state store)

### What to Build

**3a. Cron Scheduler**
- Built-in cron scheduler that reads pipeline configs and runs them on declared schedules
- Runs as a long-running process on the VM
- Prevents duplicate concurrent runs of the same pipeline (locking via state store)
- Reference: Dagu's built-in scheduler, SQLMesh's cron-per-model approach

**3b. Webhook / Event Listener**
- Small HTTP server built into the orchestrator binary
- Endpoints: `POST /trigger/{pipeline_name}` — trigger a pipeline run
- `POST /trigger/{pipeline_name}?assets=a,b,c` — trigger a subgraph
- Authentication: API key in header
- Use case: S3/GCS event notifications, CI/CD webhooks, external systems signaling new data

**3c. Environment Config**
- Separate environment config file: dev/prod/test definitions with database, prefix, destination overrides
- `--env dev` flag on CLI and in trigger API
- Node runners read environment config to determine connection strings and table prefixes
- Reference: SQLMesh's environment-per-schema approach, dbt's target/profile pattern

**3d. Connectors: BigQuery, GCS, Hetzner Cloud Storage, Iceberg**
- BigQuery: SQL execution and table writes (likely already working from Phase 1)
- GCS/Hetzner: File read/write capability in node runners (for landing files, reading extracts)
- Iceberg: Read/write via PyIceberg or DuckDB's Iceberg extension. Treat as a connection type in config.
- Cross-system movement (Iceberg → BigQuery, Postgres → GCS, etc.) uses the `source_connection` + `destination_connection` pattern. The node runner injects both connection configs as environment variables. dlt is the recommended node type for these transfers.
- These are node runner capabilities, not orchestrator features — keep the orchestrator agnostic

### How to Test Phase 3

- **Cron**: Configure a pipeline to run every minute. Let it run for 5 minutes. Verify 5 runs in the log store. Verify no duplicate runs.
- **Webhooks**: Send a POST to the trigger endpoint. Verify a run starts. Send a POST with `?assets=a,b`. Verify only that subgraph runs.
- **Locking**: Trigger two runs of the same pipeline simultaneously. Verify the second one is queued or rejected.
- **Environments**: Run the same pipeline with `--env dev` and `--env prod`. Verify they write to different databases/prefixes. Verify no cross-contamination.
- **GCS/Hetzner**: Upload a file to storage, trigger a pipeline that reads it, verify data flows through.

### Unknowns

- **Webhook security**: API key is the minimum. Do we need HMAC signature verification for production webhooks? Probably yes eventually, but API key is fine for now.
- **Concurrent run handling**: Queue or reject? If a pipeline is already running and a new trigger comes in, do we queue it (run after current finishes) or reject it (409 Conflict)? Configurable per pipeline probably. Start with reject.
- **Iceberg complexity**: PyIceberg is still maturing. DuckDB's Iceberg support is read-only in some configurations. Need to test actual read/write patterns before committing.
- **GCS vs Hetzner storage**: Different APIs. Do we abstract behind a common interface, or just have separate node runner configs? Start with separate configs, abstract later if the patterns converge.

### Definition of Done
Pipelines run on cron schedules unattended. Webhooks trigger runs for event-driven data. Dev and prod environments are fully isolated.

---

## Phase 4: Make It Testable

**Goal:** AI agents (and humans) can safely test pipeline changes against real data without touching production.

**Depends on:** Phase 3 (environments, connectors), Phase 1 (checks)

### What to Build

**4a. DuckDB Test Mode**
- `--test` flag swaps all destinations to a local DuckDB instance
- The executor reads from real upstream sources but writes to DuckDB
- For SQL assets: rewrite the destination in the query (or use DuckDB's BigQuery/Postgres attach if available)
- For Python assets: swap the destination connection string via environment config
- Reference: SQLMesh's virtual environments for the "zero-copy, swap destination" concept

**4b. Default Checks by Layer**
- Define default check sets per layer:
  - **source**: not null on declared key columns, row_count > 0, freshness (data arrived within expected window)
  - **staging**: unique keys, no unexpected nulls, schema matches expected columns
  - **mart**: referential integrity vs upstream, row count ratios within expected range
- Assets declare their layer in config or source file (`-- layer: staging`)
- Default checks auto-attach unless `default_checks: false`
- Custom checks in `tests/` folder override or extend defaults
- Reference: dbt's built-in tests (unique, not_null, accepted_values, relationships) for the pattern; Soda's SodaCL for the syntax

**4c. Test Metadata Capture**
- When running in test mode, capture additional metadata:
  - Schema comparison: expected columns vs actual columns
  - Row count comparison: expected range vs actual
  - Value distributions: basic stats on key columns
  - Delta from production: if prod data exists, compare test output to prod
- Save metadata to a test results file (JSON) alongside the DuckDB data
- If all checks pass: delete DuckDB data, keep metadata
- If any check fails: keep DuckDB data for inspection

**4d. Custom Test Definitions**
- Tests in `tests/` folder follow a naming convention: `test_{asset_name}.sql` or `test_{asset_name}.py`
- SQL tests: query returns 0 rows = pass
- Python tests: script exits 0 = pass, exits non-zero = fail
- Tests can reference the test DuckDB directly for assertions

### How to Test Phase 4

- **Test mode**: Run a pipeline with `--test`. Verify all writes go to DuckDB, not BigQuery. Verify upstream reads still work.
- **Default checks**: Run a staging asset without custom checks. Verify unique + not_null checks run automatically. Override with `default_checks: false`, verify they don't run.
- **Test metadata**: Run in test mode, all checks pass → verify DuckDB data deleted, metadata file exists. Force a check failure → verify DuckDB data preserved.
- **AI workflow simulation**: Modify a SQL model, run `--test`, verify the test captures the change and checks pass/fail appropriately. This is the core use case for AI agents testing their own changes.

### Unknowns

- **DuckDB ↔ BigQuery compatibility**: Can DuckDB run the same SQL as BigQuery? Mostly, but there are dialect differences (BigQuery's `SAFE_DIVIDE`, `STRUCT` types, etc.). We may need SQL transpilation for test mode. SQLMesh uses SQLGlot for this — worth investigating.
- **Reading from production in test mode**: The test pipeline needs to read real upstream data but write locally. This means the node runners need to handle mixed sources (real upstream, local destination). Design this carefully.
- **Schema comparison complexity**: How do we define "expected schema"? From the last successful prod run? From a declared schema in config? Start with comparing to the last prod run's metadata in the log store.
- **Performance**: DuckDB is fast but loading large datasets locally for testing might be slow. May need a `--test --sample N` flag that only processes N rows/intervals.

### Definition of Done
An AI agent can: modify a SQL model → run `orchestrator run --test --assets modified_asset` → get back a JSON file with check results, schema diffs, and row count comparisons → make a decision to promote or not. All without touching production.

---

## Phase 5: Make It Production-Ready

**Goal:** Virtual environments, multi-pipeline support, and production hardening.

**Depends on:** All previous phases

### What to Build

**5a. Virtual Environments**
- Production is a set of views pointing to physical tables
- Dev environment: create view copies pointing to same physical tables, only materialize changed assets to new physical tables
- Promotion: swap view pointers from dev to prod (no data movement)
- Rollback: swap back to previous physical table version
- Fingerprinting: hash model logic to detect whether an asset has actually changed
- Reference: SQLMesh's virtual data environments — the full implementation (snapshot tables, fingerprinting, view swapping)

**5b. Multi-Pipeline Support**
- Top-level config that references multiple pipeline configs
- Cross-pipeline dependencies: asset in pipeline A depends on asset in pipeline B
- Shared state store across pipelines
- Independent scheduling per pipeline, with dependency awareness

**5c. Graph Discovery / Convention-Based Config**
- Instead of listing every asset in a config file, discover assets from directory structure:
  - `sql/source/*.sql` → source layer SQL assets
  - `sql/staging/*.sql` → staging layer SQL assets
  - `python/transforms/*.py` → Python transform assets
  - `dlt/extracts/*.py` → dlt pipeline assets
- Config becomes minimal: just destination, schedule, and overrides
- Dependencies still parsed from source files

**5d. Production Hardening**
- Graceful shutdown: finish in-progress nodes, don't start new ones
- Health check endpoint: `GET /health`
- Metrics export: Prometheus-compatible metrics (run count, duration, failure rate, node latency)
- Alerting hooks: webhook on failure (Slack, PagerDuty, etc.)
- Log rotation and retention policies
- State store backup and recovery

**5e. Documentation and Onboarding**
- README with quickstart
- Example pipelines
- Migration guide from Dagster

### How to Test Phase 5

- **Virtual environments**: Create a dev environment, modify an asset, materialize only that asset, verify prod unchanged. Promote. Verify prod now uses new version. Rollback. Verify prod uses old version.
- **Multi-pipeline**: Define two pipelines with a cross-pipeline dependency. Run pipeline B. Verify it waits for pipeline A's dependency to be fresh.
- **Convention discovery**: Create the standard directory structure with 20 SQL files. Run without explicit asset config. Verify all assets discovered with correct layers and dependencies.
- **Graceful shutdown**: Start a long-running pipeline, send SIGTERM. Verify in-progress nodes complete, no new nodes start, final state is consistent.
- **Metrics**: Scrape the Prometheus endpoint during a run. Verify metrics update in real-time.

### Unknowns

- **Virtual environments with non-SQL assets**: SQLMesh's view-swapping works because everything is SQL tables. How do we handle Python assets that write to files or APIs? May need a different promotion model for non-SQL assets.
- **Cross-pipeline dependency resolution**: This is complex. How does the scheduler know that pipeline A's `revenue_summary` is fresh enough for pipeline B? Needs a freshness/staleness model. May defer to Phase 6 or beyond.
- **Convention vs configuration balance**: Too much convention = magic that's hard to debug. Too much configuration = verbose config files. Need to find the right default conventions with easy overrides.
- **Migration path from Dagster**: How much can be automated? Can we parse Dagster asset definitions and generate our config? Worth investigating but low priority.

### Definition of Done
The orchestrator is running in production on a VM, executing multiple pipelines on schedules, with dev/prod isolation via virtual environments, Prometheus metrics, and Slack alerting on failures.

---

## Timeline Estimates (Rough)

| Phase | Estimated Effort | Cumulative |
|-------|-----------------|------------|
| Phase 0: Foundation | 2-3 weeks | 2-3 weeks |
| Phase 1: Make It Useful | 3-4 weeks | 5-7 weeks |
| Phase 2: Make It Smart | 2-3 weeks | 7-10 weeks |
| Phase 3: Make It Operational | 2-3 weeks | 9-13 weeks |
| Phase 4: Make It Testable | 3-4 weeks | 12-17 weeks |
| Phase 5: Make It Production-Ready | 4-6 weeks | 16-23 weeks |

These assume one developer focused on it. Phases 3 and 4 could be partially parallelized if there are two people working on it.

## Key Risks

1. **SQL dialect compatibility**: BigQuery SQL ≠ DuckDB SQL ≠ Postgres SQL. Test mode (Phase 4) and virtual environments (Phase 5) both depend on SQL being portable across engines. SQLGlot (used by SQLMesh for transpilation) may be needed.

2. **Python subprocess overhead**: If every Python node spins up a new Python process, the overhead might be significant (500ms-1s per node). May need a long-running Python worker process that accepts tasks via stdin/socket.

3. **State store reliability**: The interval tracking state store is critical. If it's wrong, pipelines reprocess data or miss intervals. Needs careful transaction handling and recovery logic.

4. **Scope creep**: Each phase reveals new features that seem essential. Discipline is needed to stay on the critical path and not get pulled into building a full framework.

5. **Adoption friction**: If migrating from Dagster is painful, the tool won't get used. Phase 5's migration guide and convention-based discovery are important for adoption.

## What to Build First (Next Steps)

1. Set up a Go project with basic structure
2. Implement the graph parser (JSON config + source file dependency extraction)
3. Implement the DAG executor with goroutine worker pool
4. Implement shell command node runner
5. Implement JSONL log store
6. Write the benchmark test: 100 no-op nodes, measure overhead
7. If overhead is <1 second: move to Phase 1. If not: profile and optimize.
