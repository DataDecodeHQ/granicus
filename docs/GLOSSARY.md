# Glossary

Domain-specific terms used in Granicus. Defined as they are used in this system, not generically.

---

## Asset

A named unit of pipeline logic -- a SQL query, Python script, or other executable code -- that Granicus manages and executes. Assets are defined in `pipeline.yaml` with a type, source file, and resource bindings. At execution time, assets are scheduled based on their links and produce results with status, telemetry, and check outcomes.

**Used in:** `config.AssetConfig`, `graph.Asset`, `runner.Asset`, `pipeline.yaml` `assets:` section

**Also see:** Link, Check, Run

---

## Check

A validation test on an asset's output. Checks run after their parent asset completes and produce a signal with severity (info, warning, error, critical) and a blocking flag. The executor responds to the signal per the Check Signal Response pattern in PATTERNS.md. Checks are SQL queries or Python scripts that assert data quality conditions -- row counts, null rates, referential integrity, business rules.

**Used in:** `config.CheckConfig`, `runner.SQLCheckRunner`, `runner.PythonCheckRunner`, `pipeline.yaml` `checks:` on each asset

**Also see:** Asset

---

## Credential

An authentication artifact that grants Granicus access to a resource. Each resource references a credential (service account key, OAuth token, API key) that the runner uses at execution time. Credentials are stored externally (Secret Manager in cloud mode, local files in local mode) and are never embedded in pipeline config.

**Used in:** `config.ResourceConfig.Credentials`, Secret Manager secrets in infrastructure

**Also see:** Resource, Execution Environment

---

## Dispatch

The mechanism that routes asset execution to a compute environment. The `DispatchRegistry` maps runner names to `RunnerDispatch` implementations: `LocalDispatch` runs assets in-process via the `RunnerRegistry`, `CloudRunJobDispatch` sends work to Cloud Run Jobs and waits for results via Pub/Sub. Dispatch is pure routing -- it does not handle retry, scheduling, or execution order. The executor calls dispatch; dispatch calls the compute layer.

**Used in:** `runner.DispatchRegistry`, `runner.RunnerDispatch`, `runner.LocalDispatch`, `runner.CloudRunJobDispatch`

**Also see:** Asset, Execution Environment, Run

---

## Execution Environment

Where asset code runs. Determined by mode (local vs cloud) and dispatch configuration. In local mode, assets run in-process on the user's machine. In cloud mode, assets are dispatched to managed compute (Cloud Run today, with future support for Lambda, Cloud Functions, etc.). Checks run in the same execution environment as their parent asset.

**Used in:** `runner.RunnerDispatch` interface, `runner.LocalDispatch`, `runner.CloudRunJobDispatch`

**Also see:** Asset, Run

---

## Interval

A bounded datetime window (start/end pair) for incremental asset processing. Granicus divides the date range between an asset's `start_date` and now into intervals by unit (hour, day, week, month), tracks which intervals are complete, and only processes missing or failed ones. The `lookback` setting forces reprocessing of the last N completed intervals for sources where data can change after initial load. All intervals use datetime format regardless of unit.

**Used in:** `state.Interval`, `state.GenerateIntervals`, `state.ComputeMissing`, `executor.executeIncremental`

**Also see:** Asset, Run

---

## Link

A dependency between assets -- "run A before B." Links form the directed acyclic graph (DAG) that determines execution order. Declared via `depends_on` (explicit) or `upstream` (inferred from SQL references) in pipeline config. The executor uses links to schedule assets in parallel where possible and sequentially where required.

**Used in:** `config.AssetConfig.Upstream`, `config.AssetConfig.DependsOn`, `graph.Asset.DependsOn`, `graph.Asset.DependedOnBy`

**Also see:** Asset, Pipeline

---

## Pipeline

The top-level unit of configuration. A YAML file defining assets, resources, checks, and their relationships. A pipeline has a name, one or more resources, and a set of assets with links between them. Pipelines can be run on-demand, on a schedule, or via webhook trigger.

**Used in:** `config.PipelineConfig`, `pipeline.yaml`, CLI `granicus run <pipeline.yaml>`

**Also see:** Asset, Resource, Run

---

## Scheduler

The infrastructure-layer mechanism that determines when pipelines run. The `schedule` field in `pipeline.yaml` is the source of truth for timing. Should be managed through the CLI/API with mode-based routing, following the same pattern as Dispatch (see BL-293 for unification work).

**Local mode:** An in-process cron engine (`robfig/cron`) that registers cron entries from pipeline config and invokes runs directly. Supports hot-reload and asset-level polling (`poll_interval` on `gcs_ingest` assets). Acquires a lock before each run to prevent overlapping executions.

**Cloud mode:** Routes to Cloud Scheduler jobs that send authenticated HTTP requests to the engine's trigger endpoint (`/api/v1/pipelines/{name}/trigger`). The engine handles execution; the scheduler only fires the trigger.

**Current state:** Local scheduling is fully built. Cloud scheduling is Terraform-only -- not yet managed through the CLI/API. See backlog item for unification.

**Used in:** `scheduler.Scheduler`, `scheduler.NewScheduler`, `scheduler.LoadAndRegister`, `scheduler.Reload`, Cloud Scheduler Terraform resources

**Also see:** Pipeline, Run, Dispatch

---

## Resource

A persistent external data service that assets read from and write to -- a BigQuery project, GCS bucket, S3 bucket, or other storage target. Each resource has a type, properties (project ID, dataset, bucket name), and a credential that grants access. Resources are the transformation targets: Granicus uses them to execute and store pipeline results.

**Used in:** `config.ResourceConfig`, `pipeline.yaml` `resources:` section

**Also see:** Credential, Asset

---

## Run

A single pipeline execution identified by a unique run ID (e.g., `run_20240115_143022`). A run processes all scheduled assets (or a filtered subset), tracks their results, and records events. Runs have a status (success, completed_with_failures), duration, and per-asset results with telemetry.

**Used in:** `executor.RunResult`, `state.Store`, `events.Store`, CLI `granicus run`, `granicus status`, `granicus history`

**Also see:** Asset, Pipeline, Interval

---

## Run Retention

The lifecycle process for pipeline run data. Completed runs live in Firestore (hot storage) where all CLI commands and APIs query them. After a retention period (default 90 days, configurable via `GRANICUS_RETENTION_DAYS`), runs are archived as immutable JSONL to GCS (`gs://<bucket>/runs/<pipeline>/YYYY/MM/run_<id>.jsonl`), then pruned from Firestore. Pruning only proceeds if the GCS archive exists -- this safety invariant prevents data loss. GCS archives are currently write-only; no Granicus command reads from them. The retention window determines how far back `granicus history` and `granicus failures` can see.

**Current state:** The archive and prune code exists (`archive.RunArchiver`, `archive.Pruner`) but is not yet wired into `granicus gc` or the Cloud Scheduler job. See BL-292.

**Used in:** `archive.RunArchiver`, `archive.Pruner`

**Also see:** Run, Interval
