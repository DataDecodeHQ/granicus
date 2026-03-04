# Granicus CLI Reference

Granicus is a lightweight asset-oriented data pipeline orchestrator. All commands follow the pattern:

```
granicus <command> [arguments] [flags]
```

---

## Commands

- [run](#run)
- [validate](#validate)
- [status](#status)
- [history](#history)
- [events](#events)
- [models](#models)
- [gc](#gc)
- [backup](#backup)
- [serve](#serve)
- [migrate](#migrate)
- [completion](#completion)
- [doctor](#doctor)
- [version](#version)

---

## run

Execute a pipeline defined by a YAML config file.

```
granicus run <config.yaml> [flags]
```

**Arguments:** Path to pipeline config file (required).

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--assets <string>` | | Run only these assets and their upstream dependencies (comma-separated) |
| `--downstream-only` | false | With `--assets`, run only downstream dependents, skipping the named assets themselves |
| `--max-parallel <int>` | 0 | Override the `max_parallel` value from config (0 = use config value) |
| `--from-failure <string>` | | Re-run from a specific failed run ID |
| `--from-date <string>` | | Override start date for incremental assets (format: `YYYY-MM-DD`) |
| `--to-date <string>` | | Override end date for incremental assets (format: `YYYY-MM-DD`) |
| `--full-refresh` | false | Invalidate all interval state and reprocess from start |
| `--test` | false | Run in test mode (creates a temporary dataset, cleaned up after run) |
| `--test-window <string>` | | Limit test mode data window (e.g., `7d`, `4w`, `3m`) |
| `--keep-test-data` | false | Preserve the test dataset after run completes |
| `--dry-run` | false | Print execution plan (assets, intervals, checks) without running anything |
| `--output <string>` | | Output format; `json` emits a structured JSON result |
| `--project-root <string>` | `.` | Project root directory for resolving relative paths |

**Examples:**

```bash
# Run all assets in a pipeline
granicus run pipeline.yaml

# Run specific assets and their dependencies
granicus run pipeline.yaml --assets stg_orders,int_order_costs

# Run only downstream of a specific asset
granicus run pipeline.yaml --assets stg_orders --downstream-only

# Preview what would run without executing
granicus run pipeline.yaml --dry-run

# Refresh all incremental state and reprocess from scratch
granicus run pipeline.yaml --full-refresh

# Run in test mode with a 7-day window
granicus run pipeline.yaml --test --test-window 7d

# Override date range for incremental assets
granicus run pipeline.yaml --from-date 2024-01-01 --to-date 2024-01-31

# Capture JSON output for programmatic use
granicus run pipeline.yaml --output json
```

**JSON output schema** (`--output json`):

```json
{
  "run_id": "run_20240115_143022",
  "pipeline": "my_pipeline",
  "status": "success",
  "duration_seconds": 42.3,
  "succeeded": 8,
  "failed": 0,
  "skipped": 0,
  "total_nodes": 8,
  "nodes": [
    {
      "asset": "stg_orders",
      "status": "success",
      "duration_seconds": 5.1
    }
  ]
}
```

Possible `status` values: `success`, `completed_with_failures`.

---

## validate

Validate a pipeline config file and its dependency graph without executing anything.

```
granicus validate <config.yaml> [flags]
```

**Arguments:** Path to pipeline config file (required).

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--strict` | false | Promote warnings to errors (non-zero exit if any warnings) |
| `--quiet` | false | Only show errors and warnings, suppress informational output |
| `--json` | false | Output validation results as JSON |
| `--output <string>` | | Output format; `json` emits structured results |
| `--project-root <string>` | `.` | Project root directory |

**Examples:**

```bash
# Validate a pipeline config
granicus validate pipeline.yaml

# Validate and fail on warnings
granicus validate pipeline.yaml --strict

# Output results as JSON for CI integration
granicus validate pipeline.yaml --json

# Quiet mode (errors and warnings only)
granicus validate pipeline.yaml --quiet
```

**Exit codes:** `0` if valid (or no errors when not strict), `1` if validation fails.

---

## status

Show the execution status of a pipeline run. Defaults to the most recent run.

```
granicus status [run_id] [flags]
```

**Arguments:** Run ID (optional). If omitted, shows the most recent run.

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--output <string>` | | Output format; `json` emits structured results |
| `--project-root <string>` | `.` | Project root directory |

**Examples:**

```bash
# Show status of the most recent run
granicus status

# Show status of a specific run
granicus status run_20240115_143022

# JSON output
granicus status --output json
```

**JSON output schema** (`--output json`):

```json
{
  "run_id": "run_20240115_143022",
  "pipeline": "my_pipeline",
  "status": "success",
  "start_time": "2024-01-15T14:30:22Z",
  "end_time": "2024-01-15T14:31:04Z",
  "duration_seconds": 42.3,
  "succeeded": 8,
  "failed": 0,
  "skipped": 0,
  "total_nodes": 8,
  "nodes": [...]
}
```

---

## history

List recent pipeline runs.

```
granicus history [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--limit <int>` | 10 | Number of runs to display |
| `--output <string>` | | Output format; `json` emits structured results |
| `--project-root <string>` | `.` | Project root directory |

**Examples:**

```bash
# Show 10 most recent runs
granicus history

# Show 25 runs
granicus history --limit 25

# JSON output
granicus history --output json
```

---

## events

Query the event store for pipeline execution events.

```
granicus events [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--run-id <string>` | | Filter events by run ID |
| `--asset <string>` | | Filter events by asset name |
| `--type <string>` | | Filter by event type(s), comma-separated (see event types below) |
| `--pipeline <string>` | | Filter events by pipeline name |
| `--since <string>` | | Show events from the last duration (e.g., `24h`, `7d`) |
| `--limit <int>` | 50 | Maximum number of events to return |
| `--json` | false | Output events as JSON |
| `--output <string>` | | Output format; `json` emits structured results |
| `--project-root <string>` | `.` | Project root directory |

**Event types:**

| Type | Description |
|------|-------------|
| `run_started` | Pipeline run began |
| `run_completed` | Pipeline run finished |
| `pipeline_triggered` | Pipeline triggered via webhook |
| `node_started` | Individual asset execution began |
| `node_succeeded` | Asset completed successfully |
| `node_failed` | Asset execution failed |
| `node_skipped` | Asset was skipped |
| `stale_lock_recovered` | Stale execution lock was recovered on serve startup |
| `interval_recovered` | Orphaned interval was recovered on serve startup |

**Examples:**

```bash
# Show recent events
granicus events

# Filter by run
granicus events --run-id run_20240115_143022

# Show only failures from the last 24 hours
granicus events --type node_failed --since 24h

# Filter by asset across all runs
granicus events --asset stg_orders

# JSON output
granicus events --json
```

---

## models

Show the model registry and version history for pipeline assets.

```
granicus models [asset_name] [flags]
```

**Arguments:** Asset name (optional). If omitted, lists all registered models.

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--diff <string>` | | Show diff between two model versions (e.g., `1,2`) |
| `--output <string>` | | Output format; `json` emits structured results |
| `--project-root <string>` | `.` | Project root directory |

**Examples:**

```bash
# List all models
granicus models

# Show version history for a specific asset
granicus models stg_orders

# Diff two versions of a model
granicus models stg_orders --diff 1,2

# JSON output
granicus models --output json
```

---

## gc

Clean up old run logs and test artifacts from the state and event stores.

```
granicus gc [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--retention-days <int>` | 30 | Delete run records and events older than this many days |
| `--project-root <string>` | `.` | Project root directory |

**What gc cleans:**
- Old run records from the state store
- Events older than the retention period from the event store
- Test artifacts and temporary datasets

**Examples:**

```bash
# Run garbage collection with default 30-day retention
granicus gc

# Keep only the last 7 days of data
granicus gc --retention-days 7
```

---

## backup

Create a backup of the Granicus state stores (`state.db` and `events.db`).

```
granicus backup [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--output <string>` | | Backup destination path (default: alongside `state.db`) |
| `--keep <int>` | 7 | Number of backup copies to retain; older backups are pruned |
| `--project-root <string>` | `.` | Project root directory |

**Examples:**

```bash
# Create backup with default settings (keep 7 copies)
granicus backup

# Write backup to a specific location
granicus backup --output /backups/granicus/state.db.bak

# Keep only the 3 most recent backups
granicus backup --keep 3
```

---

## serve

Start the Granicus scheduler and HTTP trigger server. Runs pipelines on schedule and accepts webhook triggers.

```
granicus serve --config-dir <dir> [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--config-dir <string>` | | **(Required)** Directory containing pipeline YAML config files |
| `--server-config <string>` | | Path to `granicus-server.yaml` (port, API keys) |
| `--env-config <string>` | | Path to `granicus-env.yaml` (environment variable overrides) |
| `--env <string>` | `dev` | Environment name (used for environment config resolution) |
| `--orphan-timeout <duration>` | `2h` | Time before an `in_progress` interval is considered orphaned and recovered |
| `--project-root <string>` | `.` | Project root directory |

**Examples:**

```bash
# Start server with pipelines from a config directory
granicus serve --config-dir ./pipelines

# Start with server config (custom port, API keys) and environment config
granicus serve --config-dir ./pipelines \
  --server-config ./granicus-server.yaml \
  --env-config ./granicus-env.yaml \
  --env production

# Shorten orphan recovery timeout
granicus serve --config-dir ./pipelines --orphan-timeout 30m
```

**HTTP API endpoints:**

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/health` | GET | Health check — returns server status and pipeline count |
| `/metrics` | GET | Prometheus metrics (run counts, durations, node results) |
| `/api/v1/pipelines/<name>/trigger` | POST | Manually trigger a pipeline run |

**Trigger request body (JSON):**

```json
{
  "assets": ["stg_orders", "int_order_costs"],
  "from_date": "2024-01-01",
  "to_date": "2024-01-31"
}
```

All fields are optional. An empty body triggers a full pipeline run.

**Server config file (`granicus-server.yaml`):**

```yaml
server:
  port: 8080
  api_keys:
    - name: ci
      key: your-api-key-here
```

**Startup behavior:**
- Recovers stale execution locks (older than 6 hours)
- Recovers orphaned intervals that have been `in_progress` longer than `--orphan-timeout`
- Watches `--config-dir` for config file changes and reloads automatically
- Shuts down gracefully on `SIGTERM`/`SIGINT`, draining in-progress runs (up to 5 minutes)

---

## migrate

Migrate a pipeline config file to the latest format version.

```
granicus migrate <config.yaml> [flags]
```

**Arguments:** Path to pipeline config file (required).

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--dry-run` | false | Show what would change without modifying the file |
| `--from-version <string>` | | Override the detected config version (e.g., `0.2`) |

**Examples:**

```bash
# Migrate a config to the latest format
granicus migrate pipeline.yaml

# Preview changes without applying
granicus migrate pipeline.yaml --dry-run

# Force migration from a specific version
granicus migrate pipeline.yaml --from-version 0.2
```

---

## completion

Generate shell completion scripts for bash, zsh, fish, and PowerShell.

```
granicus completion <bash|zsh|fish|powershell>
```

**Arguments:** Shell type (required). One of: `bash`, `zsh`, `fish`, or `powershell`.

**Description:**

Generates a completion script for the specified shell. Pipe the output to the appropriate command to install the completion:

- **bash**: `granicus completion bash | sudo tee /etc/bash_completion.d/granicus`
- **zsh**: `granicus completion zsh | sudo tee /usr/share/zsh/site-functions/_granicus`
- **fish**: `granicus completion fish | sudo tee /usr/share/fish/vendor_completions.d/granicus.fish`
- **powershell**: `granicus completion powershell | Out-String | Invoke-Expression`

Or temporarily source the completion in your current session:

- **bash**: `source <(granicus completion bash)`
- **zsh**: `source <(granicus completion zsh)`
- **fish**: `granicus completion fish | source`
- **powershell**: `granicus completion powershell | Out-String | Invoke-Expression`

**Examples:**

```bash
# Generate bash completion and install
granicus completion bash | sudo tee /etc/bash_completion.d/granicus

# Temporarily enable for zsh
source <(granicus completion zsh)

# View powershell completion
granicus completion powershell
```

---

## doctor

Run environment health checks and report pass/fail/warn status for each check.

```
granicus doctor [config.yaml] [flags]
```

**Arguments:** Path to pipeline config file (optional). When provided, connectivity checks are run for each declared connection.

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--project-root <string>` | `.` | Project root directory |
| `--output <string>` | | Output format (`json`) |

**Checks performed:**

| Check | Description |
|-------|-------------|
| `go_version` | Reports the Go runtime version the binary was compiled with |
| `bq:<name>` | Verifies BigQuery connectivity for each `bigquery` connection (requires config) |
| `gcs:<name>` | Validates GCS connection config (bucket set, credentials file accessible) for each `gcs` connection (requires config) |
| `state.db` | Verifies `.granicus/state.db` is writable and passes SQLite integrity check |
| `events.db` | Verifies `.granicus/events.db` is writable |
| `disk_space` | Reports available disk space; warns below 1 GB, fails below 100 MB |

**Exit code:** Non-zero if any check has `fail` status.

**Examples:**

```bash
# Check local environment only (no connections)
granicus doctor --project-root .

# Check including BigQuery/GCS connectivity
granicus doctor pipeline.yaml --project-root .

# JSON output for scripting
granicus doctor --output json | jq '.checks[] | select(.status != "pass")'
```

**JSON output shape:**

```json
{
  "healthy": true,
  "checks": [
    {"name": "go_version", "status": "pass", "message": "go1.26.0"},
    {"name": "state.db",   "status": "pass", "message": "writable, integrity ok"},
    {"name": "events.db",  "status": "pass", "message": "writable"},
    {"name": "disk_space", "status": "pass", "message": "17.5 GB available"}
  ]
}
```

---

## version

Print the Granicus version string.

```
granicus version
```

**Example:**

```bash
$ granicus version
granicus 0.2.0
```

---

## Global notes

**`--project-root`**

Most commands accept `--project-root` (default: `.`). This sets the base directory for resolving relative paths in config files (SQL sources, credential files, state databases). Set this when running Granicus from a directory other than the project root.

**`--output json`**

Commands that support `--output json` write a single JSON object to stdout and suppress human-readable output. Exit code still reflects success or failure. Use this for CI pipelines, monitoring scripts, or any programmatic consumption of Granicus output.

**State files**

Granicus stores state in `.granicus/` relative to the project root:

| File | Contents |
|------|----------|
| `.granicus/state.db` | Incremental interval tracking |
| `.granicus/events.db` | Pipeline execution event log |
| `.granicus/test-state.db` | State for `--test` mode runs |
