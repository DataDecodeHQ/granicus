# Phase 0 Spec: Foundation — Graph Parser, DAG Executor, Structured Logs

## Project Overview

You are building a lightweight, asset-oriented data pipeline orchestrator in Go. The project name is `granicus`. This is Phase 0 — the foundation. It includes: a graph definition parser, a parallel DAG executor with independent failure, stub node runners, a structured log store, and a CLI entry point.

The orchestrator's job is: read a pipeline config, parse source files for dependencies, build a DAG, execute nodes in parallel (respecting dependencies), handle failures independently, and log everything.

## Project Structure

```
granicus/
├── cmd/
│   └── granicus/
│       └── main.go              # CLI entry point
├── internal/
│   ├── graph/
│   │   ├── graph.go             # DAG data structure
│   │   ├── parser.go            # Config + source file parser
│   │   └── graph_test.go        # Unit tests
│   ├── executor/
│   │   ├── executor.go          # Parallel DAG executor
│   │   └── executor_test.go     # Unit tests
│   ├── runner/
│   │   ├── runner.go            # Node runner interface + shell runner
│   │   └── runner_test.go       # Unit tests
│   ├── logging/
│   │   ├── store.go             # JSONL log store
│   │   └── store_test.go        # Unit tests
│   └── config/
│       ├── config.go            # Config structs and loading
│       └── config_test.go       # Unit tests
├── go.mod
├── go.sum
└── README.md
```

## Decisions (No Ambiguity)

### Config Format: YAML

Pipeline configs are YAML files. Example:

```yaml
pipeline: revenue_daily
max_parallel: 10

assets:
  - name: raw_transactions
    type: shell
    source: scripts/extract.sh

  - name: stg_transactions
    type: sql
    source: sql/stg_transactions.sql

  - name: revenue_summary
    type: python
    source: scripts/revenue_summary.py

  - name: check_revenue
    type: shell
    source: tests/check_revenue.sh
```

Notes:
- `type` is one of: `sql`, `python`, `shell`, `dlt`. In Phase 0, only `shell` is implemented. The others are defined in the enum but return a "not implemented" error if used.
- `source` is a relative file path from the project root.
- `name` is the unique identifier for the asset. If omitted, infer from the source filename (without extension).
- `max_parallel` defaults to 10 if omitted.
- Dependencies are NOT declared in the config. They are parsed from source files.

### Dependency Declaration in Source Files

Dependencies are declared in source files using comments. The parser looks for these patterns:

**SQL files (.sql):**
```sql
-- depends_on: raw_transactions
-- depends_on: dim_customers
```

**Python files (.py):**
```python
# depends_on: stg_transactions
# depends_on: stg_customers
```

**Shell files (.sh):**
```bash
# depends_on: raw_transactions
```

Rules:
- One dependency per line.
- The pattern is: `(--|#) depends_on: <asset_name>` (with optional whitespace).
- Only lines matching this exact pattern are parsed. Other comments are ignored.
- Dependencies are parsed from the first 50 lines of the file only (don't scan entire large files).
- If a file declares a dependency on an asset that doesn't exist in the config, the parser returns an error listing the missing dependency.
- If a file has no `depends_on` lines, it has no dependencies (it's a root node).

### Multi-Output Assets

Not implemented in Phase 0. A single asset produces a single logical output. Multi-output is deferred to Phase 2.

### Graph Data Structure

```go
type Asset struct {
    Name         string
    Type         string   // "sql", "python", "shell", "dlt"
    Source       string   // relative file path
    DependsOn    []string // names of upstream assets
    DependedOnBy []string // names of downstream assets (computed)
}

type Graph struct {
    Assets    map[string]*Asset
    RootNodes []string // assets with no dependencies
}
```

The graph must support:
- Cycle detection (return error with the cycle path)
- Topological sort
- Finding all root nodes (no dependencies)
- Finding all descendants of a given node (for failure propagation)
- Validation: all dependencies reference existing assets

### DAG Executor

The executor takes a `Graph` and a `RunnerFunc` and executes all nodes respecting dependencies with maximum parallelism.

```go
type NodeResult struct {
    AssetName  string
    Status     string    // "success", "failed", "skipped"
    StartTime  time.Time
    EndTime    time.Time
    Duration   time.Duration
    Error      string    // empty if success
    Stdout     string    // captured stdout
    Stderr     string    // captured stderr
    ExitCode   int
}

type RunConfig struct {
    MaxParallel int
    Assets      []string // if empty, run entire graph. if specified, run only these and their dependencies.
}
```

**Execution algorithm:**

1. Compute the set of nodes to run. If `RunConfig.Assets` is specified, compute the subgraph: the specified assets plus all their ancestors (transitive dependencies). Otherwise, run the entire graph.
2. Initialize a counter per node: number of unresolved dependencies.
3. Create a semaphore (buffered channel) of size `MaxParallel`.
4. Put all root nodes (zero unresolved deps) into a "ready" queue.
5. For each ready node:
   a. Acquire the semaphore.
   b. Launch a goroutine that:
      - Calls the runner function for this node.
      - Records the result.
      - If success: for each dependent, decrement their unresolved counter. If counter hits 0, add to ready queue.
      - If failure: for each descendant (transitive), mark as "skipped".
      - Release the semaphore.
6. Wait for all nodes to complete (either succeeded, failed, or skipped).
7. Return all `NodeResult`s.

**Important behaviors:**
- A node only runs when ALL its dependencies have succeeded.
- If any dependency has failed or been skipped, the node is skipped (not failed — it never ran).
- The executor must be safe for concurrent access. Use mutexes or channels for the dependency counters and ready queue.
- The executor should track wall-clock time: total run start, total run end.

### Shell Node Runner

The only runner implemented in Phase 0. It executes a shell command via `os/exec`.

```go
func ShellRunner(asset *Asset, projectRoot string) NodeResult {
    // Construct command: sh -c "source_file_path"
    // Or if source is a .sh file: bash source_file_path
    // Working directory: projectRoot
    // Capture stdout and stderr separately
    // Capture exit code
    // Timeout: 5 minutes per node (configurable later)
    // Return NodeResult with all fields populated
}
```

Notes:
- The shell runner makes the source file executable if it isn't already.
- Environment variables: pass `GRANICUS_ASSET_NAME`, `GRANICUS_RUN_ID`, and `GRANICUS_PROJECT_ROOT` to the subprocess.
- Stdout and stderr are captured in memory (not streamed to files yet). If output exceeds 1MB, truncate and note truncation in the result.

### Structured Log Store

Each run gets a unique ID: `run_{timestamp}_{random_suffix}` (e.g., `run_20260224_143022_a7f3`).

Logs are stored in a directory:

```
.granicus/
└── runs/
    └── run_20260224_143022_a7f3/
        ├── run.json          # Run-level summary
        └── nodes.jsonl       # One JSON line per node result
```

**run.json:**
```json
{
  "run_id": "run_20260224_143022_a7f3",
  "pipeline": "revenue_daily",
  "start_time": "2026-02-24T14:30:22Z",
  "end_time": "2026-02-24T14:31:05Z",
  "duration_seconds": 43,
  "total_nodes": 20,
  "succeeded": 17,
  "failed": 1,
  "skipped": 2,
  "status": "completed_with_failures",
  "config": {
    "max_parallel": 10,
    "assets_filter": []
  }
}
```

Status values: `success` (all nodes succeeded), `completed_with_failures` (some failed/skipped), `failed` (executor error).

**nodes.jsonl** (one line per node, written as each node completes):
```json
{"asset": "raw_transactions", "status": "success", "start_time": "2026-02-24T14:30:22Z", "end_time": "2026-02-24T14:30:25Z", "duration_ms": 3012, "exit_code": 0, "error": "", "stdout_lines": 5, "stderr_lines": 0}
{"asset": "stg_transactions", "status": "failed", "start_time": "2026-02-24T14:30:25Z", "end_time": "2026-02-24T14:30:26Z", "duration_ms": 1205, "exit_code": 1, "error": "exit status 1", "stdout_lines": 0, "stderr_lines": 12}
{"asset": "revenue_summary", "status": "skipped", "start_time": "", "end_time": "", "duration_ms": 0, "exit_code": -1, "error": "skipped: dependency stg_transactions failed", "stdout_lines": 0, "stderr_lines": 0}
```

Notes:
- `stdout_lines` and `stderr_lines` are counts. Full stdout/stderr is stored in separate files if we need it later (Phase 1), but for Phase 0 just store in the JSONL as truncated strings.
- Actually, store full stdout and stderr in the JSONL entry up to 10KB each. Truncate beyond that with a `"[truncated]"` marker.
- The log store must be safe for concurrent writes (multiple goroutines writing node results simultaneously). Use a mutex around the JSONL writer.
- Provide functions: `WriteNodeResult(runID, result)`, `WriteRunSummary(runID, summary)`, `ReadRunSummary(runID)`, `ReadNodeResults(runID)`, `ListRuns()`.

### CLI

The CLI uses Go's `cobra` library (or just `flag` if you prefer minimal dependencies — use cobra, it's standard for Go CLIs).

Commands:

```
granicus run <config.yaml>
  --max-parallel N       Override max_parallel from config
  --assets a,b,c         Run only these assets and their dependencies
  --project-root /path   Project root directory (default: current directory)

granicus validate <config.yaml>
  --project-root /path   Validate config and graph, report any issues

granicus status [run_id]
  If run_id provided: show detailed status of that run
  If no run_id: show status of most recent run

granicus history
  --limit N              Show last N runs (default: 10)

granicus version
  Print version string
```

**`granicus run` output:**

```
Pipeline: revenue_daily
Assets: 20 (4 root nodes)
Max parallel: 10

[14:30:22] ● raw_transactions        started
[14:30:22] ● dim_customers           started
[14:30:25] ✓ raw_transactions        success (3.0s)
[14:30:25] ● stg_transactions        started
[14:30:27] ✓ dim_customers           success (5.2s)
[14:30:26] ✗ stg_transactions        failed (1.2s) — exit status 1
[14:30:26] ○ revenue_summary         skipped — dependency failed

Run complete: 17 succeeded, 1 failed, 2 skipped (43s total)
Run ID: run_20260224_143022_a7f3
```

Symbols: `●` running, `✓` success, `✗` failed, `○` skipped.

Colors: green for success, red for failure, yellow for skipped, white for running. Use a color library that respects `NO_COLOR` environment variable.

**`granicus validate` output:**

```
Pipeline: revenue_daily
Assets: 20
Dependencies: 35
Root nodes: 4

Validation:
  ✓ No cycles detected
  ✓ All dependencies resolved
  ✓ All source files exist
  ✓ No duplicate asset names

Graph is valid.
```

Or if there are errors:

```
Validation:
  ✗ Missing dependency: stg_transactions depends on "raw_events" which is not defined
  ✗ Source file not found: sql/missing_file.sql
  ✗ Cycle detected: A → B → C → A

2 assets have errors. Graph is invalid.
```

**`granicus status` output:**

```
Run: run_20260224_143022_a7f3
Pipeline: revenue_daily
Status: completed_with_failures
Duration: 43s
Nodes: 17 succeeded, 1 failed, 2 skipped

Failed:
  stg_transactions — exit status 1

Skipped:
  revenue_summary — dependency stg_transactions failed
  check_revenue — dependency revenue_summary failed
```

**`granicus history` output:**

```
Run ID                           Pipeline        Status                  Duration  Date
run_20260224_143022_a7f3         revenue_daily   completed_with_failures 43s       2026-02-24 14:30
run_20260224_120000_b2e1         revenue_daily   success                 38s       2026-02-24 12:00
run_20260223_143015_c9d4         revenue_daily   success                 41s       2026-02-23 14:30
```

## Testing Requirements

### Unit Tests

**graph/graph_test.go:**
1. `TestParseConfig_ValidYAML` — parse a valid config, verify all assets loaded with correct types and sources
2. `TestParseConfig_MissingFields` — config with missing required fields returns appropriate error
3. `TestParseDependencies_SQL` — SQL file with `-- depends_on:` lines parsed correctly
4. `TestParseDependencies_Python` — Python file with `# depends_on:` lines parsed correctly
5. `TestParseDependencies_NoDeps` — file with no dependency declarations returns empty list
6. `TestParseDependencies_OnlyFirst50Lines` — dependency on line 51 is not parsed
7. `TestBuildGraph_Valid` — valid config + deps builds correct graph with correct edges
8. `TestBuildGraph_CycleDetection` — graph with cycle A→B→C→A returns error with cycle path
9. `TestBuildGraph_MissingDependency` — dependency on non-existent asset returns error
10. `TestBuildGraph_DuplicateAssetName` — duplicate names return error
11. `TestBuildGraph_RootNodes` — correctly identifies root nodes
12. `TestBuildGraph_Descendants` — correctly computes transitive descendants of a node
13. `TestBuildGraph_Subgraph` — given a list of target assets, computes correct subgraph (targets + all ancestors)

**executor/executor_test.go:**
1. `TestExecute_LinearChain` — A→B→C runs in order
2. `TestExecute_ParallelRoots` — A, B, C (no deps) all run concurrently
3. `TestExecute_DiamondDependency` — A→B, A→C, B→D, C→D. D runs only after both B and C complete.
4. `TestExecute_FailurePropagation` — B fails, C (depends on B) is skipped, A (no dep on B) succeeds
5. `TestExecute_MaxParallel` — with max_parallel=2 and 5 independent nodes, at most 2 run concurrently (verify via timing)
6. `TestExecute_AllFail` — all root nodes fail, everything downstream is skipped
7. `TestExecute_EmptyGraph` — no nodes, returns immediately with empty results
8. `TestExecute_SingleNode` — one node, no deps, runs and succeeds
9. `TestExecute_SubgraphExecution` — specify 2 assets, verify only those assets and their ancestors run
10. `TestExecute_LargeGraph` — 100 nodes with chain dependencies, completes correctly
11. `TestExecute_Benchmark100Nodes` — 100 no-op nodes, total overhead < 1 second

**runner/runner_test.go:**
1. `TestShellRunner_Success` — runs `echo hello`, captures stdout, exit code 0
2. `TestShellRunner_Failure` — runs `exit 1`, captures exit code 1
3. `TestShellRunner_Stderr` — runs a command that writes to stderr, captured separately
4. `TestShellRunner_Timeout` — command that sleeps 10 minutes, times out at 5 minutes (use a shorter timeout for test)
5. `TestShellRunner_EnvVars` — verify GRANICUS_ASSET_NAME and GRANICUS_RUN_ID are set
6. `TestShellRunner_LargeOutput` — command that outputs 2MB, verify truncation at 10KB

**logging/store_test.go:**
1. `TestWriteAndReadNodeResult` — write a result, read it back, verify match
2. `TestWriteAndReadRunSummary` — write summary, read back, verify match
3. `TestConcurrentWrites` — 20 goroutines writing simultaneously, all results present
4. `TestListRuns` — create 5 runs, list returns all 5 in reverse chronological order
5. `TestRunDirectory_CreatedAutomatically` — writing to a new run_id creates the directory

### Integration Test

Create a test fixture directory with:
- A config.yaml defining 10 assets
- Shell scripts that: some succeed (echo + sleep 0.1s), some fail (exit 1), various dependency patterns
- Run `granicus run config.yaml` programmatically
- Verify:
  - Correct nodes succeeded
  - Failed nodes produced correct error
  - Skipped nodes were correctly identified
  - Logs written correctly
  - Total time is reasonable (parallel execution observed)

### Benchmark Test

100 shell scripts that each run `echo "done"` (near-zero work). Complex dependency graph (10 layers of 10 nodes each, each layer depends on the previous).

Measure: total wall-clock time from `granicus run` start to completion. Target: < 2 seconds. The actual work is ~0, so this measures pure orchestrator overhead.

## Dependencies (Go Modules)

- `github.com/spf13/cobra` — CLI framework
- `gopkg.in/yaml.v3` — YAML parsing
- `github.com/fatih/color` — Terminal colors (respects NO_COLOR)
- Standard library for everything else (os/exec, encoding/json, sync, time, path/filepath)

Do NOT add dependencies beyond these unless absolutely necessary. Keep the binary small and the dependency tree shallow.

## What NOT to Build in Phase 0

- SQL, Python, or dlt node runners (shell only)
- Incremental loading / interval tracking
- Cron scheduling or webhook triggers
- Environment config (dev/prod)
- Checks as a distinct concept (checks are just shell scripts for now)
- Multi-output assets
- Config file discovery (explicit config path only)
- Any UI
- Any database (file-based logs only)

## Definition of Done

1. `granicus run config.yaml` executes a 10-asset pipeline with mixed success/failure and produces correct logs
2. `granicus validate config.yaml` catches cycles, missing deps, missing files
3. `granicus status` and `granicus history` show correct information from the log store
4. The 100-node benchmark completes in < 2 seconds
5. All unit tests pass
6. The binary compiles to a single file < 15MB
7. `go vet` and `go test ./...` pass with no warnings
