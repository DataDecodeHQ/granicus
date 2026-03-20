# Patterns

Intentional design patterns for this codebase.

---

## Asset Layer Convention

**Rule:** Name every asset with a layer prefix (`src_`, `stg_`, `int_`, `ent_`, `rpt_`) and set the `layer` field in config to match. Each layer represents a defined problem space for diagnostics.

**Why:** When an asset fails, the layer narrows the investigation. A `stg_` failure means source data quality or extraction. An `int_` failure means transformation logic. Layers are diagnostic boundaries -- they constrain what could have gone wrong.

**Layers:**

| Prefix | Layer | What it does | Failure means |
|--------|-------|-------------|---------------|
| `src_` | source | Raw data as received from external systems. No transformation. | Source system is down, schema changed, credentials expired |
| `stg_` | staging | Light cleaning, type casting, deduplication. First point of validation. | Source data quality issue -- nulls, bad formats, unexpected values |
| `int_` | intermediate | Business logic transformations, joins, aggregations. | Transformation logic bug, upstream schema drift, join key mismatch |
| `ent_` | entity | Canonical business entities. Stable grain, stable schema. | Entity definition changed, upstream intermediate logic broke |
| `rpt_` | report | Shaped for consumption -- dashboards, exports, APIs. | Report logic bug, or upstream entity changed |

**Example:** `stg_event_consultation_disqualified` -- staging layer, so checks here should validate source data shape, not business logic.

**Exception:** Default checks (row count, null rate) apply to all layers. Layer-specific check rules are planned but not yet enforced.

**Direction:** Over time, each layer should have defined check requirements (e.g., staging must have schema conformance checks, entities must have grain uniqueness checks). The goal is to make layers as static as possible so failures are immediately classifiable.

---

## Three-Layer Model

**Rule:** Classify every package, type, and concept into exactly one primary layer: Infrastructure, Pipeline, or Data. New packages and public types use the layer prefix (`infra_`, `pipe_`, `data_`).

**Why:** The codebase has three distinct concerns that change for different reasons and at different rates. Without explicit layer membership, names collide across layers (e.g., `source` means pipeline versioning in one package and raw data in the asset layer convention). Making the layer visible in the name prevents misinterpretation and catches cross-layer violations in review. See ADR-007.

**Layers:**

| Layer | What it covers | Examples (current) |
|-------|---------------|--------------------|
| **Infrastructure** | Compute, storage, auth, scheduling, networking | `pool`, `doctor`, `backup`, `gc`, `logging`, `archive`, `server` |
| **Pipeline** | Definition versioning, config, validation, graph construction | `config`, `graph`, `validate`, `checker`, `migrate`, `pipe_registry` |
| **Data** | Asset execution, intervals, events, checks, results | `executor`, `runner`, `types`, `events`, `state`, `result`, `monitor`, `testmode`, `rerun` |

**Boundary rule:** Some packages are primarily one layer but create clients for another (e.g., `state` is data-layer but creates Firestore clients). The prefix reflects the primary layer. Cross-layer dependencies at boundary points are expected -- the package name tells you what the package is *for*, not every service it touches.

**Example:** The `pipe_registry` package implements `PipelineRegistry` -- it manages pipeline definition versioning (register, activate, fetch). It belongs to the Pipeline layer, not Data.

**Exception:** Existing packages are renamed incrementally via backlog items, not all at once.

---

## Observation Modes

**Rule:** Classify every observation feature as either Pulse (real-time signals and responses during execution) or Analysis (historical trends queried after the fact). Pulse features emit signals and trigger responses. Analysis features query accumulated history and never block execution.

**Why:** Observation touches all three layers but serves two different audiences at two different timescales. Pulse serves the on-call operator who needs to know right now. Analysis serves the team reviewing pipeline reliability and cost over time. Mixing the two leads to alert fatigue (analysis findings triggering pages) or blind spots (pulse checks not alerting because they were designed for batch review).

**Pulse (signals + responses):**
- Check result signals (severity + blocking) emitted after asset checks run
- Executor responses: halt run, skip downstream subtree, or continue based on signal matrix
- Alert manager: webhook notifications fired in response to failure signals
- Schema stability drift detection + threshold violation flags

**Analysis:**
- `granicus stats` -- node reliability over time
- `granicus history` / `granicus failures` -- run history queries
- `events.GetRunCostSummary()` -- BQ cost per run
- Metric snapshot time series -- stored in SQLite for trend comparison

**Key distinction:** A check is data-layer execution -- it runs validation and produces a result. The pulse is the signal from that result and the system's response to it. The check's SQL may change because the data model changed. The signal's severity may change because the team's risk tolerance changed. The response's alert routing may change because the on-call rotation changed. These are independent concerns.

**Example:** `monitor.CompareSnapshots()` is pulse -- it runs after metric collection and flags threshold violations that may trigger alerts. `granicus stats --node stg_orders --since 30d` is analysis -- it aggregates historical event data to show reliability trends.

**Exception:** None yet. The boundary is clear in the current code.

---

## Check Signal Response

**Rule:** Separate the act of checking from the signal it produces and the response to that signal. A check is data-layer execution. The signal (severity + blocking) is the pulse. The response (halt, skip, alert, log) is the system's reaction to the pulse.

**Why:** Checks, signals, and responses change for different reasons. A check's SQL may change because the data model changed. A signal's severity may change because the team's risk tolerance changed. A response's alert routing may change because the on-call rotation changed. Coupling any two of these makes all three harder to modify.

**Signal matrix:**

| Severity | Blocking | Pipeline effect | Alert |
|----------|----------|----------------|-------|
| `critical` | (always enforced) | Halts entire run + skips downstream subtree | Yes |
| `error` | `true` | Skips downstream subtree | Yes |
| `error` | `false` | Check fails, downstream continues | Yes |
| `warning` | (ignored) | No pipeline impact | Configurable |
| `info` | (ignored) | No pipeline impact | No |

**Example:** `check:A:validate` is a SQL check (data layer). It returns `severity: error, blocking: true` (signal). The executor skips B and C which depend on A, and the alert manager fires a webhook (response).

**Exception:** None. `critical` always blocks and halts regardless of the `Blocking` field -- this is intentional and not configurable.
