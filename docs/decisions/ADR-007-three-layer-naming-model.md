# ADR-007: Three-Layer Naming Model (Infrastructure, Pipeline, Data)

## Status

Accepted (initial renames complete)

## Date

2026-03-20

## Context

The codebase has three distinct conceptual layers -- infrastructure (where things run), pipeline (what to run), and data (what flows through) -- but nothing makes this explicit. Terms collide across layers: the `source` package manages pipeline versioning but reads like a data concept, `Connection` is an infrastructure concern dressed in pipeline-layer config, and the `src_` asset prefix uses "source" to mean raw data. A developer reading the code must already know which layer a package belongs to in order to interpret its names correctly.

As the codebase grows and more contributors touch it, this implicit knowledge becomes a liability. New code inherits whatever naming the author reaches for, and the layers blur further.

## Decision

We adopt a three-layer model as the organizing principle for all Granicus code. Every package, type, and concept belongs to exactly one primary layer:

| Layer | What it covers | Prefix (new code) |
|-------|---------------|-------------------|
| **Infrastructure** | Compute, storage, auth, scheduling, networking. Where things run and how they connect. | `infra_` |
| **Pipeline** | Definition versioning, config loading, validation, graph construction, push/activate lifecycle. What to run. | `pipe_` |
| **Data** | Asset execution, intervals, events, checks, transformations, results. What flows through. | `data_` |

The prefix convention applies to new packages and public types going forward. Existing code is renamed incrementally via targeted backlog items, not a big-bang refactor.

## Reasoning

Three layers is the minimum that captures the real boundaries. Two layers (e.g., "platform" and "application") conflate infrastructure with pipeline mechanics. Four or more layers add precision nobody needs yet.

The prefix convention makes layer membership visible at the call site without requiring knowledge of the package hierarchy. `source.Register()` is ambiguous; `pipe_registry.Register()` is not. This is especially valuable in cross-layer code (e.g., the executor calling dispatch) where multiple layers interact in the same function.

We chose incremental adoption over a big-bang rename because the codebase has 471 functions across 84 packages. A single rename PR would be unreviewable and high-risk. Targeted renames (starting with the worst offenders like `source` and `Connection`) deliver most of the clarity with manageable blast radius.

The tradeoff: during the transition period, old and new naming conventions coexist. This is acceptable because the three-layer model is documented (PATTERNS.md) and the glossary maps terms to their layer, so the implicit knowledge is captured even before every rename lands.

## Consequences

### Easier
- New contributors can identify what layer they're touching from the name alone
- Code review catches layer violations (a `data_` type creating infrastructure clients is a visible smell)
- Glossary terms naturally group by layer
- Debugging narrows faster when layer boundaries are explicit

### Harder / Constraints
- Transition period has mixed naming (old packages without prefix, new ones with)
- Every new package/type requires a layer classification decision
- Some packages genuinely span layers (e.g., `state` is data-layer but creates Firestore clients) -- the prefix reflects the primary layer, and cross-layer dependencies are expected at boundary points

## Alternatives Considered

**No convention -- rely on package documentation.** Rejected because documentation drifts from code. The `source` package has accurate doc comments (`PipelineSource`) but the package name still misleads at every import site.

**Full prefix on all existing code immediately.** Rejected due to risk. 471 functions across 84 packages in a single rename would break every import path, every test, and every external reference. Incremental adoption is safer.

**Two layers (platform / application).** Rejected because it conflates infrastructure (Cloud Run, Firestore) with pipeline mechanics (config versioning, graph construction). These change for different reasons and at different rates.

**Layer-as-directory (internal/infra/, internal/pipe/, internal/data/).** Considered and may be a future step, but moving packages is more disruptive than renaming them. Start with naming, consider restructuring later if the naming convention proves valuable.

## Related

- PATTERNS.md: Three-Layer Model pattern (added with this ADR)
- GLOSSARY.md: Terms should note their layer
- Backlog: BL-294 `source` -> `pipe_registry` (complete)
- Backlog: BL-295 `Connection` -> `Resource` (complete)
