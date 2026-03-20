# ADR-004: Cloud-Only Features Gate Cleanly

## Status

Accepted

## Date

2026-03-20

## Context

Some features (audit log, team management, hosted scheduling) only exist in the paid cloud tier. The CLI still needs to present a coherent interface — users should be able to discover these features and understand what they need to unlock them, rather than having commands silently missing.

## Decision

Commands that only exist in the paid tier still appear in the CLI. In local mode they return a clear error with guidance (e.g., `Error: audit-log requires Granicus Cloud. Run 'granicus login' to connect`). The API surface is the superset; local mode implements a subset.

## Reasoning

Hiding commands creates confusion — users don't know what's available. Showing commands with clear gating messages serves as both documentation and a natural upgrade path. This pattern is well-established in tools like GitHub CLI (`gh`) where enterprise features are visible but gated.

## Consequences

### Easier
- Users discover the full feature set from the CLI itself
- Upgrade path is self-documenting
- No conditional CLI builds or feature-flag complexity in command registration

### Harder / Constraints
- Must write clear, helpful error messages for every gated command
- CLI help output shows commands users may not be able to use — could feel noisy
- Must keep gating logic in sync as features move between tiers

## Alternatives Considered

**Hide cloud-only commands in local mode** — cleaner local experience but users can't discover features without reading docs. Also requires conditional command registration, which adds complexity.

**Separate plugin system for cloud features** — over-engineered for the current scope. Plugin architectures add distribution and versioning complexity.

## Related

- ADR-003: CLI is a thin HTTP client in cloud mode
- ADR-005: No fallback from cloud to local
- BL-284 spec: Granicus Cloud architecture
