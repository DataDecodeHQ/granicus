# ADR-005: No Fallback from Cloud to Local

## Status

Accepted

## Date

2026-03-20

## Context

When the CLI is configured for cloud mode but the cloud API is unreachable (network issue, outage), the system could either fall back to local execution or fail explicitly. This decision has significant implications for data consistency and user trust.

## Decision

If cloud is configured but unreachable, the CLI errors. No silent fallback. The `--local` flag is the explicit escape hatch when users intentionally want local execution.

## Reasoning

Silent fallback is dangerous. If a user expects cloud execution (with its scheduling, history, and audit trail) but silently gets local execution, the results may differ and the audit trail has a gap. Terraform, Pulumi, Prefect, Dagster, and Supabase all follow this pattern — none implement automatic fallback. Explicit failure is safer than implicit mode switching.

## Consequences

### Easier
- Users always know which mode ran — no ambiguity in execution history
- Debugging is straightforward — cloud errors are cloud errors, not masked by local fallback
- Audit trail integrity is preserved

### Harder / Constraints
- Cloud outages block all cloud-mode users (no graceful degradation)
- Users must explicitly add `--local` if they want to work offline
- May frustrate users during transient network issues

## Alternatives Considered

**Silent fallback to local** — better availability but creates data consistency risks. A pipeline that runs locally during an outage may produce different results than the cloud version (different credentials, different compute environment). The audit trail would show a gap or inconsistency.

**Fallback with warning** — a middle ground, but warnings are easily missed in CI/CD logs. The risk of unnoticed mode switching outweighs the convenience.

## Related

- ADR-002: Mode detection — layered config
- ADR-004: Cloud-only features gate cleanly
- BL-284 spec: Granicus Cloud architecture
