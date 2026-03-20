# ADR-003: CLI is a Thin HTTP Client in Cloud Mode

## Status

Accepted (current implementation partially conforms — see Conformance Note)

## Date

2026-03-20

## Context

The CLI needs to work in both local and cloud modes. In local mode, commands execute logic in-process against local files and BigQuery. In cloud mode, the same commands need to hit the cloud API. The question is how much logic the CLI should contain when operating in cloud mode.

## Decision

Every CLI command maps to an API endpoint. In local mode, the same commands execute equivalent logic in-process. The CLI binary contains both code paths. In cloud mode, the CLI is a thin HTTP client -- it serializes arguments, sends the request, and renders the response.

The CLI in cloud mode holds only an API key. All service credentials (GCS, Firestore, BigQuery, Secret Manager) live on the engine. The CLI never creates cloud service clients directly in cloud mode.

## Reasoning

A thin client in cloud mode means the CLI never diverges from the API. If the CLI contained business logic in cloud mode, we would have two implementations to keep in sync. The API is the source of truth; the CLI in cloud mode is just a user-friendly way to call it.

Local mode keeps the full logic in-process because requiring a running server for local development would be a poor developer experience.

Industry standard (Airflow, Dagster, Prefect, Terraform Cloud) routes all operations -- including reads -- through the API for five reasons:

1. **Single auth boundary** -- one place to enforce who can do what
2. **Audit trail** -- every operation logged in one place
3. **Credential simplification** -- CLI only needs an API key, not service account credentials for every cloud service
4. **Integration parity** -- CLI, CI/CD, MCP server, and future integrations all use the same API surface
5. **Policy enforcement** -- API can reject operations the raw service cannot (e.g., block activating a version that hasn't passed checks)

The tradeoff (extra hop for reads, engine must be running) is the accepted cost across the industry.

## Conformance Note

**Current state does not fully conform.** Only mutating trigger operations (trigger, validate, run, status) route through the engine API via HTTP. Most cloud-gated commands (push, activate, versions, diff, history, events, failures, stats, cloud-status, intervals) create GCS and Firestore clients directly in the CLI binary, bypassing the API.

This was a pragmatic shortcut -- these commands work without a running engine, which was useful during early development. The migration path is:

1. **Phase 1 (mutating operations):** push, activate, gc -- route through API. These benefit most from policy enforcement and audit logging.
2. **Phase 2 (read operations):** history, events, failures, stats, versions, intervals, cloud-status, diff -- route through API. Completes the thin client migration.

See BL-297 for tracking.

## Consequences

### Easier
- API and CLI are always in sync in cloud mode -- no dual-implementation drift
- CLI updates for cloud features are trivial (new endpoint mapping)
- Testing cloud mode reduces to testing the API
- Integrations are trivial to build (same API)
- Users only need an API key, not cloud service credentials

### Harder / Constraints
- CLI binary is larger because it bundles both local logic and HTTP client code
- Must maintain a complete command-to-endpoint mapping table
- Error handling differs between modes (HTTP errors vs in-process errors)
- Read-only operations require the engine to be running in cloud mode
- Every new CLI command requires a corresponding API endpoint

## Alternatives Considered

**CLI always calls API (even locally via localhost)** -- clean architecture but requires running a local server, which hurts developer experience and complicates installation.

**Separate CLI binaries for local and cloud** -- eliminates the mode-switching complexity but doubles distribution and testing burden. Users would need to install the right binary.

**CLI talks directly to cloud services (current state)** -- simpler in the short term, but creates thick clients, scatters credentials, and makes integrations harder. Every new consumer (MCP, CI/CD, Slack) would need its own cloud service clients. Does not scale.

## Related

- ADR-001: Cloud = orchestration control plane
- ADR-004: Cloud-only features gate cleanly
- ADR-006: CLI as the universal integration surface
- BL-297: Migrate cloud CLI commands to thin HTTP client
