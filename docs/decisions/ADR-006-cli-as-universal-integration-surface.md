# ADR-006: CLI as the Universal Integration Surface

## Status

Accepted

## Date

2026-03-20

## Context

Multiple integration points need to interact with Granicus: MCP servers, VS Code extensions, GitHub Actions, Slack bots, and potentially others. Each integration could build its own client against the API, or there could be a shared layer that all integrations use.

## Decision

Every CLI command maps to a well-defined function with structured I/O. Any integration (MCP server, VS Code extension, GitHub Action, Slack bot) is a thin wrapper over the same command layer. Mode detection, config, and auth are handled by the shared layer — integrations don't need to know local vs cloud.

## Reasoning

The CLI already implements mode detection (ADR-002), cloud/local switching (ADR-003), and feature gating (ADR-004). Building integrations on top of the CLI means all that logic is reused, not reimplemented. Each integration is a thin adapter that translates its input format into CLI commands and renders the output in its native format.

This also means the CLI is always the most complete and tested interface — integrations inherit its correctness.

## Consequences

### Easier
- New integrations are trivial to build — just map integration-specific I/O to CLI commands
- All integrations get mode detection, auth, and config for free
- CLI is always the canonical interface — one place to test, one place to fix bugs

### Harder / Constraints
- CLI must have structured output (JSON) for machine consumption, not just human-readable text
- CLI startup time matters more — integrations call it frequently
- Breaking CLI changes break all integrations simultaneously

## Related

- ADR-001: Cloud = orchestration control plane
- ADR-003: CLI is a thin HTTP client in cloud mode
- BL-284 spec: Granicus Cloud architecture
