# ADR-002: Mode Detection — Layered Config with API Key as the Gate

## Status

Accepted

## Date

2026-03-20

## Context

The CLI must work in both local and cloud modes. Users need a predictable way to control which mode is active, and the system needs a clear signal to switch between them. Configuration comes from multiple sources (flags, env vars, project config, user config) and these must compose without surprises.

## Decision

We use a layered configuration resolution order: CLI flags > env vars > project config (`.granicus/config.json`) > user config (`~/.granicus/config.json`) > default (local). Cloud mode activates when an API key is resolvable. The user-global file holds auth (never committed); the project file holds overrides (safe to commit).

## Reasoning

Layered config is a well-understood pattern (Terraform, Docker, git). API key as the gate is simple and unambiguous — either you have credentials or you don't. Splitting auth into user-global config prevents accidental commits of API keys while allowing project-level overrides to live in version control.

## Consequences

### Easier
- Mode is deterministic and inspectable — users can reason about which config wins
- Project config is safe to commit (no secrets)
- CI/CD can set mode via env vars without touching config files

### Harder / Constraints
- Must document resolution order clearly — layered config confuses users when they don't know which layer is active
- Debugging requires a `granicus config show` or similar introspection command
- API key presence is binary — no partial cloud mode

## Alternatives Considered

**Explicit mode flag required** — forces users to always specify `--cloud` or `--local`. Verbose and error-prone in scripts. Rejected because the API key presence is a cleaner implicit signal with `--local` as an explicit escape hatch.

**Single config file** — simpler but conflates auth with project settings, risking credential leaks in version control.

## Related

- ADR-005: No fallback from cloud to local
- BL-284 spec: Granicus Cloud architecture
