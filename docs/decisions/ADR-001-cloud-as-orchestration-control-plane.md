# ADR-001: Cloud = Orchestration Control Plane, Compute is Flexible

## Status

Accepted

## Date

2026-03-20

## Context

Granicus needs a cloud offering that coordinates pipeline execution — scheduling, retry, history, logs, DAG validation, config versioning. The question is whether the cloud layer should also own compute, or whether compute should be decoupled so it can run on customer infrastructure in the future.

Customers provide destination credentials (BigQuery, GCS). The cloud layer needs to provide reliable orchestration without locking customers into a single compute model.

## Decision

Granicus Cloud always coordinates. Compute initially runs on Granicus-managed Cloud Run, but may dispatch to customer infrastructure in the future. The customer provides destination credentials (BQ, GCS); Granicus Cloud provides execution, scheduling, retry, history, logs, DAG validation, and config versioning.

## Reasoning

Decoupling orchestration from compute allows the product to start simple (managed compute) while preserving optionality for enterprise customers who need data to stay on their infrastructure. The orchestration plane is where the product value lives — scheduling, retry logic, DAG validation, versioned config. Compute is commodity.

## Consequences

### Easier
- Enterprise customers can adopt Granicus without moving data off their infrastructure (future)
- Cloud Run gives a fast initial path without building custom compute infra
- Orchestration logic is testable independently of where compute runs

### Harder / Constraints
- Must design all orchestration APIs to be compute-location-agnostic from day one
- Customer-managed compute introduces credential and networking complexity
- Monitoring and log aggregation become harder when compute is distributed

## Alternatives Considered

**Fully managed compute only** — simpler to build but blocks enterprise customers who cannot send data outside their VPC. Limits market.

**Compute-first, orchestration later** — inverts the value prop. Orchestration is the differentiator; raw compute is not.

## Related

- ADR-003: CLI is a thin HTTP client in cloud mode
- ADR-006: CLI as the universal integration surface
- BL-284 spec: Granicus Cloud architecture
