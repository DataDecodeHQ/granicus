# Checklist

## Phase 1 — Infrastructure

- [ ] `terraform init`
- [ ] `terraform import` every existing GCP resource
- [ ] Write `.tf` declarations for every imported resource
- [ ] `terraform plan` — must show no changes
- [ ] `python scripts/check_infrastructure.py ./infra --skip-plan` — fix all failures
- [ ] `python scripts/check_infrastructure.py ./infra` — must exit 0
- [ ] `python scripts/run_dag_agent.py --type infra --message "initial"`
- [ ] Review `dag/infrastructure_dag.json` — resolve any `cannot_infer` flags and bridge gaps

---

## Phase 2 — Code

- [ ] `python scripts/check_codebase.py . --warn-only` — review the full picture
- [ ] Fix all `MUTABLE_GLOBAL` findings
- [ ] Review all `MODULE_LEVEL_LOGIC` findings — fix RISK category only
- [ ] Fix all `INLINE_SIDE_EFFECT_FILE_IO` findings
- [ ] `python scripts/check_codebase.py .` — must exit 0
- [ ] `python scripts/run_dag_agent.py --type code --message "initial"`
- [ ] Review `dag/static_dag.json` — fix any `cannot_infer` flags
- [ ] Re-run agent until all confidence scores are above 0.70

---

## Phase 3 — Security

- [ ] `python scripts/check_security.py` — fix all CRITICAL findings
- [ ] `python scripts/run_dag_agent.py --type security --message "initial"`
- [ ] Review `dag/security_review.json` — fix or document all CRITICAL and HIGH findings
- [ ] `python scripts/check_security.py` — must exit 0

---

## Release

- [ ] `python scripts/check_infrastructure.py ./infra`
- [ ] `python scripts/check_codebase.py .`
- [ ] `python scripts/check_security.py`
- [ ] `pytest`
- [ ] Release

---

## Every Code Change

- [ ] `python scripts/check_codebase.py .`
- [ ] `pytest`
- [ ] `python scripts/run_dag_agent.py --type code --diff-mode --message "<what changed>"`
- [ ] If change touches auth, PII, or external calls: `python scripts/check_security.py`

## Every Infrastructure Change

- [ ] `python scripts/check_infrastructure.py ./infra`
- [ ] `terraform plan` — review diff
- [ ] `terraform apply`
- [ ] `python scripts/run_dag_agent.py --type infra --message "<what changed>"`
- [ ] `python scripts/check_security.py`

## When Investigating a Bug

- [ ] Get trace ID from Cloud Trace or Cloud Run logs
- [ ] `python scripts/compare_trace_to_dag.py --trace-id <id>`
- [ ] Fix the divergence
- [ ] `python scripts/check_codebase.py .`
- [ ] `python scripts/run_dag_agent.py --type code --diff-mode --message "fix: <description>"`
