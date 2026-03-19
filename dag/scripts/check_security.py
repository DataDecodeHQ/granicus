#!/usr/bin/env python3
"""
Security Pre-Process Checker
------------------------------
Deterministic security checks against the annotated static DAG and
infrastructure DAG. Runs before the security review agent — catches
clear violations that don't require LLM judgment.

Requires both DAGs to have been produced with security annotations:
    python scripts/run_dag_agent.py --type code
    python scripts/run_dag_agent.py --type infra

Usage:
    python scripts/check_security.py
    python scripts/check_security.py --warn-only
    python scripts/check_security.py --dag-dir ./dag
"""

import sys
import json
import logging
import argparse
from pathlib import Path
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Data structures
# ─────────────────────────────────────────────

@dataclass
class Finding:
    level: str       # CRITICAL | HIGH | MEDIUM | LOW | INFO
    rule: str
    location: str
    message: str


@dataclass
class SecurityCheckResult:
    findings: list[Finding] = field(default_factory=list)

    def add(self, level: str, rule: str, location: str, message: str) -> None:
        """Record a security finding."""
        self.findings.append(Finding(level, rule, location, message))

    def by_level(self, level: str) -> list[Finding]:
        """Return findings filtered by severity level."""
        return [f for f in self.findings if f.level == level]


SENSITIVE_LEVELS = {"pii", "sensitive", "credentials", "critical"}


# ─────────────────────────────────────────────
# DAG loader
# ─────────────────────────────────────────────

def load_dag(path: Path, name: str) -> dict:
    """Load a DAG JSON file, exit with a helpful message if missing."""
    if not path.exists():
        dag_type = "code" if "static" in path.name else "infra"
        logger.info(f"\n  ✗ {name} not found at {path}")
        logger.info(f"    Run: python scripts/run_dag_agent.py --type {dag_type}")
        sys.exit(1)
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        logger.info(f"\n  ✗ Could not parse {name}: {e}")
        sys.exit(1)


# ─────────────────────────────────────────────
# Graph traversal helpers
# ─────────────────────────────────────────────

def build_call_map(code_dag: dict) -> dict[str, list[str]]:
    """Build a forward adjacency map from edges: {caller: [callee, ...]}."""
    call_map: dict[str, list[str]] = {}
    for edge in code_dag.get("edges", []):
        src = edge.get("from", "")
        dst = edge.get("to", "")
        if src and dst:
            call_map.setdefault(src, []).append(dst)
    return call_map


def reachable_from(start: str, call_map: dict[str, list[str]]) -> set[str]:
    """Return all function names reachable from start via forward edges."""
    visited: set[str] = set()
    stack = [start]
    while stack:
        node = stack.pop()
        if node in visited:
            continue
        visited.add(node)
        stack.extend(call_map.get(node, []))
    return visited


# ─────────────────────────────────────────────
# Individual checks — code DAG
# ─────────────────────────────────────────────

def check_public_entry_to_sensitive_data(
        code_dag: dict, result: SecurityCheckResult) -> None:
    """
    CRITICAL: A public entry point reachable to sensitive data with no auth
    boundary is an immediate exposure risk.
    """
    functions = code_dag.get("functions", {})
    entry_points = set(code_dag.get("entry_points", []))
    call_map = build_call_map(code_dag)

    for ep in entry_points:
        fn = functions.get(ep, {})
        security = fn.get("security", {})
        trust_level = security.get("trust_level")

        if trust_level != "public":
            continue

        reachable = reachable_from(ep, call_map)
        for fn_name in reachable - {ep}:
            target = functions.get(fn_name, {})
            target_sec = target.get("security", {})
            sensitivity = target_sec.get("data_sensitivity", "public")
            auth_required = target_sec.get("auth_required", False)

            if sensitivity in SENSITIVE_LEVELS and not auth_required:
                result.add(
                    "CRITICAL",
                    "PUBLIC_ENTRY_REACHES_SENSITIVE_DATA",
                    ep,
                    f"Public entry point '{ep}' can reach '{fn_name}' "
                    f"({sensitivity} data) with no auth boundary declared"
                )
                break  # one finding per entry point


def check_low_confidence_sensitive(
        code_dag: dict, result: SecurityCheckResult) -> None:
    """
    HIGH/MEDIUM: Sensitive data in low-confidence functions cannot be
    verified to be handled safely — the contract is unknown.
    """
    functions = code_dag.get("functions", {})

    for fn_name, fn_data in functions.items():
        security = fn_data.get("security", {})
        sensitivity = security.get("data_sensitivity", "public")
        confidence = fn_data.get("confidence", 1.0)

        if sensitivity not in SENSITIVE_LEVELS:
            continue

        if confidence < 0.50:
            result.add(
                "HIGH",
                "CANNOT_INFER_SENSITIVE_FUNCTION",
                fn_name,
                f"'{fn_name}' handles {sensitivity} data but confidence is "
                f"{confidence:.2f} — contract cannot be verified. "
                f"Reason: {fn_data.get('confidence_reason', 'unknown')}"
            )
        elif confidence < 0.70:
            result.add(
                "MEDIUM",
                "LOW_CONFIDENCE_SENSITIVE_FUNCTION",
                fn_name,
                f"'{fn_name}' handles {sensitivity} data with uncertain confidence "
                f"({confidence:.2f}). "
                f"Reason: {fn_data.get('confidence_reason', 'unknown')}"
            )


def check_credentials_in_return_values(
        code_dag: dict, result: SecurityCheckResult) -> None:
    """
    HIGH: Credentials in return values risk propagating further than intended
    and appearing in logs or downstream systems.
    """
    for edge in code_dag.get("edges", []):
        security = edge.get("security", {})
        if (edge.get("transfer_type") == "return"
                and security.get("carries_credentials", False)):
            result.add(
                "HIGH",
                "CREDENTIALS_IN_RETURN_VALUE",
                edge.get("id", "unknown_edge"),
                f"Edge {edge.get('id')} returns credential material from "
                f"'{edge.get('from')}' to '{edge.get('to')}' — "
                f"credentials should not travel as return values"
            )


def check_sensitive_data_to_logs(
        code_dag: dict, result: SecurityCheckResult) -> None:
    """
    HIGH: Sensitive data reaching a logging function risks appearing in
    log aggregation systems and violating data handling requirements.
    """
    log_indicators = {"log", "logger", "logging", "print", "stdout", "stderr"}
    functions = code_dag.get("functions", {})

    for edge in code_dag.get("edges", []):
        to_fn = edge.get("to", "")
        security = edge.get("security", {})

        is_log_target = any(ind in to_fn.lower() for ind in log_indicators)
        carries_sensitive = (
            security.get("carries_pii", False)
            or security.get("carries_credentials", False)
        )

        if is_log_target and carries_sensitive:
            sensitivity_type = (
                "credentials" if security.get("carries_credentials") else "PII"
            )
            result.add(
                "HIGH",
                "SENSITIVE_DATA_TO_LOGS",
                edge.get("id", "unknown_edge"),
                f"Edge {edge.get('id')} carries {sensitivity_type} from "
                f"'{edge.get('from')}' to logging function '{to_fn}' — "
                f"verify sensitive fields are excluded before logging"
            )


def check_unauthenticated_entry_with_unvalidated_input(
        code_dag: dict, result: SecurityCheckResult) -> None:
    """
    HIGH: A public entry point with unvalidated input that reaches an
    external call is a potential injection vector.
    """
    functions = code_dag.get("functions", {})
    entry_points = set(code_dag.get("entry_points", []))
    call_map = build_call_map(code_dag)

    for ep in entry_points:
        fn = functions.get(ep, {})
        security = fn.get("security", {})

        if security.get("trust_level") != "public":
            continue
        if security.get("input_validated", True):
            continue

        # Check if any reachable function makes an external call
        reachable = reachable_from(ep, call_map)
        for fn_name in reachable:
            target = functions.get(fn_name, {})
            target_sec = target.get("security", {})
            if target_sec.get("external_calls"):
                result.add(
                    "HIGH",
                    "UNVALIDATED_INPUT_TO_EXTERNAL_CALL",
                    ep,
                    f"Public entry point '{ep}' has unvalidated input and "
                    f"reaches '{fn_name}' which makes external calls to "
                    f"{target_sec['external_calls']} — potential injection vector"
                )
                break


def check_hand_written_boundary_with_pii(
        code_dag: dict, result: SecurityCheckResult) -> None:
    """
    MEDIUM: Hand-written language boundaries carrying PII have no
    machine-enforced contract — drift is possible and undetected.
    """
    functions = code_dag.get("functions", {})

    for boundary in code_dag.get("language_boundaries", []):
        mechanism = boundary.get("bridge_mechanism")
        if mechanism not in ("hand_written", "implicit"):
            continue

        from_fn = functions.get(boundary.get("from_function", ""), {})
        to_fn = functions.get(boundary.get("to_function", ""), {})

        from_sensitive = from_fn.get("security", {}).get(
            "data_sensitivity", "public") in SENSITIVE_LEVELS
        to_sensitive = to_fn.get("security", {}).get(
            "data_sensitivity", "public") in SENSITIVE_LEVELS

        if from_sensitive or to_sensitive:
            level = "HIGH" if mechanism == "implicit" else "MEDIUM"
            result.add(
                level,
                f"{mechanism.upper()}_BOUNDARY_WITH_SENSITIVE_DATA",
                boundary.get("id", "unknown_boundary"),
                f"Language boundary '{boundary.get('id')}' "
                f"({boundary.get('from_language')} → {boundary.get('to_language')}) "
                f"carries sensitive data with a {mechanism} contract — "
                f"drift between the two sides cannot be automatically detected"
            )


# ─────────────────────────────────────────────
# Individual checks — infrastructure DAG
# ─────────────────────────────────────────────

def check_publicly_accessible_sensitive_stores(
        infra_dag: dict, result: SecurityCheckResult) -> None:
    """
    CRITICAL: A publicly accessible resource storing sensitive data
    is an immediate data exposure risk.
    """
    for resource_id, resource in infra_dag.get("resources", {}).items():
        security = resource.get("security", {})
        if (security.get("publicly_accessible", False)
                and security.get("data_sensitivity", "public") in SENSITIVE_LEVELS):
            result.add(
                "CRITICAL",
                "PUBLIC_SENSITIVE_STORE",
                resource_id,
                f"Resource '{resource_id}' ({resource.get('gcp_service')}) is "
                f"publicly accessible and stores "
                f"{security['data_sensitivity']} data"
            )


def check_excess_permissions(
        infra_dag: dict, result: SecurityCheckResult) -> None:
    """
    MEDIUM: IAM bindings granting more than necessary violate least privilege
    and expand the blast radius of a credential compromise.
    """
    for resource_id, resource in infra_dag.get("resources", {}).items():
        security = resource.get("security", {})
        if security.get("excess_permissions", False):
            bindings = security.get("iam_bindings", [])
            result.add(
                "MEDIUM",
                "EXCESS_IAM_PERMISSIONS",
                resource_id,
                f"Resource '{resource_id}' has IAM bindings that exceed what "
                f"the code actually uses. Current bindings: {bindings}. "
                f"Review bridge_to_code records to identify minimum required roles."
            )


def check_implicit_boundaries_with_sensitive_data(
        infra_dag: dict, result: SecurityCheckResult) -> None:
    """
    HIGH: Infrastructure service boundaries with implicit contracts carrying
    sensitive data — the contract is unknown, drift cannot be detected.
    """
    for boundary in infra_dag.get("service_boundaries", []):
        if boundary.get("bridge_mechanism") != "implicit":
            continue

        resources = infra_dag.get("resources", {})
        from_res = resources.get(boundary.get("from_resource", ""), {})
        to_res = resources.get(boundary.get("to_resource", ""), {})

        from_sensitive = from_res.get("security", {}).get(
            "data_sensitivity", "public") in SENSITIVE_LEVELS
        to_sensitive = to_res.get("security", {}).get(
            "data_sensitivity", "public") in SENSITIVE_LEVELS

        if from_sensitive or to_sensitive:
            result.add(
                "HIGH",
                "IMPLICIT_BOUNDARY_SENSITIVE_DATA",
                boundary.get("id", "unknown_boundary"),
                f"Service boundary '{boundary.get('id')}' between "
                f"{boundary.get('from_service')} and {boundary.get('to_service')} "
                f"carries sensitive data with no declared contract"
            )


def check_code_no_infra_external_calls(
        infra_dag: dict, result: SecurityCheckResult) -> None:
    """
    MEDIUM: Code making external calls with no infrastructure declaration
    may be calling unapproved services outside the declared stack.
    """
    for record in infra_dag.get("bridge_to_code", []):
        if record.get("gap_type") != "code_no_infra":
            continue
        description = record.get("gap_description", "").lower()
        if "external" in description or "api" in description or "http" in description:
            result.add(
                "MEDIUM",
                "UNDECLARED_EXTERNAL_CALL",
                record.get("infra_edge_id", "unknown"),
                f"Code makes external calls with no Terraform declaration: "
                f"{record.get('gap_description', 'unknown')} — "
                f"verify this service is approved and documented"
            )


# ─────────────────────────────────────────────
# Runner and report
# ─────────────────────────────────────────────

# dag:boundary
def print_report(result: SecurityCheckResult, warn_only: bool) -> int:
    """Print the findings report. Returns exit code."""
    critical = result.by_level("CRITICAL")
    high     = result.by_level("HIGH")
    medium   = result.by_level("MEDIUM")
    low      = result.by_level("LOW")

    print("\n" + "═" * 60)
    print("  SECURITY PRE-PROCESS CHECK REPORT")
    print("═" * 60)
    print(f"  Critical : {len(critical)}")
    print(f"  High     : {len(high)}")
    print(f"  Medium   : {len(medium)}")
    print(f"  Low      : {len(low)}")
    print("═" * 60)

    for severity, items in [
        ("CRITICAL", critical),
        ("HIGH", high),
        ("MEDIUM", medium),
        ("LOW", low)
    ]:
        if not items:
            continue
        print(f"\n── {severity} ─────────────────────────────────────────────\n")
        for finding in items:
            print(f"  [{finding.rule}]")
            print(f"  Location : {finding.location}")
            print(f"  {finding.message}\n")

    print("═" * 60)

    total = len(result.findings)
    if total == 0:
        print("\n  ✓  No security findings — safe to run security review agent\n")
        return 0
    elif critical and not warn_only:
        print(f"\n  ✗  {len(critical)} CRITICAL finding(s) — fix before any release\n")
        return 1
    else:
        qualifier = "(--warn-only, not blocking)" if warn_only else ""
        print(f"\n  ⚠  {total} finding(s) found {qualifier}\n")
        print("  Run the security review agent for full analysis:\n")
        print("  python scripts/run_dag_agent.py --type security\n")
        return 0 if warn_only else (1 if critical else 0)


def main() -> int:
    """Entry point."""
    parser = argparse.ArgumentParser(
        description="Security pre-process checker — "
                    "runs deterministic checks before the security review agent"
    )
    parser.add_argument(
        "--warn-only",
        action="store_true",
        help="Report findings but do not block on any severity (exit 0)"
    )
    parser.add_argument(
        "--dag-dir",
        default="dag",
        help="Directory containing static_dag.json and infrastructure_dag.json"
    )
    args = parser.parse_args()

    dag_dir   = Path(args.dag_dir)
    code_dag  = load_dag(dag_dir / "static_dag.json", "Static code DAG")
    infra_dag = load_dag(dag_dir / "infrastructure_dag.json", "Infrastructure DAG")

    result = SecurityCheckResult()

    # Code DAG checks
    check_public_entry_to_sensitive_data(code_dag, result)
    check_low_confidence_sensitive(code_dag, result)
    check_credentials_in_return_values(code_dag, result)
    check_sensitive_data_to_logs(code_dag, result)
    check_unauthenticated_entry_with_unvalidated_input(code_dag, result)
    check_hand_written_boundary_with_pii(code_dag, result)

    # Infrastructure DAG checks
    check_publicly_accessible_sensitive_stores(infra_dag, result)
    check_excess_permissions(infra_dag, result)
    check_implicit_boundaries_with_sensitive_data(infra_dag, result)
    check_code_no_infra_external_calls(infra_dag, result)

    return print_report(result, args.warn_only)


if __name__ == "__main__":
    sys.exit(main())
