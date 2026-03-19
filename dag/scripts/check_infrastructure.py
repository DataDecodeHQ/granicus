#!/usr/bin/env python3
"""
Infrastructure Pre-Process CI Pipeline
---------------------------------------
Runs compliance checks on Terraform files before Infrastructure DAG analysis.
Parallel to check_codebase.py — enforces the structural rules that make
reliable infrastructure analysis possible.

Checks HCL files statically (no terraform binary required for most checks).
Optionally runs terraform validate and terraform plan if terraform is available.

Usage:
    python check_infrastructure.py <path_to_terraform_root>
    python check_infrastructure.py ./infra
    python check_infrastructure.py ./infra --warn-only
    python check_infrastructure.py ./infra --skip-plan   # skip terraform plan (no GCP auth needed)
"""

import sys
import os
import re
import json
import argparse
import subprocess
from pathlib import Path
from dataclasses import dataclass, field


# ─────────────────────────────────────────────
# Data structures  (mirrors check_codebase.py)
# ─────────────────────────────────────────────

@dataclass
class Issue:
    level: str          # FAIL | WARN
    file: str
    line: int
    resource: str
    rule: str
    message: str

@dataclass
class CheckResult:
    issues: list[Issue] = field(default_factory=list)

    def add(self, level: str, file: str, line: int,
            resource: str, rule: str, message: str) -> None:
        """Append an issue to the result set."""
        self.issues.append(Issue(level, file, line, resource, rule, message))

    @property
    def failures(self) -> list[Issue]:
        """Return all issues with FAIL level."""
        return [i for i in self.issues if i.level == "FAIL"]

    @property
    def warnings(self) -> list[Issue]:
        """Return all issues with WARN level."""
        return [i for i in self.issues if i.level == "WARN"]


# ─────────────────────────────────────────────
# HCL parsing utilities
# (lightweight — no hcl2 dependency required)
# ─────────────────────────────────────────────

def read_hcl_file(filepath: str) -> tuple[str, list[str]]:
    """Read an HCL file and return (raw_source, lines)."""
    with open(filepath, "r", encoding="utf-8") as f:
        source = f.read()
    return source, source.splitlines()


def find_resource_blocks(source: str) -> list[tuple[str, str, int]]:
    """
    Extract resource blocks from HCL source.
    Returns list of (resource_type, resource_name, line_number).
    """
    pattern = re.compile(
        r'^resource\s+"([^"]+)"\s+"([^"]+)"',
        re.MULTILINE
    )
    results = []
    for match in pattern.finditer(source):
        line_num = source[:match.start()].count("\n") + 1
        results.append((match.group(1), match.group(2), line_num))
    return results


def find_data_blocks(source: str) -> list[tuple[str, str, int]]:
    """Extract data source blocks from HCL."""
    pattern = re.compile(
        r'^data\s+"([^"]+)"\s+"([^"]+)"',
        re.MULTILINE
    )
    results = []
    for match in pattern.finditer(source):
        line_num = source[:match.start()].count("\n") + 1
        results.append((match.group(1), match.group(2), line_num))
    return results


def extract_block_body(source: str, block_start_line: int) -> str:
    """
    Extract the body of a block starting at a given line.
    Returns content between the first { and its matching }.
    """
    lines = source.splitlines()
    # Find the opening brace
    body_lines = []
    depth = 0
    started = False
    for i, line in enumerate(lines):
        if i < block_start_line - 1:
            continue
        for char in line:
            if char == "{":
                depth += 1
                started = True
            elif char == "}":
                depth -= 1
        if started:
            body_lines.append(line)
        if started and depth == 0:
            break
    return "\n".join(body_lines)


# ─────────────────────────────────────────────
# Individual checks
# ─────────────────────────────────────────────

def check_resource_descriptions(
        source: str, filepath: str, result: CheckResult) -> None:
    """
    WARN: Every resource should have a description or purpose label/tag.
    Parallel to missing docstrings in code — gives the agent less to work with.
    Resources without descriptions produce lower confidence DAG nodes.
    """
    resources = find_resource_blocks(source)
    for rtype, rname, line_num in resources:
        block_body = extract_block_body(source, line_num)
        has_description = (
            'description' in block_body or
            '"purpose"' in block_body or
            '"app"' in block_body
        )
        if not has_description:
            result.add(
                "WARN", filepath, line_num, f"{rtype}.{rname}",
                "MISSING_DESCRIPTION",
                f"Resource '{rtype}.{rname}' has no description or purpose tag — "
                f"add a description so the DAG agent can infer its role"
            )


def check_hardcoded_values(
        source: str, filepath: str, result: CheckResult) -> None:
    """
    FAIL: No hardcoded credentials, project IDs, or region names.
    These should be variables or data sources.
    Hardcoded values are the infrastructure equivalent of mutable globals —
    they create invisible dependencies the DAG cannot track.
    """
    lines = source.splitlines()

    # Patterns that indicate hardcoded values
    credential_patterns = [
        (re.compile(
            r'(?i)(password|secret|api_key|private_key|token|credential)'
            r'\s*=\s*"(?!var\.|data\.|local\.)[^${\n]{8,}"'),
         "hardcoded credential"),
        # Catch env vars whose NAME suggests a secret but VALUE is hardcoded
        (re.compile(
            r'name\s*=\s*"[^"]*(?:SECRET|TOKEN|KEY|PASSWORD|CREDENTIAL)[^"]*"'
            r'[\s\S]{0,200}?value\s*=\s*"(?!var\.|data\.|local\.)[^${\n]{8,}"'),
         "hardcoded credential in environment variable"),
        (re.compile(r'"projects/[a-z][a-z0-9\-]+/[^${\n]+"'),
         "hardcoded project path — use var.project_id"),
    ]

    # Skip variable declarations and comments
    skip_prefixes = ("variable ", "#", "//", "description", "default")

    for i, line in enumerate(lines, 1):
        stripped = line.strip()
        if any(stripped.startswith(p) for p in skip_prefixes):
            continue
        for pattern, description in credential_patterns:
            if pattern.search(line):
                result.add(
                    "FAIL", filepath, i, "<resource>",
                    "HARDCODED_VALUE",
                    f"Possible {description} at line {i} — "
                    f"use var.* or data sources instead"
                )
                break


def check_broad_iam_roles(
        source: str, filepath: str, result: CheckResult) -> None:
    """
    FAIL: No primitive IAM roles (owner, editor, viewer) on service accounts.
    Broad roles are the infrastructure equivalent of untyped parameters —
    they don't declare what access is actually needed, making the
    permission graph unanalyzable.
    """
    broad_roles = [
        "roles/owner",
        "roles/editor",
        "roles/viewer",
    ]

    lines = source.splitlines()
    for i, line in enumerate(lines, 1):
        for role in broad_roles:
            if role in line and "description" not in line and "#" not in line.lstrip()[:1]:
                result.add(
                    "FAIL", filepath, i, "<iam_binding>",
                    "BROAD_IAM_ROLE",
                    f"Primitive IAM role '{role}' at line {i} — "
                    f"replace with the minimum specific role required "
                    f"(e.g. roles/bigquery.dataViewer instead of roles/viewer)"
                )


def check_secret_references(
        source: str, filepath: str, result: CheckResult) -> None:
    """
    FAIL: Secrets must be referenced from Secret Manager, never declared inline.
    Any resource that needs a secret should reference
    google_secret_manager_secret_version, not contain the value directly.
    """
    lines = source.splitlines()

    # Detect inline secret-like assignments
    inline_secret_pattern = re.compile(
        r'(?i)(token|password|key|secret|credential)\s*=\s*"(?!var\.|data\.|local\.)[^${\n]{8,}"'
    )

    for i, line in enumerate(lines, 1):
        stripped = line.strip()
        if stripped.startswith("#"):
            continue
        if inline_secret_pattern.search(line):
            result.add(
                "FAIL", filepath, i, "<resource>",
                "INLINE_SECRET",
                f"Possible inline secret value at line {i} — "
                f"reference via data.google_secret_manager_secret_version instead"
            )


def check_variable_declarations(
        filepath: str, tf_root: str, result: CheckResult) -> None:
    """
    WARN: Every input variable should have a type and description.
    Untyped variables are the infrastructure equivalent of untyped function parameters.
    Only checks variables.tf files.
    """
    if not filepath.endswith("variables.tf"):
        return

    source, _ = read_hcl_file(filepath)

    # Find variable blocks missing type or description
    var_pattern = re.compile(r'^variable\s+"([^"]+)"', re.MULTILINE)
    for match in var_pattern.finditer(source):
        var_name = match.group(1)
        line_num = source[:match.start()].count("\n") + 1
        block_body = extract_block_body(source, line_num)

        if "type" not in block_body:
            result.add(
                "WARN", filepath, line_num, f"variable.{var_name}",
                "UNTYPED_VARIABLE",
                f"Variable '{var_name}' has no type declaration — add type constraint"
            )
        if "description" not in block_body:
            result.add(
                "WARN", filepath, line_num, f"variable.{var_name}",
                "UNDESCRIBED_VARIABLE",
                f"Variable '{var_name}' has no description — "
                f"add description so the DAG agent understands its purpose"
            )


def check_outputs_described(
        filepath: str, result: CheckResult) -> None:
    """
    WARN: Every output should have a description.
    Outputs are edges leaving a Terraform module — undescribed outputs
    are invisible edges in the infrastructure DAG.
    Only checks outputs.tf files.
    """
    if not filepath.endswith("outputs.tf"):
        return

    source, _ = read_hcl_file(filepath)

    output_pattern = re.compile(r'^output\s+"([^"]+)"', re.MULTILINE)
    for match in output_pattern.finditer(source):
        output_name = match.group(1)
        line_num = source[:match.start()].count("\n") + 1
        block_body = extract_block_body(source, line_num)

        if "description" not in block_body:
            result.add(
                "WARN", filepath, line_num, f"output.{output_name}",
                "UNDESCRIBED_OUTPUT",
                f"Output '{output_name}' has no description — "
                f"outputs are module edges, describe what crosses them"
            )


def check_terraform_validate(tf_root: str, result: CheckResult) -> None:
    """
    FAIL: terraform validate must pass.
    Runs only if terraform binary is available.
    """
    try:
        proc = subprocess.run(
            ["terraform", "validate", "-json"],
            cwd=tf_root,
            capture_output=True,
            text=True,
            timeout=30
        )
        if proc.returncode != 0:
            try:
                output = json.loads(proc.stdout)
                for diag in output.get("diagnostics", []):
                    result.add(
                        "FAIL", diag.get("range", {}).get("filename", tf_root),
                        diag.get("range", {}).get("start", {}).get("line", 0),
                        "<terraform>",
                        "TERRAFORM_VALIDATE",
                        diag.get("summary", "terraform validate failed")
                    )
            except json.JSONDecodeError:
                result.add(
                    "FAIL", tf_root, 0, "<terraform>",
                    "TERRAFORM_VALIDATE",
                    f"terraform validate failed: {proc.stderr.strip()}"
                )
    except FileNotFoundError:
        result.add(
            "WARN", tf_root, 0, "<terraform>",
            "TERRAFORM_NOT_FOUND",
            "terraform binary not found — install terraform to enable validate check"
        )
    except subprocess.TimeoutExpired:
        result.add(
            "WARN", tf_root, 0, "<terraform>",
            "TERRAFORM_TIMEOUT",
            "terraform validate timed out — run manually to check"
        )


def check_terraform_plan(tf_root: str, result: CheckResult) -> None:
    """
    FAIL: terraform plan must show no unexpected drift.
    Any resource that exists in GCP but is not in Terraform state is a gap
    in the infrastructure DAG — an edge or node the analysis cannot see.
    Runs only if terraform binary is available and GCP auth is configured.
    """
    try:
        proc = subprocess.run(
            ["terraform", "plan", "-detailed-exitcode", "-json"],
            cwd=tf_root,
            capture_output=True,
            text=True,
            timeout=120
        )
        # exit code 0 = no changes, 1 = error, 2 = changes present
        if proc.returncode == 1:
            result.add(
                "FAIL", tf_root, 0, "<terraform>",
                "TERRAFORM_PLAN_ERROR",
                f"terraform plan errored — infrastructure state cannot be verified. "
                f"Run 'terraform plan' manually for details."
            )
        elif proc.returncode == 2:
            result.add(
                "FAIL", tf_root, 0, "<terraform>",
                "INFRASTRUCTURE_DRIFT",
                "terraform plan shows pending changes — declared state does not match "
                "actual GCP state. Drift means the Infrastructure DAG is unreliable. "
                "Run 'terraform apply' or reconcile the diff before DAG analysis."
            )
        # exit code 0 = clean, nothing to add
    except FileNotFoundError:
        result.add(
            "WARN", tf_root, 0, "<terraform>",
            "TERRAFORM_NOT_FOUND",
            "terraform binary not found — install terraform to enable plan check"
        )
    except subprocess.TimeoutExpired:
        result.add(
            "WARN", tf_root, 0, "<terraform>",
            "TERRAFORM_PLAN_TIMEOUT",
            "terraform plan timed out — run manually to check for drift"
        )



def check_secret_env_vars(
        source: str, filepath: str, result: CheckResult) -> None:
    """
    FAIL: Environment variables whose names suggest secrets must use
    secret_key_ref (Secret Manager), not a literal value = "...".

    Catches:
        env { name = "CLERK_SECRET_KEY"  value = "sk_live_abc123" }  <- FAIL
    Allows:
        env { name = "CLERK_SECRET_KEY"  value_from { secret_key_ref {...} } }
    """
    secret_name_pattern = re.compile(
        r'name\s*=\s*"[^"]*(?:SECRET|TOKEN|KEY|PASSWORD|CREDENTIAL|API_KEY)[^"]*"',
        re.IGNORECASE
    )
    literal_value_pattern = re.compile(
        r'^\s*value\s*=\s*"(?!var\.|data\.|local\.|/)[^${\n]{4,}"',
        re.MULTILINE
    )
    safe_ref_pattern = re.compile(r'secret_key_ref|secretKeyRef')
    env_block_pattern = re.compile(r'\benv\s*\{', re.MULTILINE)

    for block_match in env_block_pattern.finditer(source):
        block_start_line = source[:block_match.start()].count("\n") + 1
        block_body = extract_block_body(source, block_start_line)

        if not secret_name_pattern.search(block_body):
            continue
        if safe_ref_pattern.search(block_body):
            continue
        if literal_value_pattern.search(block_body):
            name_match = re.search(r'name\s*=\s*"([^"]+)"', block_body)
            var_name = name_match.group(1) if name_match else "<unknown>"
            result.add(
                "FAIL", filepath, block_start_line, "<env_var>",
                "SECRET_ENV_VAR_HARDCODED",
                f"Environment variable '{var_name}' has a secret-like name but a "
                f"literal value — reference via secret_key_ref from Secret Manager instead"
            )

# ─────────────────────────────────────────────
# Runner
# ─────────────────────────────────────────────

def check_tf_file(filepath: str, tf_root: str) -> CheckResult:
    """Parse a single .tf file and run all static compliance checks."""
    result = CheckResult()
    try:
        source, _ = read_hcl_file(filepath)
    except Exception as e:
        result.add("FAIL", filepath, 0, "<module>",
                   "PARSE_ERROR", f"Could not read file: {e}")
        return result

    check_resource_descriptions(source, filepath, result)
    check_hardcoded_values(source, filepath, result)
    check_broad_iam_roles(source, filepath, result)
    check_secret_references(source, filepath, result)
    check_secret_env_vars(source, filepath, result)
    check_variable_declarations(filepath, tf_root, result)
    check_outputs_described(filepath, result)

    return result


def run_pipeline(
        tf_root: str,
        warn_only: bool = False,
        skip_plan: bool = False) -> int:
    """
    Walk the Terraform root, run all checks, print report.
    Returns exit code: 0 = pass, 1 = failures found.
    """
    path = Path(tf_root)

    SKIP_DIRS = {".terraform", ".git", "node_modules"}

    tf_files = [
        f for f in path.rglob("*.tf")
        if not any(part in SKIP_DIRS for part in f.parts)
    ]

    if not tf_files:
        print(f"\nNo .tf files found in {tf_root}")
        return 0

    all_results = CheckResult()

    # Static file checks
    for filepath in sorted(tf_files):
        file_result = check_tf_file(str(filepath), tf_root)
        all_results.issues.extend(file_result.issues)

    # Terraform binary checks
    check_terraform_validate(tf_root, all_results)
    if not skip_plan:
        check_terraform_plan(tf_root, all_results)

    # ── Print report ──────────────────────────────
    print("\n" + "═" * 60)
    print("  INFRASTRUCTURE PRE-PROCESS CHECK REPORT")
    print("═" * 60)
    print(f"  Terraform files : {len(tf_files)}")
    print(f"  Failures        : {len(all_results.failures)}")
    print(f"  Warnings        : {len(all_results.warnings)}")
    if skip_plan:
        print(f"  Plan check      : skipped (--skip-plan)")
    print("═" * 60)

    if all_results.failures:
        print("\n── FAILURES (must fix before DAG analysis) ──────────────\n")
        current_file = None
        for issue in all_results.failures:
            if issue.file != current_file:
                print(f"  {issue.file}")
                current_file = issue.file
            print(f"    line {issue.line:<4} [{issue.rule}]")
            print(f"             {issue.message}\n")

    if all_results.warnings:
        print("\n── WARNINGS (flagged for review) ────────────────────────\n")
        current_file = None
        for issue in all_results.warnings:
            if issue.file != current_file:
                print(f"  {issue.file}")
                current_file = issue.file
            print(f"    line {issue.line:<4} [{issue.rule}]")
            print(f"             {issue.message}\n")

    print("═" * 60)

    if all_results.failures and not warn_only:
        print("\n  ✗  PIPELINE BLOCKED — fix failures before Infrastructure DAG analysis\n")
        return 1
    elif all_results.failures and warn_only:
        print("\n  ⚠  Failures found (--warn-only, not blocking)\n")
        return 0
    else:
        print("\n  ✓  All checks passed — safe to run Infrastructure DAG analysis\n")
        return 0


# ─────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Infrastructure Pre-Process CI Pipeline — "
                    "compliance checks before Infrastructure DAG analysis"
    )
    parser.add_argument(
        "path",
        help="Root path of the Terraform configuration to check"
    )
    parser.add_argument(
        "--warn-only",
        action="store_true",
        help="Report failures as warnings but do not block (exit 0)"
    )
    parser.add_argument(
        "--skip-plan",
        action="store_true",
        help="Skip terraform plan check (use when GCP auth is not available locally)"
    )
    args = parser.parse_args()

    exit_code = run_pipeline(
        args.path,
        warn_only=args.warn_only,
        skip_plan=args.skip_plan
    )
    sys.exit(exit_code)
