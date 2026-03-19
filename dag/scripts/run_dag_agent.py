#!/usr/bin/env python3
"""
run_dag_agent.py
-----------------
Orchestrates a DAG analysis agent run using the Anthropic API.
Reads the codebase or terraform state, calls the agent, writes output,
and manages version history.

This script is designed to be run from Claude Code or the terminal.

Usage:
    # Run the static code DAG agent
    python scripts/run_dag_agent.py --type code

    # Run the infrastructure DAG agent
    python scripts/run_dag_agent.py --type infra

    # Run the security review agent (requires annotated DAGs)
    python scripts/run_dag_agent.py --type security

    # Run with a specific commit message for the history entry
    python scripts/run_dag_agent.py --type code --message "add chart caching layer"

    # Dry run — produce output but don't write to dag/ or commit
    python scripts/run_dag_agent.py --type code --dry-run

Requirements:
    pip install anthropic
    ANTHROPIC_API_KEY must be set in environment or .env file

Output:
    dag/static_dag.json         (or infrastructure_dag.json)
    dag/history/<type>_dag_<commit>.json
    dag/history/<type>_dag_<commit>.diff.json   (change summary)
"""

import os
import sys
import json
import argparse
import subprocess
import shutil
from pathlib import Path
from datetime import datetime, timezone


# ─────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────

REPO_ROOT    = Path(__file__).resolve().parents[2]   # dag/scripts/ -> dag/ -> granicus/
DAG_DIR      = REPO_ROOT / "dag"
HISTORY_DIR  = DAG_DIR / "history"
SCRIPTS_DIR  = DAG_DIR / "scripts"
PROMPTS_DIR  = DAG_DIR / "prompts"

CODE_PROMPT_FILE     = PROMPTS_DIR / "static_analysis_agent.md"
INFRA_PROMPT_FILE    = PROMPTS_DIR / "infrastructure_dag_agent.md"
SECURITY_PROMPT_FILE = PROMPTS_DIR / "security_review_agent.md"

CODE_DAG_FILE     = DAG_DIR / "static_dag.json"
INFRA_DAG_FILE    = DAG_DIR / "infrastructure_dag.json"
SECURITY_DAG_FILE = DAG_DIR / "security_review.json"

MODEL = "claude-opus-4-5"
MAX_TOKENS = 8192


# ─────────────────────────────────────────────
# Git utilities
# ─────────────────────────────────────────────

def get_current_commit() -> str:
    """Return the current git commit hash (short)."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True
        )
        return result.stdout.strip() or "unknown"
    except Exception:
        return "unknown"


def get_changed_files(since_commit: str) -> list[str]:
    """Return list of files changed since a given commit."""
    try:
        result = subprocess.run(
            ["git", "diff", "--name-only", since_commit, "HEAD"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True
        )
        return [f for f in result.stdout.strip().splitlines() if f]
    except Exception:
        return []


def git_commit_dag(dag_type: str, commit_hash: str, message: str) -> None:
    """Stage and commit the updated DAG files."""
    try:
        subprocess.run(
            ["git", "add", "dag/"],
            cwd=REPO_ROOT,
            check=True
        )
        commit_msg = f"dag: update {dag_type} DAG [{commit_hash}] — {message}"
        subprocess.run(
            ["git", "commit", "-m", commit_msg, "--no-verify"],
            cwd=REPO_ROOT,
            check=True
        )
        print(f"\n  ✓ Committed DAG update: {commit_msg}")
    except subprocess.CalledProcessError as e:
        print(f"\n  ⚠ Git commit failed (DAG files written but not committed): {e}")


# ─────────────────────────────────────────────
# Codebase context builders
# ─────────────────────────────────────────────

def build_code_context(changed_files: list[str] | None = None) -> str:
    """
    Build the context string for the static code DAG agent.
    Includes all .go and .py files up to a reasonable token budget.
    If changed_files is provided, includes those plus the previous DAG for diff mode.
    """
    SKIP_DIRS = {"vendor", "__pycache__", ".git", "dist",
                 "build", "coverage", "venv", "testdata"}
    INCLUDE_EXTS = {".go", ".py"}
    MAX_FILES = 80  # token budget guard

    sections: list[str] = []

    # If previous DAG exists and we have changed files, include for diff context
    if changed_files and CODE_DAG_FILE.exists():
        sections.append("## Previous Static DAG (for diff mode)\n")
        sections.append("```json")
        sections.append(CODE_DAG_FILE.read_text(encoding="utf-8"))
        sections.append("```\n")
        sections.append(f"## Changed files since last DAG: {', '.join(changed_files)}\n")
        sections.append("## Source files to reassess\n")
        # In diff mode only include changed files + their neighbors
        files_to_include = [
            REPO_ROOT / f for f in changed_files
            if Path(f).suffix in INCLUDE_EXTS
        ]
    else:
        sections.append("## Full codebase for initial DAG analysis\n")
        files_to_include = [
            f for f in sorted(REPO_ROOT.rglob("*"))
            if f.is_file()
            and f.suffix in INCLUDE_EXTS
            and not any(part in SKIP_DIRS for part in f.parts)
        ][:MAX_FILES]

    for filepath in files_to_include:
        if not filepath.exists():
            continue
        rel_path = filepath.relative_to(REPO_ROOT)
        try:
            content = filepath.read_text(encoding="utf-8")
        except Exception:
            continue
        sections.append(f"### {rel_path}\n```{filepath.suffix.lstrip('.')}")
        sections.append(content)
        sections.append("```\n")

    sections.append(f"\n## Metadata\n")
    sections.append(f"- Commit: {get_current_commit()}")
    sections.append(f"- Generated: {datetime.now(timezone.utc).isoformat()}")
    sections.append(f"- Files included: {len(files_to_include)}")

    return "\n".join(sections)


def build_infra_context() -> str:
    """
    Build the context string for the infrastructure DAG agent.
    Uses terraform show -json as the primary source.
    """
    sections: list[str] = []

    # Try to get terraform state
    infra_dir = REPO_ROOT / "infra"
    if not infra_dir.exists():
        infra_dir = REPO_ROOT  # fallback to repo root

    try:
        result = subprocess.run(
            ["terraform", "show", "-json"],
            cwd=infra_dir,
            capture_output=True,
            text=True,
            timeout=30
        )
        if result.returncode == 0 and result.stdout.strip():
            sections.append("## Terraform State\n```json")
            sections.append(result.stdout.strip())
            sections.append("```\n")
        else:
            sections.append("## Terraform State\n")
            sections.append("⚠ Could not retrieve terraform state. "
                            "Run `terraform init` and `terraform apply` first.\n")
    except FileNotFoundError:
        sections.append("## Terraform State\n")
        sections.append("⚠ terraform binary not found. "
                        "Install terraform to enable infrastructure analysis.\n")
    except subprocess.TimeoutExpired:
        sections.append("⚠ terraform show timed out.\n")

    # Include .tf source files for descriptions and intent
    tf_files = sorted(infra_dir.rglob("*.tf"))
    tf_files = [f for f in tf_files if ".terraform" not in str(f)]

    if tf_files:
        sections.append("## Terraform Source Files\n")
        for tf_file in tf_files:
            rel_path = tf_file.relative_to(REPO_ROOT)
            sections.append(f"### {rel_path}\n```hcl")
            try:
                sections.append(tf_file.read_text(encoding="utf-8"))
            except Exception:
                sections.append("(could not read file)")
            sections.append("```\n")

    # Include previous infrastructure DAG if it exists
    if INFRA_DAG_FILE.exists():
        sections.append("## Previous Infrastructure DAG (for diff mode)\n```json")
        sections.append(INFRA_DAG_FILE.read_text(encoding="utf-8"))
        sections.append("```\n")

    sections.append(f"\n## Metadata\n")
    sections.append(f"- Commit: {get_current_commit()}")
    sections.append(f"- Generated: {datetime.now(timezone.utc).isoformat()}")

    return "\n".join(sections)


# ─────────────────────────────────────────────
# Agent runner
# ─────────────────────────────────────────────

def build_security_context() -> str:
    """
    Build the context string for the security review agent.
    Reads both annotated DAGs and passes them together.
    Both must exist and have security annotations populated.
    """
    sections: list[str] = []

    if not CODE_DAG_FILE.exists():
        print("  ✗ static_dag.json not found — run: python scripts/run_dag_agent.py --type code")
        sys.exit(1)
    if not INFRA_DAG_FILE.exists():
        print("  ✗ infrastructure_dag.json not found — run: python scripts/run_dag_agent.py --type infra")
        sys.exit(1)

    code_dag = json.loads(CODE_DAG_FILE.read_text(encoding="utf-8"))
    infra_dag = json.loads(INFRA_DAG_FILE.read_text(encoding="utf-8"))

    # Verify security annotations are present
    functions = code_dag.get("functions", {})
    annotated = sum(1 for f in functions.values() if "security" in f)
    if annotated == 0:
        print("  ⚠  Static DAG has no security annotations.")
        print("     The security review will have limited coverage.")
        print("     Re-run: python scripts/run_dag_agent.py --type code")

    sections.append("## Static Code DAG (with security annotations)\n```json")
    sections.append(json.dumps(code_dag, indent=2))
    sections.append("```\n")

    sections.append("## Infrastructure DAG (with security annotations)\n```json")
    sections.append(json.dumps(infra_dag, indent=2))
    sections.append("```\n")

    sections.append(f"\n## Metadata\n")
    sections.append(f"- Commit: {get_current_commit()}")
    sections.append(f"- Generated: {datetime.now(timezone.utc).isoformat()}")
    sections.append(f"- Functions in code DAG: {len(functions)}")
    sections.append(f"- Functions with security annotations: {annotated}")

    return "\n".join(sections)


def run_agent(system_prompt: str, user_context: str) -> str:
    """
    Call the Anthropic API with the agent prompt and context.
    Returns the raw response text.
    """
    try:
        import anthropic
    except ImportError:
        print("\n  ✗ anthropic package not found. Run: pip install anthropic")
        sys.exit(1)

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        # Try loading from .env in repo root
        env_file = REPO_ROOT / ".env"
        if env_file.exists():
            for line in env_file.read_text().splitlines():
                if line.startswith("ANTHROPIC_API_KEY="):
                    api_key = line.split("=", 1)[1].strip().strip('"')
                    break

    if not api_key:
        print("\n  ✗ ANTHROPIC_API_KEY not set. Set it in your environment or .env file.")
        sys.exit(1)

    client = anthropic.Anthropic(api_key=api_key)

    print(f"  → Calling {MODEL}...")
    print(f"  → Context size: ~{len(user_context) // 4} tokens (estimated)")

    message = client.messages.create(
        model=MODEL,
        max_tokens=MAX_TOKENS,
        system=system_prompt,
        messages=[
            {
                "role": "user",
                "content": user_context
            }
        ]
    )

    return message.content[0].text


def extract_json_from_response(response: str) -> dict:
    """
    Extract the JSON DAG from the agent response.
    The agent may wrap it in markdown code fences.
    """
    import re

    # Try to find JSON in a code block first
    json_block = re.search(r'```json\s*([\s\S]+?)\s*```', response)
    if json_block:
        try:
            return json.loads(json_block.group(1))
        except json.JSONDecodeError:
            pass

    # Try the whole response as JSON
    try:
        return json.loads(response)
    except json.JSONDecodeError:
        pass

    # Try to find the outermost { } block
    start = response.find('{')
    end = response.rfind('}')
    if start != -1 and end != -1:
        try:
            return json.loads(response[start:end + 1])
        except json.JSONDecodeError:
            pass

    raise ValueError(
        "Could not extract valid JSON from agent response. "
        "Raw response saved to dag/history/last_raw_response.txt"
    )


# ─────────────────────────────────────────────
# Version management
# ─────────────────────────────────────────────

def save_dag(
        dag_data: dict,
        dag_type: str,
        commit_hash: str,
        dry_run: bool = False) -> None:
    """
    Save the new DAG as the current version and archive the previous.
    dag_type: 'code' or 'infra'
    """
    file_map = {"code": CODE_DAG_FILE, "infra": INFRA_DAG_FILE, "security": SECURITY_DAG_FILE}
    name_map = {"code": "static_dag", "infra": "infrastructure_dag", "security": "security_review"}
    current_file = file_map[dag_type]
    dag_name = name_map[dag_type]

    # Archive previous version if it exists
    if current_file.exists() and not dry_run:
        archive_path = HISTORY_DIR / f"{dag_name}_{commit_hash}_prev.json"
        shutil.copy2(current_file, archive_path)
        print(f"  → Archived previous DAG to {archive_path.relative_to(REPO_ROOT)}")

    # Save new version
    dag_json = json.dumps(dag_data, indent=2)

    if dry_run:
        dry_path = DAG_DIR / f"{dag_name}_dry_run.json"
        dry_path.write_text(dag_json, encoding="utf-8")
        print(f"  → Dry run: wrote to {dry_path.relative_to(REPO_ROOT)}")
    else:
        DAG_DIR.mkdir(parents=True, exist_ok=True)
        HISTORY_DIR.mkdir(parents=True, exist_ok=True)
        current_file.write_text(dag_json, encoding="utf-8")

        # Also save as versioned copy
        versioned_path = HISTORY_DIR / f"{dag_name}_{commit_hash}.json"
        versioned_path.write_text(dag_json, encoding="utf-8")

        print(f"  → Saved DAG to {current_file.relative_to(REPO_ROOT)}")
        print(f"  → Saved versioned copy to {versioned_path.relative_to(REPO_ROOT)}")

    # Save change summary diff if present
    if "change_summary" in dag_data and not dry_run:
        diff_path = HISTORY_DIR / f"{dag_name}_{commit_hash}.diff.json"
        diff_path.write_text(
            json.dumps(dag_data["change_summary"], indent=2),
            encoding="utf-8"
        )
        print(f"  → Saved change summary to {diff_path.relative_to(REPO_ROOT)}")


def print_summary(dag_data: dict, dag_type: str) -> None:
    """Print a human-readable summary of the DAG produced."""
    print("\n" + "═" * 60)
    print(f"  DAG ANALYSIS SUMMARY — {dag_type.upper()}")
    print("═" * 60)

    if dag_type == "code":
        funcs = dag_data.get("functions", {})
        edges = dag_data.get("edges", [])
        flags = dag_data.get("flags", [])
        boundaries = dag_data.get("language_boundaries", [])

        print(f"  Functions mapped    : {len(funcs)}")
        print(f"  Edges mapped        : {len(edges)}")
        print(f"  Language boundaries : {len(boundaries)}")
        print(f"  Flags               : {len(flags)}")

        hard_flags = [f for f in flags if f.get("type") == "cannot_infer"]
        low_conf   = [f for f in flags if f.get("type") == "low_confidence"]
        print(f"    cannot_infer      : {len(hard_flags)}")
        print(f"    low_confidence    : {len(low_conf)}")

        if hard_flags:
            print("\n  ── Hard flags (require immediate attention) ─────────")
            for flag in hard_flags[:5]:  # show first 5
                print(f"    {flag.get('target', '?')} — {flag.get('reason', '')[:70]}")
            if len(hard_flags) > 5:
                print(f"    ... and {len(hard_flags) - 5} more")

    elif dag_type == "infra":
        resources = dag_data.get("resources", {})
        edges = dag_data.get("edges", [])
        boundaries = dag_data.get("service_boundaries", [])
        bridge = dag_data.get("bridge_to_code", [])
        flags = dag_data.get("flags", [])

        gaps = [b for b in bridge if b.get("gap_type")]

        print(f"  Resources mapped    : {len(resources)}")
        print(f"  Edges mapped        : {len(edges)}")
        print(f"  Service boundaries  : {len(boundaries)}")
        print(f"  Code bridge gaps    : {len(gaps)}")
        print(f"  Flags               : {len(flags)}")

        if gaps:
            print("\n  ── Bridge gaps (code ↔ infra mismatches) ────────────")
            for gap in gaps[:5]:
                print(f"    [{gap.get('gap_type')}] edge {gap.get('infra_edge_id', '?')}")
                print(f"      {gap.get('gap_description', '')[:70]}")
            if len(gaps) > 5:
                print(f"    ... and {len(gaps) - 5} more")

    if "change_summary" in dag_data:
        cs = dag_data["change_summary"]
        print("\n  ── Changes from previous version ────────────────────")
        for key, val in cs.items():
            if val:
                print(f"    {key}: {len(val) if isinstance(val, list) else val}")

    print("═" * 60)


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main() -> int:
    """Entry point."""
    parser = argparse.ArgumentParser(
        description="Run a DAG analysis agent and version the output"
    )
    parser.add_argument(
        "--type",
        choices=["code", "infra", "security"],
        required=True,
        help="Which DAG agent to run: code, infra, or security"
    )
    parser.add_argument(
        "--message",
        default="manual run",
        help="Description of what changed (used in commit message)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run agent and write output but do not update dag/ or commit"
    )
    parser.add_argument(
        "--no-commit",
        action="store_true",
        help="Write DAG files but skip the git commit step"
    )
    parser.add_argument(
        "--diff-mode",
        action="store_true",
        help="Compare against previous DAG (agent reassesses changed files only)"
    )
    args = parser.parse_args()

    commit_hash = get_current_commit()
    print(f"\n  DAG Agent Runner")
    print(f"  Type    : {args.type}")
    print(f"  Commit  : {commit_hash}")
    print(f"  Mode    : {'dry run' if args.dry_run else 'diff' if args.diff_mode else 'full'}")

    # Load the agent prompt
    prompt_file_map = {
        "code": CODE_PROMPT_FILE,
        "infra": INFRA_PROMPT_FILE,
        "security": SECURITY_PROMPT_FILE
    }
    prompt_file = prompt_file_map[args.type]
    if not prompt_file.exists():
        print(f"\n  ✗ Prompt file not found: {prompt_file}")
        print(f"    Copy the agent .md files to {PROMPTS_DIR}/")
        return 1

    system_prompt = prompt_file.read_text(encoding="utf-8")
    print(f"\n  → Loaded prompt from {prompt_file.relative_to(REPO_ROOT)}")

    # Build context
    print(f"  → Building context...")
    if args.type == "code":
        changed_files = None
        if args.diff_mode and CODE_DAG_FILE.exists():
            prev_dag = json.loads(CODE_DAG_FILE.read_text())
            prev_commit = prev_dag.get("version", "HEAD~1")
            changed_files = get_changed_files(prev_commit)
            print(f"  → Diff mode: {len(changed_files)} changed files since {prev_commit}")
        context = build_code_context(changed_files)
    elif args.type == "infra":
        context = build_infra_context()
    else:  # security
        context = build_security_context()

    # Run the agent
    print(f"\n  → Running agent...")
    try:
        raw_response = run_agent(system_prompt, context)
    except Exception as e:
        print(f"\n  ✗ Agent call failed: {e}")
        return 1

    # Save raw response for debugging
    raw_path = HISTORY_DIR / "last_raw_response.txt"
    if not args.dry_run:
        HISTORY_DIR.mkdir(parents=True, exist_ok=True)
        raw_path.write_text(raw_response, encoding="utf-8")

    # Extract JSON
    print(f"  → Extracting DAG from response...")
    try:
        dag_data = extract_json_from_response(raw_response)
    except ValueError as e:
        print(f"\n  ✗ {e}")
        if not args.dry_run:
            raw_path.write_text(raw_response, encoding="utf-8")
            print(f"    Raw response saved to {raw_path.relative_to(REPO_ROOT)}")
        return 1

    # Add version metadata if agent didn't include it
    if "version" not in dag_data:
        dag_data["version"] = commit_hash
    if "generated_at" not in dag_data:
        dag_data["generated_at"] = datetime.now(timezone.utc).isoformat()

    # Print summary
    print_summary(dag_data, args.type)

    # Save
    save_dag(dag_data, args.type, commit_hash, dry_run=args.dry_run)

    # Commit
    if not args.dry_run and not args.no_commit:
        git_commit_dag(args.type, commit_hash, args.message)

    return 0


if __name__ == "__main__":
    sys.exit(main())
