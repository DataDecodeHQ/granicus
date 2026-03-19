#!/usr/bin/env python3
"""
DAG Pre-Process: Codebase Check Orchestrator
----------------------------------------------
Auto-detects languages in a codebase and runs the appropriate
standalone checkers. Produces a unified report.

Usage:
    python check_codebase.py <path>
    python check_codebase.py . --warn-only
    python check_codebase.py . --json

Language detection:
    .go files present  ->  runs check_go.py checks
    .py files present  ->  runs check_python.py checks
    Both can run in the same codebase.
"""

import sys
import json
import argparse
from pathlib import Path

# dag:boundary
def run_pipeline(root_path: str, warn_only: bool = False,
                 output_json: bool = False) -> int:
    """
    Detect languages, run checks, print unified report.
    Returns exit code: 0 = pass, 1 = failures found.
    """
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from code_checks.check_go import (
        collect_go_files,
        check_go_file,
        run_go_vet,
        CheckResult as GoCheckResult,
    )
    from code_checks.check_python import (
        collect_python_files,
        check_python_file,
        catalogue_boundary_file,
        check_env_files,
        CheckResult as PyCheckResult,
    )

    # Detect what's present
    go_source, go_tests = collect_go_files(root_path)
    py_files, boundary_files = collect_python_files(root_path)

    languages: list[str] = []
    if go_source:
        languages.append("go")
    if py_files:
        languages.append("python")

    if not languages:
        print("No Go or Python files found.")
        return 0

    # Run checks
    go_result = GoCheckResult()
    py_result = PyCheckResult()

    if go_source:
        run_go_vet(root_path, go_result)
        for filepath in sorted(go_source):
            file_result = check_go_file(str(filepath))
            go_result.issues.extend(file_result.issues)

    if py_files:
        check_env_files(root_path, py_result)
        for filepath in sorted(py_files):
            file_result = check_python_file(str(filepath))
            py_result.issues.extend(file_result.issues)

    for filepath in sorted(boundary_files):
        catalogue_boundary_file(str(filepath), py_result)

    total_failures = len(go_result.failures) + len(py_result.failures)
    total_warnings = len(go_result.warnings) + len(py_result.warnings)

    # ── Output ──
    if output_json:
        data = {
            "languages": languages,
            "total_failures": total_failures,
            "total_warnings": total_warnings,
        }
        if go_source:
            go_data = go_result.to_dict()
            go_data["files_checked"] = len(go_source)
            go_data["test_files_skipped"] = len(go_tests)
            data["go"] = go_data
        if py_files:
            py_data = py_result.to_dict()
            py_data["files_checked"] = len(py_files)
            py_data["boundary_files"] = len(boundary_files)
            data["python"] = py_data
        print(json.dumps(data, indent=2))
    else:
        print("\n" + "=" * 60)
        print("  DAG PRE-PROCESS CHECK REPORT")
        print("=" * 60)
        print(f"  Languages       : {', '.join(languages)}")
        if go_source:
            print(f"  Go source files : {len(go_source)}")
            print(f"  Go test files   : {len(go_tests)} (skipped)")
        if py_files:
            print(f"  Python files    : {len(py_files)}")
        if boundary_files:
            print(f"  Boundary markers: {len(boundary_files)}")
        print(f"  Failures        : {total_failures}")
        print(f"  Warnings        : {total_warnings}")
        print("=" * 60)

        # Go failures
        if go_result.failures:
            print(f"\n-- GO FAILURES ({len(go_result.failures)}) "
                  "----------------------------------\n")
            current_file = None
            for issue in go_result.failures:
                if issue.file != current_file:
                    print(f"  {issue.file}")
                    current_file = issue.file
                print(f"    line {issue.line:<4} [{issue.rule}]")
                print(f"             {issue.message}\n")

        # Python failures
        if py_result.failures:
            print(f"\n-- PYTHON FAILURES ({len(py_result.failures)}) "
                  "------------------------------\n")
            current_file = None
            for issue in py_result.failures:
                if issue.file != current_file:
                    print(f"  {issue.file}")
                    current_file = issue.file
                print(f"    line {issue.line:<4} [{issue.rule}]")
                print(f"             {issue.message}\n")

        # Go warnings
        if go_result.warnings:
            print(f"\n-- GO WARNINGS ({len(go_result.warnings)}) "
                  "----------------------------------\n")
            current_file = None
            for issue in go_result.warnings:
                if issue.file != current_file:
                    print(f"  {issue.file}")
                    current_file = issue.file
                print(f"    line {issue.line:<4} [{issue.rule}]")
                print(f"             {issue.message}\n")

        # Python warnings
        if py_result.warnings:
            print(f"\n-- PYTHON WARNINGS ({len(py_result.warnings)}) "
                  "------------------------------\n")
            current_file = None
            for issue in py_result.warnings:
                if issue.file != current_file:
                    print(f"  {issue.file}")
                    current_file = issue.file
                print(f"    line {issue.line:<4} [{issue.rule}]")
                print(f"             {issue.message}\n")

        print("=" * 60)

        if total_failures and not warn_only:
            print("\n  BLOCKED -- fix failures before DAG analysis\n")
        elif total_failures:
            print("\n  Failures found (--warn-only, not blocking)\n")
        else:
            print("\n  All checks passed -- safe to run static "
                  "DAG analysis\n")

    if total_failures and not warn_only:
        return 1
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="DAG Pre-Process: unified codebase checks "
                    "(auto-detects Go, Python)"
    )
    parser.add_argument("path", help="Root path of codebase to check")
    parser.add_argument(
        "--warn-only", action="store_true",
        help="Report failures as warnings (exit 0)")
    parser.add_argument(
        "--json", action="store_true", dest="output_json",
        help="Output results as JSON")
    args = parser.parse_args()

    sys.exit(run_pipeline(args.path, args.warn_only, args.output_json))
