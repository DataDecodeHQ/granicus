#!/usr/bin/env python3
"""
DAG Pre-Process: Go Structural Checker
----------------------------------------
Runs structural checks on Go source files before static DAG analysis.
Enforces code rules that make reliable contract inference possible.

Standalone — works on any Go codebase. No project-specific assumptions.

Usage:
    python check_go.py <path>
    python check_go.py . --warn-only
    python check_go.py ./cmd/myapp --json

Checks (FAIL — blocks DAG analysis):
    INLINE_SIDE_EFFECT_*   Side effects in non-boundary functions
    GO_VET                 Issues reported by go vet

Checks (WARN — flagged for review):
    INIT_FUNCTION          init() runs at import time, invisible to DAG
    PACKAGE_LEVEL_VAR      Mutable package-level state
    HIGH_LINE_COUNT        Function exceeds line threshold
    HIGH_PARAM_COUNT       Function exceeds parameter threshold
    MISSING_GODOC          Exported function without godoc comment
    SWALLOWED_ERROR        Return value discarded with _
"""

import re
import sys
import json
import argparse
import subprocess
from pathlib import Path
from dataclasses import dataclass, field, asdict


# ─────────────────────────────────────────────
# Data structures
# ─────────────────────────────────────────────

@dataclass
class Issue:
    level: str          # FAIL | WARN
    file: str
    line: int
    function: str
    rule: str
    message: str

@dataclass
class CheckResult:
    issues: list[Issue] = field(default_factory=list)

    def add(self, level: str, file: str, line: int, function: str,
            rule: str, message: str) -> None:
        """Append an issue to the result set."""
        self.issues.append(Issue(level, file, line, function, rule, message))

    @property
    def failures(self) -> list[Issue]:
        """Return all issues with FAIL level."""
        return [i for i in self.issues if i.level == "FAIL"]

    @property
    def warnings(self) -> list[Issue]:
        """Return all issues with WARN level."""
        return [i for i in self.issues if i.level == "WARN"]

    def to_dict(self) -> dict:
        """Serialize the result as a summary dictionary."""
        return {
            "language": "go",
            "total": len(self.issues),
            "failures": len(self.failures),
            "warnings": len(self.warnings),
            "issues": [asdict(i) for i in self.issues],
        }


# ─────────────────────────────────────────────
# Go function parser
# ─────────────────────────────────────────────

RE_FUNC_DECL = re.compile(
    r'^func\s+'
    r'(?:\(\s*\w+\s+\*?\w+\s*\)\s+)?'  # optional receiver
    r'(\w+)\s*\('                         # function name
)
RE_PACKAGE_VAR = re.compile(r'^var\s+(\w+)\s+')
RE_PACKAGE_VAR_BLOCK_START = re.compile(r'^var\s*\(')
RE_PACKAGE_VAR_IN_BLOCK = re.compile(r'^\s+(\w+)\s+')
RE_CONST_DECL = re.compile(r'^const\s+')
RE_INIT_FUNC = re.compile(r'^func\s+init\s*\(')
RE_EXPORT = re.compile(r'^[A-Z]')


def parse_go_functions(lines: list[str]) -> list[dict]:
    """
    Extract function declarations with their line ranges from Go source.
    Returns list of dicts with: name, start_line, end_line, receiver,
    exported, params_str.
    """
    functions: list[dict] = []
    i = 0
    while i < len(lines):
        line = lines[i]
        match = RE_FUNC_DECL.match(line)
        if match:
            func_name = match.group(1)
            start_line = i + 1  # 1-indexed

            # Detect receiver
            receiver_match = re.match(
                r'^func\s+\(\s*(\w+)\s+\*?(\w+)\s*\)', line)
            receiver = receiver_match.group(2) if receiver_match else None

            # Extract parameter string (content between first parens after name)
            try:
                paren_start = line.index(
                    '(', line.index(func_name) + len(func_name))
            except ValueError:
                i += 1
                continue

            params_str = ""
            depth = 0
            collecting = False
            for ch in line[paren_start:]:
                if ch == '(':
                    if depth == 0:
                        collecting = True
                        depth += 1
                        continue
                    depth += 1
                elif ch == ')':
                    depth -= 1
                    if depth == 0:
                        break
                if collecting:
                    params_str += ch

            # Find the closing brace via brace counting
            brace_depth = 0
            found_open = False
            end_line = start_line
            for j in range(i, len(lines)):
                for ch in lines[j]:
                    if ch == '{':
                        brace_depth += 1
                        found_open = True
                    elif ch == '}':
                        brace_depth -= 1
                if found_open and brace_depth == 0:
                    end_line = j + 1  # 1-indexed
                    break

            functions.append({
                "name": func_name,
                "start_line": start_line,
                "end_line": end_line,
                "receiver": receiver,
                "exported": bool(RE_EXPORT.match(func_name)),
                "params_str": params_str.strip(),
            })
            i = end_line
        else:
            i += 1

    return functions


# ─────────────────────────────────────────────
# Individual checks
# ─────────────────────────────────────────────

def check_init_functions(lines: list[str], filepath: str,
                         result: CheckResult) -> None:
    """
    WARN: init() functions execute at import time with no explicit caller.
    They are invisible edges in the DAG.
    """
    for i, line in enumerate(lines, 1):
        if RE_INIT_FUNC.match(line.strip()):
            result.add(
                "WARN", filepath, i, "init",
                "INIT_FUNCTION",
                "init() executes at import time -- ensure it only sets "
                "config/registers, not business logic or external calls"
            )


def check_package_level_vars(lines: list[str], filepath: str,
                             result: CheckResult) -> None:
    """
    WARN: Package-level var declarations (non-const) are mutable shared state.
    These create invisible edges the DAG cannot track.
    """
    in_var_block = False
    in_func = False
    brace_depth = 0

    for i, line in enumerate(lines, 1):
        stripped = line.strip()

        # Track whether we're inside a function body
        if RE_FUNC_DECL.match(stripped):
            in_func = True
        if in_func:
            brace_depth += stripped.count('{') - stripped.count('}')
            if brace_depth <= 0:
                in_func = False
                brace_depth = 0
            continue

        if RE_PACKAGE_VAR_BLOCK_START.match(stripped):
            in_var_block = True
            continue
        if in_var_block:
            if stripped == ')':
                in_var_block = False
                continue
            var_match = RE_PACKAGE_VAR_IN_BLOCK.match(line)
            if var_match:
                var_name = var_match.group(1)
                if var_name.startswith("//"):
                    continue
                result.add(
                    "WARN", filepath, i, "<package>",
                    "PACKAGE_LEVEL_VAR",
                    f"Package-level var '{var_name}' is mutable shared "
                    f"state -- consider passing as parameter or using const"
                )
            continue

        if RE_CONST_DECL.match(stripped):
            continue

        var_match = RE_PACKAGE_VAR.match(stripped)
        if var_match:
            var_name = var_match.group(1)
            result.add(
                "WARN", filepath, i, "<package>",
                "PACKAGE_LEVEL_VAR",
                f"Package-level var '{var_name}' is mutable shared "
                f"state -- consider passing as parameter or using const"
            )


def check_inline_side_effects(lines: list[str], functions: list[dict],
                              filepath: str, result: CheckResult) -> None:
    """
    FAIL: Side effects must be in named boundary functions.
    Detects direct calls to external systems inside non-boundary functions.
    """
    side_effect_patterns: dict[str, str] = {
        # database / BigQuery
        r'\.Query\(':       "database",
        r'\.QueryRow\(':    "database",
        r'\.Exec\(':        "database",
        r'bigquery\.':      "database",
        r'\.InsertRows\(':  "database",
        # network / HTTP client
        r'http\.Post\(':    "network",
        r'http\.Get\(':     "network",
        r'\.Do\(req\)':    "network",
        r'httpClient\.':    "network",
        # file I/O (writes)
        r'os\.Create\(':      "file_io",
        r'os\.WriteFile\(':   "file_io",
        r'os\.Remove\(':      "file_io",
        r'os\.Rename\(':      "file_io",
        r'ioutil\.WriteFile\(': "file_io",
        r'\.Write\(':         "file_io",
        r'\.WriteString\(':   "file_io",
        # GCS
        r'storage\.':       "gcs",
        r'\.NewWriter\(':   "gcs",
        # Firestore
        r'firestore\.':     "firestore",
        r'\.Set\(ctx':      "firestore",
        r'\.Delete\(ctx':   "firestore",
        # Pub/Sub
        r'\.Publish\(':     "pubsub",
    }

    # Keywords in function names that indicate it IS a boundary function
    boundary_keywords = (
        "write", "save", "send", "publish", "store", "delete", "create",
        "upload", "query", "exec", "fetch", "get", "put", "post",
        "connect", "close", "insert", "update", "remove", "archive",
        "backup", "restore", "download", "subscribe", "listen", "dial",
        "flush", "commit", "persist", "load", "read", "open", "init",
        "new", "setup", "start", "stop", "run", "handle", "serve",
        "migrate", "prune", "sync", "pull", "push", "trigger",
    )

    for func in functions:
        name_lower = func["name"].lower()
        if any(kw in name_lower for kw in boundary_keywords):
            continue

        # Check for // dag:boundary directive in the comment block above
        comment_idx = func["start_line"] - 2  # 0-indexed, line above
        has_boundary_directive = False
        while comment_idx >= 0:
            prev_line = lines[comment_idx].strip()
            if prev_line.startswith("//"):
                if "dag:boundary" in prev_line:
                    has_boundary_directive = True
                    break
                comment_idx -= 1
                continue
            elif prev_line == "":
                comment_idx -= 1
                continue
            else:
                break
        if has_boundary_directive:
            continue

        func_body = lines[func["start_line"] - 1: func["end_line"]]
        seen_categories: set[str] = set()
        func_text = "\n".join(func_body)
        has_string_builder = bool(re.search(
            r'strings\.(New)?Builder|bytes\.(New)?Buffer|bufio\.NewWriter',
            func_text))

        for line_idx, line in enumerate(func_body):
            stripped = line.strip()
            if stripped.startswith("//"):
                continue
            for pattern, category in side_effect_patterns.items():
                if category in seen_categories:
                    continue
                if re.search(pattern, line):
                    # Skip false positives: in-memory writers and type-only references
                    if category == "file_io" and (
                        has_string_builder
                        or re.search(r'(strings\.|bytes\.|bufio\.|Builder|Buffer)', line)
                    ):
                        continue
                    if category == "database" and re.search(r'(bigquery\.Value|bigquery\.Schema|\.DataTo\()', line):
                        continue
                    if category == "firestore" and re.search(r'(firestore\.CollectionRef|firestore\.DocumentRef)', line):
                        continue
                    seen_categories.add(category)
                    result.add(
                        "FAIL", filepath,
                        func["start_line"] + line_idx,
                        func["name"],
                        f"INLINE_SIDE_EFFECT_{category.upper()}",
                        f"'{func['name']}' makes a direct {category} "
                        f"call -- wrap in a named boundary function"
                    )


def check_function_complexity(functions: list[dict], filepath: str,
                              result: CheckResult) -> None:
    """WARN: Functions over line/parameter thresholds."""
    MAX_LINES = 50
    MAX_PARAMS = 5

    for func in functions:
        line_count = func["end_line"] - func["start_line"]
        if line_count > MAX_LINES:
            result.add(
                "WARN", filepath, func["start_line"], func["name"],
                "HIGH_LINE_COUNT",
                f"'{func['name']}' is {line_count} lines "
                f"(max {MAX_LINES}) -- may have mixed responsibilities"
            )

        params = func["params_str"]
        if params:
            param_count = len([p for p in params.split(",") if p.strip()])
            if param_count > MAX_PARAMS:
                result.add(
                    "WARN", filepath, func["start_line"], func["name"],
                    "HIGH_PARAM_COUNT",
                    f"'{func['name']}' has {param_count} parameters "
                    f"(max {MAX_PARAMS}) -- consider a config/options struct"
                )


def check_missing_godoc(lines: list[str], functions: list[dict],
                        filepath: str, result: CheckResult) -> None:
    """WARN: Exported functions without a godoc comment."""
    for func in functions:
        if not func["exported"]:
            continue

        start_idx = func["start_line"] - 2  # 0-indexed, line above
        has_comment = False
        while start_idx >= 0:
            prev_line = lines[start_idx].strip()
            if prev_line.startswith("//"):
                has_comment = True
                break
            elif prev_line == "":
                start_idx -= 1
                continue
            else:
                break

        if not has_comment:
            result.add(
                "WARN", filepath, func["start_line"], func["name"],
                "MISSING_GODOC",
                f"Exported function '{func['name']}' has no godoc "
                f"comment -- add '// {func['name']} ...' above"
            )


def check_error_handling(lines: list[str], functions: list[dict],
                         filepath: str, result: CheckResult) -> None:
    """WARN: Detect swallowed errors (return value assigned to _)."""
    re_err_swallowed = re.compile(r'_\s*=\s*\w+\.?\w*\(')

    # Common safe patterns where discarding return is intentional
    safe_patterns = ("fmt.Fprint", "io.Copy", "fmt.Fprintf",
                     "fmt.Fprintln", "w.Write")

    for func in functions:
        func_body = lines[func["start_line"] - 1: func["end_line"]]
        for line_idx, line in enumerate(func_body):
            stripped = line.strip()
            if stripped.startswith("//"):
                continue
            if re_err_swallowed.search(stripped):
                if any(p in stripped for p in safe_patterns):
                    continue
                result.add(
                    "WARN", filepath,
                    func["start_line"] + line_idx,
                    func["name"],
                    "SWALLOWED_ERROR",
                    f"'{func['name']}' discards a return value with _ "
                    f"-- verify this is not an error being silently swallowed"
                )


# ─────────────────────────────────────────────
# go vet integration
# ─────────────────────────────────────────────

def run_go_vet(root_path: str, result: CheckResult) -> None:
    """Run `go vet` if the go binary is available."""
    try:
        proc = subprocess.run(
            ["go", "vet", "./..."],
            cwd=root_path,
            capture_output=True,
            text=True,
            timeout=60
        )
        if proc.returncode != 0 and proc.stderr:
            for line in proc.stderr.strip().splitlines():
                match = re.match(r'(.+\.go):(\d+):\d+:\s+(.+)', line)
                if match:
                    result.add(
                        "FAIL", match.group(1),
                        int(match.group(2)), "<go vet>",
                        "GO_VET", match.group(3)
                    )
                elif line.strip():
                    result.add(
                        "WARN", root_path, 0, "<go vet>",
                        "GO_VET", line.strip()
                    )
    except FileNotFoundError:
        result.add(
            "WARN", root_path, 0, "<go vet>",
            "GO_VET_NOT_FOUND",
            "go binary not found -- install Go to enable go vet checks"
        )
    except subprocess.TimeoutExpired:
        result.add(
            "WARN", root_path, 0, "<go vet>",
            "GO_VET_TIMEOUT",
            "go vet timed out after 60s"
        )


# ─────────────────────────────────────────────
# File-level runner
# ─────────────────────────────────────────────

def check_go_file(filepath: str) -> CheckResult:
    """Run all Go structural checks on a single file."""
    result = CheckResult()
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            source = f.read()
    except Exception as e:
        result.add("FAIL", filepath, 0, "<module>", "PARSE_ERROR",
                   f"Could not read file: {e}")
        return result

    lines = source.splitlines()
    functions = parse_go_functions(lines)

    check_init_functions(lines, filepath, result)
    check_package_level_vars(lines, filepath, result)
    check_inline_side_effects(lines, functions, filepath, result)
    check_function_complexity(functions, filepath, result)
    check_missing_godoc(lines, functions, filepath, result)
    check_error_handling(lines, functions, filepath, result)

    return result


# ─────────────────────────────────────────────
# Pipeline
# ─────────────────────────────────────────────

SKIP_DIRS = {"vendor", ".git", "dist", "build", "coverage", "testdata"}


def collect_go_files(root_path: str) -> tuple[list[Path], list[Path]]:
    """Return (source_files, test_files) under root_path."""
    path = Path(root_path)
    all_go = [
        f for f in path.rglob("*.go")
        if f.is_file()
        and not any(part in SKIP_DIRS for part in f.parts)
    ]
    source = [f for f in all_go if not f.name.endswith("_test.go")]
    tests = [f for f in all_go if f.name.endswith("_test.go")]
    return source, tests


def run_pipeline(root_path: str, warn_only: bool = False,
                 output_json: bool = False) -> int:
    """
    Check all Go source files under root_path.
    Returns exit code: 0 = pass, 1 = failures found.
    """
    source_files, test_files = collect_go_files(root_path)
    result = CheckResult()

    # go vet (compiler-level)
    run_go_vet(root_path, result)

    # Per-file structural checks
    for filepath in sorted(source_files):
        file_result = check_go_file(str(filepath))
        result.issues.extend(file_result.issues)

    # ── Output ──
    if output_json:
        data = result.to_dict()
        data["files_checked"] = len(source_files)
        data["test_files_skipped"] = len(test_files)
        print(json.dumps(data, indent=2))
    else:
        print("\n" + "=" * 60)
        print("  GO STRUCTURAL CHECK")
        print("=" * 60)
        print(f"  Go source files : {len(source_files)}")
        print(f"  Go test files   : {len(test_files)} (skipped)")
        print(f"  Failures        : {len(result.failures)}")
        print(f"  Warnings        : {len(result.warnings)}")
        print("=" * 60)

        if result.failures:
            print("\n-- FAILURES (must fix before DAG analysis) "
                  "------------------\n")
            current_file = None
            for issue in result.failures:
                if issue.file != current_file:
                    print(f"  {issue.file}")
                    current_file = issue.file
                print(f"    line {issue.line:<4} [{issue.rule}]")
                print(f"             {issue.message}\n")

        if result.warnings:
            print("\n-- WARNINGS (flagged for review) "
                  "----------------------------\n")
            current_file = None
            for issue in result.warnings:
                if issue.file != current_file:
                    print(f"  {issue.file}")
                    current_file = issue.file
                print(f"    line {issue.line:<4} [{issue.rule}]")
                print(f"             {issue.message}\n")

        print("=" * 60)

        if result.failures and not warn_only:
            print("\n  BLOCKED -- fix failures before DAG analysis\n")
        elif result.failures:
            print("\n  Failures found (--warn-only, not blocking)\n")
        else:
            print("\n  All Go checks passed\n")

    if result.failures and not warn_only:
        return 1
    return 0


# ─────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="DAG Pre-Process: Go structural checks"
    )
    parser.add_argument("path", help="Root path of Go codebase to check")
    parser.add_argument(
        "--warn-only", action="store_true",
        help="Report failures as warnings (exit 0)")
    parser.add_argument(
        "--json", action="store_true", dest="output_json",
        help="Output results as JSON instead of human-readable report")
    args = parser.parse_args()

    sys.exit(run_pipeline(args.path, args.warn_only, args.output_json))
