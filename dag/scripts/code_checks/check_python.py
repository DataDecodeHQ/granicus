#!/usr/bin/env python3
"""
DAG Pre-Process: Python Structural Checker
--------------------------------------------
Runs structural checks on Python source files before static DAG analysis.
Enforces code rules that make reliable contract inference possible.

Standalone — works on any Python codebase. No project-specific assumptions.

Usage:
    python check_python.py <path>
    python check_python.py . --warn-only
    python check_python.py ./src --json

Checks (FAIL — blocks DAG analysis):
    UNTYPED_RETURN         Function missing return type annotation
    UNTYPED_PARAM          Parameter missing type annotation
    MODULE_LEVEL_LOGIC     Executable logic at module level
    MUTABLE_GLOBAL         Function uses `global` on mutable module-level name
    INLINE_SIDE_EFFECT_*   Side effects in non-boundary functions
    SYNTAX_ERROR           File cannot be parsed
    ENV_FILE_NOT_IGNORED   .env file not covered by .gitignore

Checks (WARN — flagged for review):
    INLINE_SIDE_EFFECT_LOGGING   print() usage (use logger instead)
    HIGH_LINE_COUNT              Function exceeds line threshold
    HIGH_PARAM_COUNT             Function exceeds parameter threshold
    MISSING_DOCSTRING            Public function without docstring
    ENV_FILE_HAS_VALUES          Gitignored .env with real-looking values
"""

import ast
import re
import sys
import json
import fnmatch
import argparse
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
            "language": "python",
            "total": len(self.issues),
            "failures": len(self.failures),
            "warnings": len(self.warnings),
            "issues": [asdict(i) for i in self.issues],
        }


# ─────────────────────────────────────────────
# Individual checks
# ─────────────────────────────────────────────

def check_type_annotations(tree: ast.AST, filepath: str,
                           result: CheckResult) -> None:
    """
    FAIL: Every function must have typed parameters and a return type.
    Without this, contract inference is guesswork.
    """
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            func_name = node.name

            if node.returns is None:
                result.add(
                    "FAIL", filepath, node.lineno, func_name,
                    "UNTYPED_RETURN",
                    f"Function '{func_name}' has no return type annotation"
                )

            for arg in node.args.args:
                if arg.arg in ("self", "cls"):
                    continue
                if arg.annotation is None:
                    result.add(
                        "FAIL", filepath, node.lineno, func_name,
                        "UNTYPED_PARAM",
                        f"Parameter '{arg.arg}' in '{func_name}' "
                        f"has no type annotation"
                    )


def check_module_level_logic(tree: ast.AST, filepath: str,
                             result: CheckResult) -> None:
    """
    FAIL: No executable logic at module level.
    Allowed: imports, assignments, class/function defs,
    docstrings, if __name__ == '__main__'.
    """
    allowed_node_types = (
        ast.Import, ast.ImportFrom,
        ast.FunctionDef, ast.AsyncFunctionDef,
        ast.ClassDef,
        ast.Assign, ast.AnnAssign,
        ast.Expr,  # docstrings
        ast.If,    # if __name__ == '__main__'
        ast.Pass,
    )

    for node in ast.iter_child_nodes(tree):
        if isinstance(node, allowed_node_types):
            if isinstance(node, ast.If):
                if isinstance(node.test, ast.Compare):
                    if (isinstance(node.test.left, ast.Name) and
                            node.test.left.id == '__name__'):
                        continue
                result.add(
                    "FAIL", filepath, node.lineno, "<module>",
                    "MODULE_LEVEL_LOGIC",
                    f"Conditional logic at module level (line "
                    f"{node.lineno}) -- move inside a function"
                )
            elif isinstance(node, ast.Expr):
                if not isinstance(node.value, ast.Constant):
                    result.add(
                        "FAIL", filepath, node.lineno, "<module>",
                        "MODULE_LEVEL_LOGIC",
                        f"Executable expression at module level "
                        f"(line {node.lineno}) -- move inside a function"
                    )
        else:
            result.add(
                "FAIL", filepath, node.lineno, "<module>",
                "MODULE_LEVEL_LOGIC",
                f"Executable statement '{type(node).__name__}' "
                f"at module level (line {node.lineno})"
            )


def _has_boundary_directive(source: str, def_lineno: int) -> bool:
    """Check if `# dag:boundary` appears on lines above the def statement."""
    lines = source.splitlines()
    idx = def_lineno - 2  # def_lineno is 1-based; check line above
    while idx >= 0:
        stripped = lines[idx].strip()
        if stripped == "" or stripped.startswith("#") or stripped.startswith("@"):
            if "# dag:boundary" in lines[idx]:
                return True
            idx -= 1
            continue
        break
    return False


def check_inline_side_effects(tree: ast.AST, filepath: str,
                              result: CheckResult,
                              source: str = "") -> None:
    """
    FAIL: Side effects must be wrapped in named functions, not inline.
    Detects direct writes to known external systems inside function bodies.
    Functions annotated with `# dag:boundary` above their def are skipped.
    """
    side_effect_calls: dict[str, str] = {
        # cache
        "cache.set": "cache",
        "cache.put": "cache",
        # database
        "cursor.execute": "database",
        "session.commit": "database",
        "db.execute": "database",
        "conn.commit": "database",
        "conn.execute": "database",
        # network
        "requests.post": "network",
        "requests.put": "network",
        "requests.delete": "network",
        "httpx.post": "network",
        "httpx.put": "network",
        # file io (writes only)
        "os.remove": "file_io",
        "os.rename": "file_io",
        "shutil.move": "file_io",
        "shutil.copy": "file_io",
    }

    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue

        if source and _has_boundary_directive(source, node.lineno):
            continue

        func_name = node.name
        seen_categories: set[str] = set()

        for child in ast.walk(node):
            if not isinstance(child, ast.Call):
                continue

            call_str = ""
            try:
                call_str = ast.unparse(child.func)
            except Exception:
                continue

            # Check known side-effect calls
            for pattern, category in side_effect_calls.items():
                if category in seen_categories:
                    continue
                if "." in pattern:
                    obj, method = pattern.split(".", 1)
                    if obj in call_str and call_str.endswith(method):
                        seen_categories.add(category)
                        result.add(
                            "FAIL", filepath, node.lineno, func_name,
                            f"INLINE_SIDE_EFFECT_{category.upper()}",
                            f"'{func_name}' calls '{call_str}' directly "
                            f"-- wrap in a named boundary function"
                        )

            # Detect file writes via open()
            if call_str == "open" and "file_io" not in seen_categories:
                for arg in child.args[1:]:
                    if isinstance(arg, ast.Constant) and "w" in str(arg.value):
                        seen_categories.add("file_io")
                        result.add(
                            "FAIL", filepath, node.lineno, func_name,
                            "INLINE_SIDE_EFFECT_FILE_IO",
                            f"'{func_name}' opens file for writing "
                            f"directly -- wrap in a named boundary function"
                        )
                for kw in child.keywords:
                    if (kw.arg == "mode"
                            and isinstance(kw.value, ast.Constant)
                            and "w" in str(kw.value.value)
                            and "file_io" not in seen_categories):
                        seen_categories.add("file_io")
                        result.add(
                            "FAIL", filepath, node.lineno, func_name,
                            "INLINE_SIDE_EFFECT_FILE_IO",
                            f"'{func_name}' opens file for writing "
                            f"directly -- wrap in a named boundary function"
                        )

            # print() -- warn only
            if call_str == "print" and "logging" not in seen_categories:
                seen_categories.add("logging")
                result.add(
                    "WARN", filepath, node.lineno, func_name,
                    "INLINE_SIDE_EFFECT_LOGGING",
                    f"'{func_name}' uses print() -- use a logger "
                    f"or named output function"
                )


def check_mutable_globals(tree: ast.AST, filepath: str,
                          result: CheckResult) -> None:
    """
    FAIL: Functions must not use `global` on mutable module-level names.
    Shared mutable globals create invisible edges the DAG cannot track.
    """
    module_level_names: set[str] = set()
    for node in ast.iter_child_nodes(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    if not target.id.isupper():
                        module_level_names.add(target.id)
        elif isinstance(node, ast.AnnAssign):
            if isinstance(node.target, ast.Name):
                if not node.target.id.isupper():
                    module_level_names.add(node.target.id)

    if not module_level_names:
        return

    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            func_name = node.name
            for child in ast.walk(node):
                if isinstance(child, ast.Global):
                    for name in child.names:
                        if name in module_level_names:
                            result.add(
                                "FAIL", filepath, node.lineno, func_name,
                                "MUTABLE_GLOBAL",
                                f"'{func_name}' uses 'global {name}' "
                                f"-- pass as parameter instead"
                            )


def check_function_complexity(tree: ast.AST, filepath: str,
                              result: CheckResult) -> None:
    """WARN: Functions over line/parameter thresholds."""
    MAX_LINES = 40
    MAX_PARAMS = 5

    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            func_name = node.name

            if hasattr(node, 'end_lineno') and node.end_lineno:
                line_count = node.end_lineno - node.lineno
                if line_count > MAX_LINES:
                    result.add(
                        "WARN", filepath, node.lineno, func_name,
                        "HIGH_LINE_COUNT",
                        f"'{func_name}' is {line_count} lines "
                        f"(max {MAX_LINES}) -- may have mixed "
                        f"responsibilities"
                    )

            params = [a for a in node.args.args
                      if a.arg not in ("self", "cls")]
            if len(params) > MAX_PARAMS:
                result.add(
                    "WARN", filepath, node.lineno, func_name,
                    "HIGH_PARAM_COUNT",
                    f"'{func_name}' has {len(params)} parameters "
                    f"(max {MAX_PARAMS}) -- consider a config object"
                )


def check_missing_docstrings(tree: ast.AST, filepath: str,
                             result: CheckResult) -> None:
    """WARN: Public functions without docstrings."""
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            func_name = node.name
            if func_name.startswith("_"):
                continue
            docstring = ast.get_docstring(node)
            if not docstring:
                result.add(
                    "WARN", filepath, node.lineno, func_name,
                    "MISSING_DOCSTRING",
                    f"'{func_name}' has no docstring -- add intent "
                    f"and edge descriptions"
                )


# ─────────────────────────────────────────────
# Environment file checks
# ─────────────────────────────────────────────

def check_env_files(root_path: str, result: CheckResult) -> None:
    """FAIL: .env files must not be present unless covered by .gitignore."""
    path = Path(root_path)

    gitignore_patterns: set[str] = set()
    for gitignore_file in path.rglob(".gitignore"):
        try:
            with open(gitignore_file, "r", encoding="utf-8") as f:
                for line in f:
                    stripped = line.strip()
                    if stripped and not stripped.startswith("#"):
                        gitignore_patterns.add(stripped)
        except Exception:
            pass

    def is_env_covered(patterns: set[str]) -> bool:
        """Return True if any pattern matches common .env filenames."""
        test_names = [".env", ".env.local", ".env.production"]
        for pattern in patterns:
            for name in test_names:
                if fnmatch.fnmatch(name, pattern):
                    return True
        return False

    globally_covered = is_env_covered(gitignore_patterns)

    skip_dirs = {"__pycache__", ".git", "dist", "build",
                 "venv", ".venv", "node_modules"}
    env_files: list[Path] = []
    for f in path.rglob("*"):
        if not f.is_file():
            continue
        if any(part in skip_dirs for part in f.parts):
            continue
        name = f.name
        if (name == ".env"
                or (name.startswith(".env") and name not in
                    {".env.example", ".env.template", ".env.sample"})
                or (name.endswith(".env") and name not in
                    {".env.example", ".env.template"})):
            env_files.append(f)

    for env_file in env_files:
        covered = globally_covered or (env_file.name in gitignore_patterns)
        if not covered:
            result.add(
                "FAIL", str(env_file), 0, "<secrets>",
                "ENV_FILE_NOT_IGNORED",
                f"'{env_file.name}' is not covered by any .gitignore -- "
                f"add '.env*' to your root .gitignore"
            )
        else:
            try:
                with open(env_file, "r", encoding="utf-8") as f:
                    env_content = f.read()
                secret_pattern = re.compile(
                    r'^[A-Z_]+=(?!your_|<|\$|example|changeme|xxx|test)'
                    r'[^\n]{8,}',
                    re.MULTILINE | re.IGNORECASE
                )
                if secret_pattern.search(env_content):
                    result.add(
                        "WARN", str(env_file), 0, "<secrets>",
                        "ENV_FILE_HAS_VALUES",
                        f"'{env_file.name}' is gitignored but contains "
                        f"what appear to be real values -- confirm no "
                        f"actual secrets are committed"
                    )
            except Exception:
                pass


# ─────────────────────────────────────────────
# File-level runner
# ─────────────────────────────────────────────

def check_python_file(filepath: str) -> CheckResult:
    """Parse a single Python file and run all structural checks."""
    result = CheckResult()
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            source = f.read()
        tree = ast.parse(source, filename=filepath)
    except SyntaxError as e:
        result.add("FAIL", filepath, e.lineno or 0, "<module>",
                   "SYNTAX_ERROR", f"Syntax error: {e.msg}")
        return result
    except Exception as e:
        result.add("FAIL", filepath, 0, "<module>",
                   "PARSE_ERROR", f"Could not parse file: {e}")
        return result

    check_type_annotations(tree, filepath, result)
    check_module_level_logic(tree, filepath, result)
    check_inline_side_effects(tree, filepath, result, source)
    check_mutable_globals(tree, filepath, result)
    check_function_complexity(tree, filepath, result)
    check_missing_docstrings(tree, filepath, result)

    return result


# ─────────────────────────────────────────────
# Boundary file cataloguing
# ─────────────────────────────────────────────

def catalogue_boundary_file(filepath: str, result: CheckResult) -> None:
    """For .sql and .sh files: note as boundary markers."""
    ext = Path(filepath).suffix.lower()
    if ext == ".sql":
        result.add(
            "WARN", filepath, 0, "<boundary>",
            "SQL_BOUNDARY_MARKER",
            "SQL file catalogued as boundary marker -- queries are "
            "system exit/entry points"
        )
    elif ext == ".sh":
        result.add(
            "WARN", filepath, 0, "<boundary>",
            "SHELL_BOUNDARY_MARKER",
            "Shell script catalogued as external orchestration "
            "entry point"
        )


# ─────────────────────────────────────────────
# Pipeline
# ─────────────────────────────────────────────

SKIP_DIRS = {"venv", ".venv", "__pycache__", ".git", "dist",
             "build", "coverage", "node_modules", ".mypy_cache",
             ".pytest_cache", "__pypackages__"}


def collect_python_files(root_path: str) -> tuple[list[Path], list[Path]]:
    """Return (source_files, boundary_files) under root_path."""
    path = Path(root_path)
    all_files = [
        f for f in path.rglob("*")
        if f.is_file()
        and not any(part in SKIP_DIRS for part in f.parts)
    ]
    py_files = [f for f in all_files if f.suffix == ".py"]
    boundary_files = [f for f in all_files if f.suffix in {".sql", ".sh"}]
    return py_files, boundary_files


def run_pipeline(root_path: str, warn_only: bool = False,
                 output_json: bool = False) -> int:
    """
    Check all Python source files under root_path.
    Returns exit code: 0 = pass, 1 = failures found.
    """
    py_files, boundary_files = collect_python_files(root_path)
    result = CheckResult()

    # Env file checks
    check_env_files(root_path, result)

    # Python files
    for filepath in sorted(py_files):
        file_result = check_python_file(str(filepath))
        result.issues.extend(file_result.issues)

    # Boundary markers
    for filepath in sorted(boundary_files):
        catalogue_boundary_file(str(filepath), result)

    # ── Output ──
    if output_json:
        data = result.to_dict()
        data["files_checked"] = len(py_files)
        data["boundary_files"] = len(boundary_files)
        print(json.dumps(data, indent=2))
    else:
        print("\n" + "=" * 60)
        print("  PYTHON STRUCTURAL CHECK")
        print("=" * 60)
        print(f"  Python files    : {len(py_files)}")
        print(f"  Boundary markers: {len(boundary_files)}")
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
            print("\n  All Python checks passed\n")

    if result.failures and not warn_only:
        return 1
    return 0


# ─────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="DAG Pre-Process: Python structural checks"
    )
    parser.add_argument("path", help="Root path of Python codebase to check")
    parser.add_argument(
        "--warn-only", action="store_true",
        help="Report failures as warnings (exit 0)")
    parser.add_argument(
        "--json", action="store_true", dest="output_json",
        help="Output results as JSON instead of human-readable report")
    args = parser.parse_args()

    sys.exit(run_pipeline(args.path, args.warn_only, args.output_json))
