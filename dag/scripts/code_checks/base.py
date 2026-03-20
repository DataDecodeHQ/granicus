"""
Shared base types and utilities for DAG pre-process structural checkers.

Provides Issue, CheckResult, print_report(), and run_pipeline_base()
so individual checkers (check_go.py, check_python.py) don't duplicate
boilerplate.
"""

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

    def to_dict(self, language: str = "unknown") -> dict:
        """Serialize the result as a summary dictionary."""
        return {
            "language": language,
            "total": len(self.issues),
            "failures": len(self.failures),
            "warnings": len(self.warnings),
            "issues": [asdict(i) for i in self.issues],
        }


# ─────────────────────────────────────────────
# Report printer
# ─────────────────────────────────────────────

def print_report(result: CheckResult, title: str, summary_lines: list[str],
                 pass_message: str, warn_only: bool) -> None:
    """
    Print a human-readable report for a CheckResult.

    title         -- e.g. "GO STRUCTURAL CHECK"
    summary_lines -- extra lines printed between the title and counts,
                     e.g. ["  Go source files : 12", "  Go test files   : 3 (skipped)"]
    pass_message  -- text printed when there are no failures,
                     e.g. "  All Go checks passed"
    warn_only     -- if True, failures don't block (exit 0 semantics for display)
    """
    print("\n" + "=" * 60)
    print(f"  {title}")
    print("=" * 60)
    for line in summary_lines:
        print(line)
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
        print(f"\n{pass_message}\n")


def run_pipeline_base(result: CheckResult, warn_only: bool) -> int:
    """
    Return the exit code for a completed pipeline run.
    Returns 1 if there are failures and warn_only is False, else 0.
    """
    if result.failures and not warn_only:
        return 1
    return 0
