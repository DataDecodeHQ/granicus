"""Tests for check_go.py — Go structural checker."""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from check_go import (
    check_error_handling,
    check_inline_side_effects,
    check_package_level_vars,
    check_go_file,
    CheckResult,
)

# Absolute path to the sql_helpers.go file containing collectBQMetadata.
# __file__ is granicus/dag/scripts/code_checks/test_check_go.py;
# three levels up reaches the granicus repo root.
_SQL_HELPERS = os.path.normpath(os.path.join(
    os.path.dirname(__file__),
    "..", "..", "..",
    "internal", "runner", "sql_helpers.go",
))


def _make_func(start_line: int, end_line: int, name: str = "doThing") -> dict:
    return {
        "name": name,
        "start_line": start_line,
        "end_line": end_line,
        "receiver": None,
        "exported": False,
        "params_str": "",
    }


class TestSwallowedErrorSuppression:
    """SWALLOWED_ERROR should be suppressed when // dag:intentional is present."""

    def test_without_dag_intentional_emits_warning(self):
        # "_ = foo()" matches the swallowed-error regex (single blank identifier)
        lines = [
            "func doThing() {\n",          # line 1
            "    _ = foo()\n",              # line 2 — swallowed error, no annotation
            "}\n",                           # line 3
        ]
        func = _make_func(start_line=1, end_line=3)
        result = CheckResult()
        check_error_handling(lines, [func], "fake.go", result)
        rules = [i.rule for i in result.warnings]
        assert "SWALLOWED_ERROR" in rules, (
            "Expected SWALLOWED_ERROR warning for unannotated discarded error"
        )

    def test_with_dag_intentional_suppresses_warning(self):
        lines = [
            "func doThing() {\n",                               # line 1
            "    _ = foo() // dag:intentional\n",               # line 2 — annotated
            "}\n",                                               # line 3
        ]
        func = _make_func(start_line=1, end_line=3)
        result = CheckResult()
        check_error_handling(lines, [func], "fake.go", result)
        rules = [i.rule for i in result.warnings]
        assert "SWALLOWED_ERROR" not in rules, (
            "Expected no SWALLOWED_ERROR warning when // dag:intentional is present"
        )


class TestCollectBQMetadataBoundary:
    """
    collectBQMetadata in sql_helpers.go interacts with BigQuery metadata
    and triggers INLINE_SIDE_EFFECT_DATABASE unless annotated // dag:boundary.

    Task 3npu: write this test so it currently FAILS (no annotation yet),
    and will PASS once task 3uk9 adds // dag:boundary to the function.
    """

    def test_without_dag_boundary_emits_failure(self):
        """INLINE_SIDE_EFFECT_DATABASE is raised for an unannotated BQ-touching function."""
        lines = [
            "func collectBQMetadata(status *bigquery.JobStatus, job *bigquery.Job) map[string]string {\n",
            "    metadata := make(map[string]string)\n",
            "    if stats := status.Statistics; stats != nil {\n",
            "        metadata[\"bytes\"] = strconv.FormatInt(stats.TotalBytesProcessed, 10)\n",
            "    }\n",
            "    return metadata\n",
            "}\n",
        ]
        func = {
            "name": "collectBQMetadata",
            "start_line": 1,
            "end_line": 7,
            "receiver": None,
            "exported": False,
            "params_str": "status *bigquery.JobStatus, job *bigquery.Job",
        }
        result = CheckResult()
        check_inline_side_effects(lines, [func], "sql_helpers.go", result)
        rules = [i.rule for i in result.failures]
        assert "INLINE_SIDE_EFFECT_DATABASE" in rules, (
            "Expected INLINE_SIDE_EFFECT_DATABASE for collectBQMetadata without dag:boundary"
        )

    def test_with_dag_boundary_suppresses_failure(self):
        """// dag:boundary above collectBQMetadata suppresses INLINE_SIDE_EFFECT_DATABASE."""
        lines = [
            "// dag:boundary\n",
            "func collectBQMetadata(status *bigquery.JobStatus, job *bigquery.Job) map[string]string {\n",
            "    metadata := make(map[string]string)\n",
            "    if stats := status.Statistics; stats != nil {\n",
            "        metadata[\"bytes\"] = strconv.FormatInt(stats.TotalBytesProcessed, 10)\n",
            "    }\n",
            "    return metadata\n",
            "}\n",
        ]
        func = {
            "name": "collectBQMetadata",
            "start_line": 2,
            "end_line": 8,
            "receiver": None,
            "exported": False,
            "params_str": "status *bigquery.JobStatus, job *bigquery.Job",
        }
        result = CheckResult()
        check_inline_side_effects(lines, [func], "sql_helpers.go", result)
        rules = [i.rule for i in result.failures]
        assert "INLINE_SIDE_EFFECT_DATABASE" not in rules, (
            "Expected no INLINE_SIDE_EFFECT_DATABASE when // dag:boundary is present"
        )

    def test_sql_helpers_go_no_inline_side_effect_failures(self):
        """
        check_go.py must report zero INLINE_SIDE_EFFECT_DATABASE failures for
        sql_helpers.go once // dag:boundary is added to collectBQMetadata.

        This test currently FAILS because the annotation has not yet been added
        (that is task 3uk9). It will PASS after 3uk9 lands.
        """
        result = check_go_file(_SQL_HELPERS)
        inline_failures = [
            i for i in result.failures
            if i.rule.startswith("INLINE_SIDE_EFFECT")
        ]
        assert inline_failures == [], (
            "Expected no INLINE_SIDE_EFFECT failures in sql_helpers.go after dag:boundary "
            "is added to collectBQMetadata. Still failing:\n"
            + "\n".join(f"  line {i.line}: {i.rule} — {i.message}" for i in inline_failures)
        )


class TestDispatchBlockNoFalsePositivePackageLevelVar:
    """
    The dispatch block in node_runner.go (currently executePipeline in serve.go)
    declares 'var r runner.NodeResult' and 'var derr error' inside a closure that
    is itself inside a named function. These are closure-local variables, not
    package-level state, and must not trigger PACKAGE_LEVEL_VAR.

    Task 75dr: this test verifies the checker never false-positives on this
    pattern. It passes immediately because check_package_level_vars already
    skips all lines inside a function body via in_func tracking.
    """

    # Minimal synthetic snippet that mirrors the dispatch block in serve.go /
    # the future node_runner.go. The outer named function sets in_func=True so
    # the var declarations inside the closure are skipped by the checker.
    _DISPATCH_BLOCK_LINES = [
        "package main\n",
        "\n",
        "func executePipeline() {\n",                                    # line 3 — in_func starts
        "\trunnerFunc := func(asset string, pr string, rid string) string {\n",
        "\t\tvar r string\n",                                            # line 5 — closure-local
        "\t\tif dispatch != nil {\n",
        "\t\t\tvar derr error\n",                                        # line 7 — closure-local
        "\t\t\tr, derr = dispatch.Execute()\n",
        "\t\t\t_ = derr\n",
        "\t\t} else {\n",
        "\t\t\tr = registry.Run()\n",
        "\t\t}\n",
        "\t\treturn r\n",
        "\t}\n",
        "\t_ = runnerFunc\n",
        "}\n",                                                            # line 16 — in_func ends
    ]

    def test_closure_local_r_not_flagged(self):
        """var r inside a closure must not produce PACKAGE_LEVEL_VAR."""
        result = CheckResult()
        check_package_level_vars(
            [l.rstrip("\n") for l in self._DISPATCH_BLOCK_LINES],
            "node_runner.go",
            result,
        )
        flagged_names = [
            i.message for i in result.warnings
            if i.rule == "PACKAGE_LEVEL_VAR" and "'r'" in i.message
        ]
        assert flagged_names == [], (
            "PACKAGE_LEVEL_VAR false-positive on closure-local 'r':\n"
            + "\n".join(flagged_names)
        )

    def test_closure_local_derr_not_flagged(self):
        """var derr inside a closure must not produce PACKAGE_LEVEL_VAR."""
        result = CheckResult()
        check_package_level_vars(
            [l.rstrip("\n") for l in self._DISPATCH_BLOCK_LINES],
            "node_runner.go",
            result,
        )
        flagged_names = [
            i.message for i in result.warnings
            if i.rule == "PACKAGE_LEVEL_VAR" and "'derr'" in i.message
        ]
        assert flagged_names == [], (
            "PACKAGE_LEVEL_VAR false-positive on closure-local 'derr':\n"
            + "\n".join(flagged_names)
        )

    def test_serve_go_dispatch_block_no_package_level_var(self):
        """
        The real dispatch block in serve.go (executePipeline) must produce zero
        PACKAGE_LEVEL_VAR warnings for 'r' and 'derr'.

        Note: this test passes immediately. The checker already handles this
        correctly because var r / var derr appear inside the executePipeline
        function body where in_func=True causes the checker to skip them.
        Once task 0x1u restructures the block to use ':=' in each branch, this
        test continues to pass (no var declarations remain at all).
        """
        serve_go = os.path.normpath(os.path.join(
            os.path.dirname(__file__),
            "..", "..", "..",
            "cmd", "granicus", "serve.go",
        ))
        if not os.path.exists(serve_go):
            return  # file may be renamed to node_runner.go after 0x1u lands

        with open(serve_go) as f:
            lines = f.read().splitlines()

        result = CheckResult()
        check_package_level_vars(lines, serve_go, result)

        flagged = [
            i for i in result.warnings
            if i.rule == "PACKAGE_LEVEL_VAR"
            and ("'r'" in i.message or "'derr'" in i.message)
        ]
        assert flagged == [], (
            "PACKAGE_LEVEL_VAR false-positives for r/derr in serve.go dispatch block:\n"
            + "\n".join(f"  line {i.line}: {i.message}" for i in flagged)
        )
