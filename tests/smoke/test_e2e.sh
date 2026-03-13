#!/bin/bash
# End-to-end validation: validate + test execution + verify state + verify context
# Combines validate and run smoke tests, then verifies artifacts
set -euo pipefail

ANALYTEHEALTH_ROOT="${ANALYTEHEALTH_ROOT:-/opt/projects/AnalyteHealth}"
PIPELINE_DIR="$ANALYTEHEALTH_ROOT/project/granicus_pipeline/analyte_health"
ASSETS="stg_orders"
FAILED=0

log() { echo "=== $1 ==="; }
pass() { echo "PASS: $1"; }
fail() { echo "FAIL: $1"; FAILED=1; }

log "E2E Validation"
echo "ANALYTEHEALTH_ROOT=$ANALYTEHEALTH_ROOT"
echo "PIPELINE_DIR=$PIPELINE_DIR"

# Phase 1: Validate pipeline config
log "Phase 1: granicus validate"
if [ ! -f "$PIPELINE_DIR/pipeline.yaml" ]; then
    fail "pipeline.yaml not found at $PIPELINE_DIR"
    exit 1
fi

validate_output=$(granicus validate "$PIPELINE_DIR/pipeline.yaml" --project-root "$PIPELINE_DIR" --output json 2>/dev/null || true)

for phase in dependencies source_files template_parse ref_resolution source_resolution; do
    status=$(echo "$validate_output" | python3 -c "
import json, sys
data = json.load(sys.stdin)
for c in data.get('checks', []):
    if c['name'] == '$phase':
        print(c['status'])
        sys.exit(0)
print('missing')
" 2>/dev/null)

    if [ "$status" = "pass" ] || [ "$status" = "warn" ]; then
        pass "validate: $phase ($status)"
    else
        fail "validate: $phase ($status)"
    fi
done

# Phase 2: Test execution (small asset subset)
log "Phase 2: granicus run --test"
granicus run "$PIPELINE_DIR/pipeline.yaml" \
    --project-root "$PIPELINE_DIR" \
    --test \
    --test-window 7d \
    --assets "$ASSETS"
run_status=$?

if [ $run_status -eq 0 ]; then
    pass "granicus run --test exited 0"
else
    fail "granicus run --test exited $run_status"
fi

# Phase 3: Verify state
log "Phase 3: Verify state artifacts"
STATE_DB="$PIPELINE_DIR/.granicus/test-state.db"
if [ -f "$STATE_DB" ]; then
    pass "test-state.db exists at $STATE_DB"
    # Verify it has content
    row_count=$(python3 -c "
import sqlite3, sys
try:
    conn = sqlite3.connect('$STATE_DB')
    count = conn.execute('SELECT COUNT(*) FROM intervals').fetchone()[0]
    print(count)
    conn.close()
except Exception as e:
    print(0)
" 2>/dev/null)
    if [ "$row_count" -gt 0 ] 2>/dev/null; then
        pass "test-state.db has $row_count interval records"
    else
        fail "test-state.db has no interval records (or table missing)"
    fi
else
    fail "test-state.db not found at $STATE_DB"
fi

# Verify state NOT in engine directory
ENGINE_STATE="$ANALYTEHEALTH_ROOT/granicus/.granicus/state.db"
if [ -f "$ENGINE_STATE" ]; then
    fail "state.db found in engine directory (should be in pipeline dir only)"
else
    pass "no state.db in engine directory"
fi

# Phase 4: Verify context
log "Phase 4: Verify context artifacts"
CONTEXT_DB="$PIPELINE_DIR/.granicus/context.db"
if [ -f "$CONTEXT_DB" ]; then
    pass "context.db exists at $CONTEXT_DB"
    # Verify it has tables
    table_count=$(python3 -c "
import sqlite3
conn = sqlite3.connect('$CONTEXT_DB')
tables = [r[0] for r in conn.execute(\"SELECT name FROM sqlite_master WHERE type='table'\").fetchall()]
print(len(tables))
conn.close()
" 2>/dev/null)
    if [ "$table_count" -gt 0 ] 2>/dev/null; then
        pass "context.db has $table_count tables"
    else
        fail "context.db has no tables"
    fi
else
    fail "context.db not found at $CONTEXT_DB (may need a full run to generate)"
fi

# Phase 5: Verify integration tests pass
log "Phase 5: Integration tests"
cd "$ANALYTEHEALTH_ROOT/granicus"
if go test ./tests/integration/ -count=1 -timeout 60s 2>&1; then
    pass "integration tests passed"
else
    fail "integration tests failed"
fi

# Phase 6: Verify smoke tests pass
log "Phase 6: Smoke tests (validate)"
if bash "$ANALYTEHEALTH_ROOT/granicus/tests/smoke/test_validate.sh" 2>&1; then
    pass "smoke test (validate) passed"
else
    fail "smoke test (validate) failed"
fi

# Summary
echo ""
log "E2E Summary"
if [ $FAILED -eq 0 ]; then
    echo "ALL CHECKS PASSED"
    exit 0
else
    echo "SOME CHECKS FAILED"
    exit 1
fi
