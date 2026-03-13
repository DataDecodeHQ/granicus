#!/bin/bash
# Smoke test: granicus validate with split layout
# Verifies assets, connections, and check files resolve from project/
# Note: validate may exit non-zero for code quality warnings (hardcoded refs)
# This test verifies the config contract works (discovery, parsing, resolution)
set -euo pipefail

ANALYTEHEALTH_ROOT="${ANALYTEHEALTH_ROOT:-/opt/projects/AnalyteHealth}"
PIPELINE_DIR="$ANALYTEHEALTH_ROOT/project/granicus_pipeline/analyte_health"

echo "=== Smoke test: granicus validate ==="
echo "ANALYTEHEALTH_ROOT=$ANALYTEHEALTH_ROOT"
echo "PIPELINE_DIR=$PIPELINE_DIR"

if [ ! -f "$PIPELINE_DIR/pipeline.yaml" ]; then
    echo "FAIL: pipeline.yaml not found at $PIPELINE_DIR"
    exit 1
fi

output=$(granicus validate "$PIPELINE_DIR/pipeline.yaml" --project-root "$PIPELINE_DIR" --output json 2>/dev/null || true)

failed=0
for phase in dependencies source_files template_parse ref_resolution source_resolution; do
    status=$(echo "$output" | python3 -c "
import json, sys
data = json.load(sys.stdin)
for c in data.get('checks', []):
    if c['name'] == '$phase':
        print(c['status'])
        sys.exit(0)
print('missing')
" 2>/dev/null)

    if [ "$status" = "pass" ] || [ "$status" = "warn" ]; then
        echo "PASS: $phase ($status)"
    else
        echo "FAIL: $phase ($status)"
        failed=1
    fi
done

if [ $failed -eq 0 ]; then
    echo "PASS: All config contract checks passed"
else
    echo "FAIL: Some config contract checks failed"
    exit 1
fi
