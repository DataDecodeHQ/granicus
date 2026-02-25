# granicus:
#   depends_on: [fetch_api_data]
#   produces: [cleaned_api_events]

import json
import sys
from pathlib import Path

INPUT = Path("/tmp/mixed_pipeline/raw_events.json")
OUTPUT = Path("/tmp/mixed_pipeline/cleaned_events.json")


def main():
    if not INPUT.exists():
        print(f"ERROR: {INPUT} not found", file=sys.stderr)
        sys.exit(1)

    with open(INPUT) as f:
        events = json.load(f)

    cleaned = [e for e in events if e.get("ts") is not None]
    dropped = len(events) - len(cleaned)

    with open(OUTPUT, "w") as f:
        json.dump(cleaned, f, indent=2)

    print(f"Cleaned {len(cleaned)} events, dropped {dropped} with null timestamps")


if __name__ == "__main__":
    main()
