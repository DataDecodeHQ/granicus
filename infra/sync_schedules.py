#!/usr/bin/env python3
"""Sync pipeline schedules from pipeline.yaml files to Terraform tfvars."""

import json
import sys
from pathlib import Path

import yaml

PIPELINES_DIR = Path(__file__).resolve().parent.parent.parent / "project" / "granicus_pipeline"
OUTPUT_FILE = Path(__file__).resolve().parent / "pipelines.auto.tfvars.json"

REQUIRED_FIELDS = ("pipeline", "client", "schedule")


def main() -> int:
    """Read pipeline.yaml files and write schedule data to Terraform tfvars."""
    errors: list[str] = []
    schedules: dict[str, dict[str, str]] = {}

    for pipeline_yaml in sorted(PIPELINES_DIR.glob("*/pipeline.yaml")):
        with open(pipeline_yaml) as f:
            config = yaml.safe_load(f)

        pipeline_dir = pipeline_yaml.parent.name

        if not config or not isinstance(config, dict):
            errors.append(f"{pipeline_dir}: invalid or empty pipeline.yaml")
            continue

        missing = [f for f in REQUIRED_FIELDS if f not in config]
        if missing:
            if "schedule" in missing and len(missing) == 1:
                continue  # no schedule = not externally triggered
            errors.append(f"{pipeline_dir}: missing required fields: {', '.join(missing)}")
            continue

        name = config["pipeline"]
        if name != pipeline_dir:
            errors.append(f"{pipeline_dir}: pipeline field '{name}' doesn't match directory name")
            continue

        schedules[name] = {
            "client": config["client"],
            "schedule": config["schedule"],
        }

    if errors:
        for e in errors:
            print(f"ERROR: {e}", file=sys.stderr)
        return 1

    output = {"pipeline_schedules": schedules}
    OUTPUT_FILE.write_text(json.dumps(output, indent=2) + "\n")
    print(f"Wrote {len(schedules)} pipeline schedule(s) to {OUTPUT_FILE.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
