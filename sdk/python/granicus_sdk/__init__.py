"""Granicus Python SDK.

Reads and validates the environment contract set by PythonRunner, and provides
write_metadata() for reporting results back to Go.

Schema version: 1.0.0
"""
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

SCHEMA_VERSION = "1.0.0"

_METADATA_ALLOWED_KEYS = {
    "rows_affected",
    "tables_created",
    "duration_seconds",
    "rows_loaded",
    "load_duration",
}


class GranicusContractError(Exception):
    """Raised when the environment contract is violated."""


@dataclass
class ConnectionConfig:
    name: str
    type: str
    properties: dict = field(default_factory=dict)

    @classmethod
    def _from_dict(cls, data: dict) -> "ConnectionConfig":
        if not isinstance(data, dict):
            raise GranicusContractError(
                f"ConnectionConfig must be a JSON object, got {type(data).__name__}"
            )
        name = data.get("name")
        conn_type = data.get("type")
        if not name:
            raise GranicusContractError("ConnectionConfig missing required field 'name'")
        if not conn_type:
            raise GranicusContractError("ConnectionConfig missing required field 'type'")
        props = {k: v for k, v in data.items() if k not in ("name", "type")}
        return cls(name=name, type=conn_type, properties=props)


@dataclass
class GranicusEnv:
    asset_name: str
    run_id: str
    project_root: Path
    metadata_path: Path
    interval_start: Optional[str] = None
    interval_end: Optional[str] = None
    dest_connection: Optional[ConnectionConfig] = None
    source_connection: Optional[ConnectionConfig] = None
    refs: Optional[dict] = None

    def __init__(self) -> None:
        env = os.environ

        # Required fields
        asset_name = env.get("GRANICUS_ASSET_NAME")
        if not asset_name:
            raise GranicusContractError(
                "Missing required env var: GRANICUS_ASSET_NAME"
            )

        run_id = env.get("GRANICUS_RUN_ID")
        if not run_id:
            raise GranicusContractError(
                "Missing required env var: GRANICUS_RUN_ID"
            )

        project_root_raw = env.get("GRANICUS_PROJECT_ROOT")
        if not project_root_raw:
            raise GranicusContractError(
                "Missing required env var: GRANICUS_PROJECT_ROOT"
            )

        metadata_path_raw = env.get("GRANICUS_METADATA_PATH")
        if not metadata_path_raw:
            raise GranicusContractError(
                "Missing required env var: GRANICUS_METADATA_PATH"
            )

        self.asset_name = asset_name
        self.run_id = run_id
        self.project_root = Path(project_root_raw)
        self.metadata_path = Path(metadata_path_raw)

        # Optional scalar fields
        self.interval_start = env.get("GRANICUS_INTERVAL_START") or None
        self.interval_end = env.get("GRANICUS_INTERVAL_END") or None

        # Optional JSON fields
        self.dest_connection = self._parse_connection("GRANICUS_DEST_CONNECTION", env)
        self.source_connection = self._parse_connection("GRANICUS_SOURCE_CONNECTION", env)
        self.refs = self._parse_refs(env)

    @staticmethod
    def _parse_connection(
        var: str, env: dict
    ) -> Optional[ConnectionConfig]:
        raw = env.get(var)
        if not raw:
            return None
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise GranicusContractError(
                f"{var} is not valid JSON: {exc}"
            ) from exc
        return ConnectionConfig._from_dict(data)

    @staticmethod
    def _parse_refs(env: dict) -> Optional[dict]:
        raw = env.get("GRANICUS_REFS")
        if not raw:
            return None
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise GranicusContractError(
                f"GRANICUS_REFS is not valid JSON: {exc}"
            ) from exc
        if not isinstance(data, dict):
            raise GranicusContractError(
                f"GRANICUS_REFS must be a JSON object, got {type(data).__name__}"
            )
        for k, v in data.items():
            if not isinstance(v, str):
                raise GranicusContractError(
                    f"GRANICUS_REFS values must be strings; key '{k}' has type {type(v).__name__}"
                )
        return data

    def write_metadata(self, data: dict) -> None:
        """Validate data against MetadataOutput schema and write to metadata_path."""
        unknown = set(data.keys()) - _METADATA_ALLOWED_KEYS
        if unknown:
            raise GranicusContractError(
                f"write_metadata received unknown fields: {sorted(unknown)}. "
                f"Allowed: {sorted(_METADATA_ALLOWED_KEYS)}"
            )
        if "tables_created" in data and not isinstance(data["tables_created"], list):
            raise GranicusContractError(
                "write_metadata: 'tables_created' must be a list of strings"
            )
        for numeric_key in ("rows_affected", "duration_seconds", "rows_loaded", "load_duration"):
            if numeric_key in data and not isinstance(data[numeric_key], (int, float)):
                raise GranicusContractError(
                    f"write_metadata: '{numeric_key}' must be a number"
                )
        self.metadata_path.parent.mkdir(parents=True, exist_ok=True)
        self.metadata_path.write_text(json.dumps(data))


__all__ = ["GranicusEnv", "ConnectionConfig", "GranicusContractError"]
