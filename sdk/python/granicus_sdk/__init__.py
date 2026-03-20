"""Granicus Python SDK.

Reads and validates the environment contract set by PythonRunner, and provides
write_metadata() for reporting results back to Go.

Schema version: 1.0.0
"""
import json
import os
import re
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
class ResourceConfig:
    name: str
    type: str
    properties: dict = field(default_factory=dict)

    @classmethod
    def _from_dict(cls, data: dict) -> "ResourceConfig":
        if not isinstance(data, dict):
            raise GranicusContractError(
                f"ResourceConfig must be a JSON object, got {type(data).__name__}"
            )
        name = data.get("name")
        conn_type = data.get("type")
        if not name:
            raise GranicusContractError("ResourceConfig missing required field 'name'")
        if not conn_type:
            raise GranicusContractError("ResourceConfig missing required field 'type'")
        props = {k: v for k, v in data.items() if k not in ("name", "type")}
        return cls(name=name, type=conn_type, properties=props)


# Backward compat alias
ConnectionConfig = ResourceConfig


@dataclass
class GranicusEnv:
    asset_name: str
    run_id: str
    project_root: Path
    metadata_path: Path
    interval_start: Optional[str] = None
    interval_end: Optional[str] = None
    dest_resource: Optional[ResourceConfig] = None
    source_resource: Optional[ResourceConfig] = None
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

        # Optional scalar fields — validate ISO 8601 datetime format
        self.interval_start = self._parse_interval("GRANICUS_INTERVAL_START", env)
        self.interval_end = self._parse_interval("GRANICUS_INTERVAL_END", env)

        # Optional JSON fields (new names, with deprecated fallback)
        self.dest_resource = self._parse_resource("GRANICUS_DEST_RESOURCE", "GRANICUS_DEST_CONNECTION", env)
        self.source_resource = self._parse_resource("GRANICUS_SOURCE_RESOURCE", "GRANICUS_SOURCE_CONNECTION", env)
        self.refs = self._parse_refs(env)

    @property
    def dest_connection(self) -> Optional[ResourceConfig]:
        """Deprecated: use dest_resource instead."""
        return self.dest_resource

    @property
    def source_connection(self) -> Optional[ResourceConfig]:
        """Deprecated: use source_resource instead."""
        return self.source_resource

    _INTERVAL_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")

    @staticmethod
    def _parse_interval(var: str, env: dict) -> Optional[str]:
        raw = env.get(var)
        if not raw:
            return None
        if not GranicusEnv._INTERVAL_RE.match(raw):
            raise GranicusContractError(
                f"{var} must be ISO 8601 datetime (YYYY-MM-DDTHH:MM:SSZ), got: {raw}"
            )
        return raw

    @staticmethod
    def _parse_resource(
        var: str, deprecated_var: str, env: dict
    ) -> Optional[ResourceConfig]:
        raw = env.get(var)
        used_var = var
        if not raw and deprecated_var:
            raw = env.get(deprecated_var)
            if raw:
                used_var = deprecated_var
                import warnings
                warnings.warn(
                    f"{deprecated_var} is deprecated, use {var} instead",
                    DeprecationWarning,
                    stacklevel=3,
                )
        if not raw:
            return None
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise GranicusContractError(
                f"{used_var} is not valid JSON: {exc}"
            ) from exc
        return ResourceConfig._from_dict(data)

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


__all__ = ["GranicusEnv", "ResourceConfig", "ConnectionConfig", "GranicusContractError"]
