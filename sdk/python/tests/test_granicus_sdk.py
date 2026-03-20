import json
import os
import tempfile
import unittest
import warnings

from granicus_sdk import GranicusContractError, GranicusEnv, ResourceConfig, ConnectionConfig


_REQUIRED_VARS = {
    "GRANICUS_ASSET_NAME": "test-asset",
    "GRANICUS_RUN_ID": "run-abc",
    "GRANICUS_PROJECT_ROOT": "/tmp/project",
    "GRANICUS_METADATA_PATH": "/tmp/meta.json",
}

_SDK_VARS = list(_REQUIRED_VARS.keys()) + [
    "GRANICUS_INTERVAL_START",
    "GRANICUS_INTERVAL_END",
    "GRANICUS_DEST_RESOURCE",
    "GRANICUS_SOURCE_RESOURCE",
    "GRANICUS_DEST_CONNECTION",
    "GRANICUS_SOURCE_CONNECTION",
    "GRANICUS_REFS",
]


class TestGranicusSDK(unittest.TestCase):
    def setUp(self):
        for key in _SDK_VARS:
            os.environ.pop(key, None)

    def tearDown(self):
        for key in _SDK_VARS:
            os.environ.pop(key, None)

    def _set_required(self, **overrides):
        env = dict(_REQUIRED_VARS)
        env.update(overrides)
        for k, v in env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def test_missing_required_vars(self):
        with self.assertRaises(GranicusContractError):
            GranicusEnv()

    def test_valid_env(self):
        self._set_required()
        env = GranicusEnv()
        self.assertEqual(env.asset_name, "test-asset")
        self.assertEqual(env.run_id, "run-abc")
        self.assertEqual(str(env.project_root), "/tmp/project")
        self.assertEqual(str(env.metadata_path), "/tmp/meta.json")

    def test_resource_parsing(self):
        conn_data = {"name": "bq-dest", "type": "bigquery", "project": "my-project"}
        self._set_required()
        os.environ["GRANICUS_DEST_RESOURCE"] = json.dumps(conn_data)
        env = GranicusEnv()
        self.assertIsNotNone(env.dest_resource)
        self.assertEqual(env.dest_resource.name, "bq-dest")
        self.assertEqual(env.dest_resource.type, "bigquery")
        self.assertEqual(env.dest_resource.properties["project"], "my-project")

    def test_deprecated_connection_fallback(self):
        conn_data = {"name": "bq-dest", "type": "bigquery"}
        self._set_required()
        os.environ["GRANICUS_DEST_CONNECTION"] = json.dumps(conn_data)
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            env = GranicusEnv()
            self.assertTrue(any("deprecated" in str(warning.message).lower() for warning in w))
        self.assertIsNotNone(env.dest_resource)
        self.assertEqual(env.dest_resource.name, "bq-dest")
        # Backward compat property
        self.assertIsNotNone(env.dest_connection)
        self.assertEqual(env.dest_connection.name, "bq-dest")

    def test_invalid_resource_json(self):
        self._set_required()
        os.environ["GRANICUS_DEST_RESOURCE"] = "{not-valid-json"
        with self.assertRaises(GranicusContractError) as ctx:
            GranicusEnv()
        self.assertIn("GRANICUS_DEST_RESOURCE", str(ctx.exception))

    def test_refs_parsing(self):
        refs = {"upstream_table": "project.dataset.table"}
        self._set_required()
        os.environ["GRANICUS_REFS"] = json.dumps(refs)
        env = GranicusEnv()
        self.assertIsNotNone(env.refs)
        self.assertIsInstance(env.refs, dict)
        self.assertEqual(env.refs["upstream_table"], "project.dataset.table")

    def test_write_metadata(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            meta_path = f.name
        try:
            self._set_required(**{"GRANICUS_METADATA_PATH": meta_path})
            env = GranicusEnv()
            env.write_metadata({"rows_affected": 42, "duration_seconds": 1.5})
            with open(meta_path) as fh:
                written = json.load(fh)
            self.assertEqual(written["rows_affected"], 42)
            self.assertAlmostEqual(written["duration_seconds"], 1.5)
        finally:
            os.unlink(meta_path)

    def test_interval_valid_datetime(self):
        self._set_required()
        os.environ["GRANICUS_INTERVAL_START"] = "2025-01-01T00:00:00Z"
        os.environ["GRANICUS_INTERVAL_END"] = "2025-01-02T00:00:00Z"
        env = GranicusEnv()
        self.assertEqual(env.interval_start, "2025-01-01T00:00:00Z")
        self.assertEqual(env.interval_end, "2025-01-02T00:00:00Z")

    def test_interval_rejects_date_only(self):
        self._set_required()
        os.environ["GRANICUS_INTERVAL_START"] = "2025-01-01"
        with self.assertRaises(GranicusContractError) as ctx:
            GranicusEnv()
        self.assertIn("GRANICUS_INTERVAL_START", str(ctx.exception))

    def test_interval_rejects_garbage(self):
        self._set_required()
        os.environ["GRANICUS_INTERVAL_START"] = "not-a-date"
        with self.assertRaises(GranicusContractError) as ctx:
            GranicusEnv()
        self.assertIn("GRANICUS_INTERVAL_START", str(ctx.exception))

    def test_optional_fields_absent(self):
        self._set_required()
        env = GranicusEnv()
        self.assertIsNone(env.interval_start)
        self.assertIsNone(env.interval_end)
        self.assertIsNone(env.dest_resource)
        self.assertIsNone(env.source_resource)
        self.assertIsNone(env.dest_connection)
        self.assertIsNone(env.source_connection)
        self.assertIsNone(env.refs)

    def test_connection_config_backward_compat_alias(self):
        self.assertIs(ConnectionConfig, ResourceConfig)


if __name__ == "__main__":
    unittest.main()
