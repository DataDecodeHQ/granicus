import json
import os
import tempfile
import unittest

from granicus_sdk import GranicusContractError, GranicusEnv


_REQUIRED_VARS = {
    "GRANICUS_ASSET_NAME": "test-asset",
    "GRANICUS_RUN_ID": "run-abc",
    "GRANICUS_PROJECT_ROOT": "/tmp/project",
    "GRANICUS_METADATA_PATH": "/tmp/meta.json",
}

_SDK_VARS = list(_REQUIRED_VARS.keys()) + [
    "GRANICUS_INTERVAL_START",
    "GRANICUS_INTERVAL_END",
    "GRANICUS_DEST_CONNECTION",
    "GRANICUS_SOURCE_CONNECTION",
    "GRANICUS_REFS",
]


class TestGranicusSDK(unittest.TestCase):
    def setUp(self):
        # Remove all SDK env vars before each test
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

    # 1. test_missing_required_vars
    def test_missing_required_vars(self):
        with self.assertRaises(GranicusContractError):
            GranicusEnv()

    # 2. test_valid_env
    def test_valid_env(self):
        self._set_required()
        env = GranicusEnv()
        self.assertEqual(env.asset_name, "test-asset")
        self.assertEqual(env.run_id, "run-abc")
        self.assertEqual(str(env.project_root), "/tmp/project")
        self.assertEqual(str(env.metadata_path), "/tmp/meta.json")

    # 3. test_connection_parsing
    def test_connection_parsing(self):
        conn_data = {"name": "bq-dest", "type": "bigquery", "project": "my-project"}
        self._set_required()
        os.environ["GRANICUS_DEST_CONNECTION"] = json.dumps(conn_data)
        env = GranicusEnv()
        self.assertIsNotNone(env.dest_connection)
        self.assertEqual(env.dest_connection.name, "bq-dest")
        self.assertEqual(env.dest_connection.type, "bigquery")
        self.assertEqual(env.dest_connection.properties["project"], "my-project")

    # 4. test_invalid_connection_json
    def test_invalid_connection_json(self):
        self._set_required()
        os.environ["GRANICUS_DEST_CONNECTION"] = "{not-valid-json"
        with self.assertRaises(GranicusContractError) as ctx:
            GranicusEnv()
        self.assertIn("GRANICUS_DEST_CONNECTION", str(ctx.exception))

    # 5. test_refs_parsing
    def test_refs_parsing(self):
        refs = {"upstream_table": "project.dataset.table"}
        self._set_required()
        os.environ["GRANICUS_REFS"] = json.dumps(refs)
        env = GranicusEnv()
        self.assertIsNotNone(env.refs)
        self.assertIsInstance(env.refs, dict)
        self.assertEqual(env.refs["upstream_table"], "project.dataset.table")

    # 6. test_write_metadata
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

    # 7. test_optional_fields_absent
    def test_optional_fields_absent(self):
        self._set_required()
        env = GranicusEnv()
        self.assertIsNone(env.interval_start)
        self.assertIsNone(env.interval_end)
        self.assertIsNone(env.dest_connection)
        self.assertIsNone(env.source_connection)
        self.assertIsNone(env.refs)


if __name__ == "__main__":
    unittest.main()
