#!/usr/bin/env python3
"""Test dlt pipeline: loads sample data to local DuckDB.
Falls back to mock mode if dlt or duckdb aren't installed."""
import os
import json
import logging
from collections.abc import Iterator

logger = logging.getLogger(__name__)


# dag:boundary
def write_metadata(data: dict) -> None:
    """Write pipeline metadata to the path specified by GRANICUS_METADATA_PATH."""
    metadata_path = os.environ.get("GRANICUS_METADATA_PATH")
    if metadata_path:
        with open(metadata_path, "w") as f:
            json.dump(data, f)


def mock_run() -> None:
    """Mock dlt execution when dependencies aren't available."""
    write_metadata({"rows_loaded": "10", "tables_created": "1", "load_duration": "0.1s"})
    logger.info("mock dlt complete: 10 rows loaded")


def real_run() -> None:
    """Run the dlt pipeline with DuckDB as the destination."""
    import dlt

    @dlt.resource
    def sample_data() -> Iterator[dict]:
        """Yield 10 sample rows for testing the pipeline."""
        for i in range(10):
            yield {"id": i, "name": f"item_{i}", "value": i * 10.5}

    pipeline = dlt.pipeline(
        pipeline_name="test_sample",
        destination="duckdb",
        dataset_name="test_data",
    )
    load_info = pipeline.run(sample_data())
    logger.info(f"Load complete: {load_info}")
    write_metadata({"rows_loaded": "10", "tables_created": "1", "load_duration": "0.5s"})


if __name__ == "__main__":
    try:
        import dlt  # noqa: F401
        import duckdb  # noqa: F401
        real_run()
    except ImportError:
        mock_run()
