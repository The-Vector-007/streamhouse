"""Shared fixtures.

Tests that need Spark or Kafka are marked, so you can run the fast ones while
iterating:

    uv run pytest -m "not spark and not kafka"
"""

import pytest


@pytest.fixture(scope="session")
def spark():
    """A local SparkSession with Delta configured.

    Depends on src/streamhouse/spark.py, which you write in P3.1.
    """
    from streamhouse.spark import build_session

    try:
        session = build_session(app_name="streamhouse-tests")
    except NotImplementedError as exc:
        pytest.skip(str(exc))
    yield session
    session.stop()


@pytest.fixture
def warehouse(tmp_path):
    """An isolated Delta warehouse root, thrown away after each test."""
    root = tmp_path / "warehouse"
    root.mkdir()
    return root


@pytest.fixture
def checkpoints(tmp_path):
    root = tmp_path / "checkpoints"
    root.mkdir()
    return root
