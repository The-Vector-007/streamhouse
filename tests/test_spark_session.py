"""P3.1 — the SparkSession builder.

You write: src/streamhouse/spark.py

Delta is not built into Spark. It arrives as a package plus two config entries,
and getting them wrong is the most common reason a first Delta setup fails.
"""

import pytest


@pytest.mark.spark
def test_session_starts(spark):
    assert spark.version.startswith("3.5")


@pytest.mark.spark
def test_delta_extensions_are_configured(spark):
    conf = dict(spark.sparkContext.getConf().getAll())
    assert "io.delta.sql.DeltaSparkSessionExtension" in conf.get("spark.sql.extensions", "")
    assert "DeltaCatalog" in conf.get("spark.sql.catalog.spark_catalog", "")


@pytest.mark.spark
def test_can_write_and_read_a_delta_table(spark, warehouse):
    path = str(warehouse / "smoke")
    spark.range(10).write.format("delta").save(path)
    assert spark.read.format("delta").load(path).count() == 10


@pytest.mark.spark
def test_shuffle_partitions_tuned_for_local(spark):
    """Spark defaults to 200 shuffle partitions. On a 6-core laptop that is 200
    tiny tasks and mostly scheduling overhead. Interviewers ask about this.
    """
    assert int(spark.conf.get("spark.sql.shuffle.partitions")) <= 16
