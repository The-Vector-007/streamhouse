"""P3.1 - the SparkSession builder.

Delta is not built into Spark. It arrives as a Maven package plus two config
entries, and both configs have to be set on the *builder*: Spark freezes its
SparkConf when the JVM starts, so a later `spark.conf.set` would not appear in
`sparkContext.getConf()` and the extension would never be installed.

Sized for a laptop, not a cluster. This runs on the host inside an 8 GB WSL VM
that also hosts Kafka, so every default here trades throughput for headroom.
See docs/RUNBOOK.md.
"""

import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

DELTA_EXTENSION = "io.delta.sql.DeltaSparkSessionExtension"
DELTA_CATALOG = "org.apache.spark.sql.delta.catalog.DeltaCatalog"

# Two cores, not local[*]. The box has six, but the Spark driver shares 8 GB with
# a Kafka container, and in local mode the driver is also the executor: more
# threads means more concurrent task memory, not more speed.
DEFAULT_MASTER = os.getenv("STREAMHOUSE_SPARK_MASTER", "local[2]")

# Spark defaults to 200 shuffle partitions, which is sane on a cluster and absurd
# here: 200 near-empty files per shuffle, and the task scheduling overhead dwarfs
# the work. Eight is roughly the partition count of txn.raw.
DEFAULT_SHUFFLE_PARTITIONS = 8


def build_session(
    app_name: str = "streamhouse",
    *,
    master: str = DEFAULT_MASTER,
    shuffle_partitions: int = DEFAULT_SHUFFLE_PARTITIONS,
) -> SparkSession:
    """A local SparkSession with Delta configured.

    Idempotent: Spark returns the existing session if one is already running, so
    calling this twice in a process gives you the first session's config, not the
    second's.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.extensions", DELTA_EXTENSION)
        .config("spark.sql.catalog.spark_catalog", DELTA_CATALOG)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        # Every timestamp in this pipeline is tz-aware UTC on the wire. Pinning the
        # session zone stops Spark reinterpreting them in the host's local zone,
        # which would silently shift every event-time window by the UTC offset.
        .config("spark.sql.session.timeZone", "UTC")
        # The web UI binds a port and holds memory for an interface nobody reads
        # during a test run.
        .config("spark.ui.enabled", "false")
        # Delta writes many small files under streaming; letting Spark coalesce
        # adaptively keeps the file count survivable without a manual repartition.
        .config("spark.sql.adaptive.enabled", "true")
    )

    # This is what injects spark.jars.packages=io.delta:delta-spark_2.12:3.2.1 and
    # makes Ivy fetch it. First call on a cold machine downloads ~6 MB; every call
    # after that resolves from ~/.ivy2.
    session = configure_spark_with_delta_pip(builder).getOrCreate()

    # Spark's INFO chatter buries actual failures in a test run.
    session.sparkContext.setLogLevel("WARN")
    return session
