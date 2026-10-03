"""A small, deterministic local SparkSession (no cluster, no network, UTC session time zone)."""

from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession


def get_spark(app: str = "pipewright") -> SparkSession:
    # Python workers (used for DataFrames made from Python rows, e.g. in tests) must run this same interpreter
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    return (
        SparkSession.builder.master(os.environ.get("SPARK_MASTER", "local[2]"))
        .appName(app)
        # local mode only talks to itself: bind to loopback (containers with odd hostnames, host networking)
        .config("spark.driver.bindAddress", os.environ.get("SPARK_DRIVER_BIND_ADDRESS", "127.0.0.1"))
        .config("spark.driver.host", os.environ.get("SPARK_DRIVER_HOST", "127.0.0.1"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "1g"))
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .getOrCreate()
    )
