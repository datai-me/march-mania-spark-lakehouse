"""Local override: Spark session builder for local runs (no Docker, no MinIO).

This file shadows `src/common/spark.py` when PYTHONPATH is set to:
    local_overrides;.

See scripts/run_local.ps1 and scripts/run_local_fast.ps1
"""

from pyspark.sql import SparkSession


def build_spark(app_name: str):
    # local[*] uses all CPU cores.
    # Keep partitions moderate for laptops/desktops.
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
