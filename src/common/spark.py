"""Spark session factory configured for MinIO (S3A) + sane defaults.

This is the most important file in the repo:
- It makes Spark behave like in production (S3 object storage)
- It avoids repeating configs across all jobs
"""

import os
from pyspark.sql import SparkSession

from src.common.logging import get_logger

logger = get_logger(__name__)


def build_spark(app_name: str) -> SparkSession:
    """Create a SparkSession with S3A (MinIO) configuration.

    Environment variables are passed via docker-compose:
    - MINIO_ENDPOINT (e.g., http://minio:9000)
    - MINIO_ACCESS_KEY
    - MINIO_SECRET_KEY

    Returns:
        SparkSession
    """
    endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "admin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "admin123")

    logger.info("Building SparkSession for MinIO endpoint=%s", endpoint)

    # NOTE:
    # - s3a.path.style.access=true is required for MinIO.
    # - ssl.enabled=false because we use http locally; in real deployments use https.
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", os.getenv("TZ", "Indian/Antananarivo"))
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        # Good defaults for small/medium local datasets (tweak for larger datasets)
        .config("spark.sql.shuffle.partitions", "32")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

    return spark
