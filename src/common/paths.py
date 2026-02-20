"""Centralized lake paths.

We store all data inside MinIO (S3-compatible) so paths are expressed using the `s3a://` scheme.
"""

import os

DEFAULT_BUCKET = os.getenv("MINIO_BUCKET", "kaggle-lake")

BRONZE_PREFIX = f"s3a://{DEFAULT_BUCKET}/bronze/march_mania"
SILVER_PREFIX = f"s3a://{DEFAULT_BUCKET}/silver/march_mania"
GOLD_PREFIX = f"s3a://{DEFAULT_BUCKET}/gold/march_mania"

# Input folder mounted in containers (see docker-compose.yml)
LOCAL_INPUT_DIR = "/opt/project/data/input"


def bronze_path(dataset: str) -> str:
    return f"{BRONZE_PREFIX}/{dataset}"


def silver_path(dataset: str) -> str:
    return f"{SILVER_PREFIX}/{dataset}"


def gold_path(dataset: str) -> str:
    return f"{GOLD_PREFIX}/{dataset}"


def bronze_misc_path(stem: str) -> str:
    return f"{BRONZE_PREFIX}/misc/{stem}"
