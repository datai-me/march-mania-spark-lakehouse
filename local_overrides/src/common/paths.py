"""Local override: replace MinIO/S3 paths with local filesystem paths.

This file shadows `src/common/paths.py` when local_overrides is first in PYTHONPATH.
"""

import os

BASE_DIR = os.path.abspath("lakehouse")

BRONZE_PREFIX = os.path.join(BASE_DIR, "bronze", "march_mania")
SILVER_PREFIX = os.path.join(BASE_DIR, "silver", "march_mania")
GOLD_PREFIX   = os.path.join(BASE_DIR, "gold", "march_mania")

LOCAL_INPUT_DIR = os.path.abspath(os.path.join("data", "input"))


def bronze_path(subpath: str) -> str:
    return os.path.join(BRONZE_PREFIX, subpath)


def silver_path(subpath: str) -> str:
    return os.path.join(SILVER_PREFIX, subpath)


def gold_path(subpath: str) -> str:
    return os.path.join(GOLD_PREFIX, subpath)


def bronze_misc_path(stem: str) -> str:
    return os.path.join(BRONZE_PREFIX, "misc", stem)
