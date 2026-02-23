"""
paths.py — Chemins centralisés du lakehouse
═════════════════════════════════════════════
Toutes les adresses S3A (MinIO) et locales passent par ce module.
Aucun job ne doit construire de chemin manuellement.

Structure du lakehouse dans MinIO :
    s3a://<bucket>/
        bronze/march_mania/     ← données brutes Kaggle (Parquet)
        silver/march_mania/     ← features nettoyées et enrichies
        gold/march_mania/       ← datasets ML-ready

Mode local (sans MinIO) :
    Surcharger MINIO_BUCKET ou utiliser LOCAL_INPUT_DIR pour les CSV source.
"""

import os
from pathlib import Path

# ── Bucket MinIO ──────────────────────────────────────────────────────────────
_BUCKET = os.getenv("MINIO_BUCKET", "kaggle-lake")

BRONZE_PREFIX = f"s3a://{_BUCKET}/bronze/march_mania"
SILVER_PREFIX = f"s3a://{_BUCKET}/silver/march_mania"
GOLD_PREFIX   = f"s3a://{_BUCKET}/gold/march_mania"

# ── Dossier d'entrée des CSV Kaggle ──────────────────────────────────────────
# Docker  : /opt/project/data/input  (monté via docker-compose.yml)
# Local   : <racine_projet>/data/input  (résolu depuis ce fichier)
_DOCKER_INPUT = "/opt/project/data/input"
_LOCAL_INPUT  = str(Path(__file__).resolve().parents[3] / "data" / "input")

LOCAL_INPUT_DIR: str = _DOCKER_INPUT if os.path.exists("/opt/project") else _LOCAL_INPUT


# ── Helpers de chemin ─────────────────────────────────────────────────────────

def bronze_path(dataset: str) -> str:
    """s3a://<bucket>/bronze/march_mania/<dataset>"""
    return f"{BRONZE_PREFIX}/{dataset}"


def silver_path(dataset: str) -> str:
    """s3a://<bucket>/silver/march_mania/<dataset>"""
    return f"{SILVER_PREFIX}/{dataset}"


def gold_path(dataset: str) -> str:
    """s3a://<bucket>/gold/march_mania/<dataset>"""
    return f"{GOLD_PREFIX}/{dataset}"


def bronze_misc_path(stem: str) -> str:
    """Chemin Bronze pour les CSV non référencés dans le registre."""
    return f"{BRONZE_PREFIX}/misc/{stem}"
