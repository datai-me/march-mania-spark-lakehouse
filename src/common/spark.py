"""
spark.py — Factory SparkSession (MinIO S3A + mode local)
══════════════════════════════════════════════════════════
Point d'entrée unique pour créer une SparkSession dans tous les jobs.

Mode Docker :
    - Se connecte à MinIO via S3A (variables MINIO_* depuis docker-compose)
    - Spark standalone cluster (master: spark://spark-master:7077)

Mode local :
    - Utilise local[*] comme master
    - Toujours configuré S3A si MINIO_ENDPOINT est défini (MinIO local possible)
    - Sinon, les chemins s3a:// ne fonctionneront pas — prévoir chemins fichier
      en mode 100% local sans MinIO

Configuration S3A requise pour MinIO :
    - path.style.access=true     : MinIO n'utilise pas de subdomain-style
    - ssl.enabled=false          : HTTP en local (HTTPS en prod)
    - SimpleAWSCredentialsProvider : credentials fixes (pas de IAM)
"""

import os

from pyspark.sql import SparkSession

from src.common.logging import get_logger
from src.common.runtime import runtime_summary

logger = get_logger(__name__)


def build_spark(app_name: str) -> SparkSession:
    """
    Crée et retourne une SparkSession configurée pour MinIO (S3A).

    Variables d'environnement lues :
        MINIO_ENDPOINT   : URL MinIO  (défaut : http://minio:9000)
        MINIO_ACCESS_KEY : clé d'accès (défaut : admin)
        MINIO_SECRET_KEY : clé secrète (défaut : admin123)
        TZ               : fuseau horaire Spark SQL (défaut : Indian/Antananarivo)

    Args:
        app_name: nom de l'application affiché dans la Spark UI.

    Returns:
        SparkSession configurée et prête à l'emploi.
    """
    summary = runtime_summary()
    logger.info("Démarrage SparkSession '%s' | runtime=%s | python=%s",
                app_name, summary["mode"], summary["python"])

    endpoint   = os.getenv("MINIO_ENDPOINT",   "http://minio:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "admin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "admin123")
    tz         = os.getenv("TZ", "Indian/Antananarivo")

    builder = (
        SparkSession.builder
        .appName(app_name)
        # Fuseau horaire uniforme pour toutes les fonctions date/timestamp
        .config("spark.sql.session.timeZone", tz)

        # ── S3A / MinIO ────────────────────────────────────────────────────────
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.endpoint",            endpoint)
        .config("spark.hadoop.fs.s3a.access.key",          access_key)
        .config("spark.hadoop.fs.s3a.secret.key",          secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access",   "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

        # ── Performance : AQE actif, partitions adaptées aux petits datasets ──
        # shuffle.partitions=32 : mieux que le défaut 200 pour des datasets ~100k-5M lignes
        # Augmenter à 200+ pour des datasets > 50M lignes
        .config("spark.sql.shuffle.partitions",  "32")
        .config("spark.sql.adaptive.enabled",    "true")

        # ── Stabilité : évite OOM sur la JVM driver pour les petits jobs ─────
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "2g"))
    )

    spark = builder.getOrCreate()

    logger.info("SparkSession prête | master=%s | endpoint=%s",
                spark.sparkContext.master, endpoint)
    return spark
