"""
Job 01 — Ingestion Bronze : CSV Kaggle → Parquet MinIO
═══════════════════════════════════════════════════════
Lit tous les fichiers *.csv dans data/input/, les convertit en Parquet
et les stocke dans la couche Bronze du lakehouse (MinIO/S3).

Comportement :
- Les fichiers connus sont rangés selon le registre datasets.spec_for_filename()
- Les fichiers inconnus vont dans bronze/misc/<nom_fichier>/  (pas de crash)
- Les colonnes texte sont nettoyées (trim) pour éviter les espaces parasites

Prérequis : MinIO accessible et bucket créé (via minio-mc au démarrage).

Usage :
    docker compose run --rm spark-submit python jobs/01_ingest_bronze.py
"""

from pathlib import Path

from pyspark.sql import functions as F

from src.common.datasets import spec_for_filename
from src.common.logging import get_logger
from src.common.paths import LOCAL_INPUT_DIR, bronze_misc_path, bronze_path
from src.common.spark import build_spark

logger = get_logger(__name__)


def _safe_stem(name: str) -> str:
    """Transforme un nom de fichier CSV en identifiant snake_case sûr."""
    return name.replace(".csv", "").replace(" ", "_").lower()


def main() -> None:
    spark = build_spark("march-mania-01-bronze")

    csv_files = sorted(Path(LOCAL_INPUT_DIR).glob("*.csv"))
    if not csv_files:
        logger.warning("Aucun CSV trouvé dans %s — placez les fichiers Kaggle dans data/input/", LOCAL_INPUT_DIR)
        return

    for file_path in csv_files:
        spec = spec_for_filename(file_path.name)

        # Lecture avec inférence de schéma (Bronze = données brutes)
        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(str(file_path))
        )

        # Trim des colonnes string pour nettoyer les espaces en début/fin
        for col_name, col_type in df.dtypes:
            if col_type == "string":
                df = df.withColumn(col_name, F.trim(F.col(col_name)))

        # Destination : chemin connu ou misc/ pour les fichiers non référencés
        out_path = bronze_path(spec.lake_subpath) if spec else bronze_misc_path(_safe_stem(file_path.name))
        if not spec:
            logger.warning("CSV inconnu '%s' → stocké dans %s", file_path.name, out_path)

        logger.info("Bronze : %s → %s", file_path.name, out_path)
        df.write.mode("overwrite").parquet(out_path)

    logger.info("Ingestion Bronze terminée — %d fichiers traités", len(csv_files))


if __name__ == "__main__":
    main()
