"""
Job 09 — Silver Massey : consensus des rankings (Hommes uniquement)
═════════════════════════════════════════════════════════════════════
Agrège les ordinals Massey (plusieurs systèmes de ranking) en un consensus
par saison et par équipe (moyenne des rangs). Disponible uniquement pour la
compétition masculine (MMasseyOrdinals.csv).

Sortie : silver/march_mania/M/massey_consensus/

Usage :
    docker compose run --rm spark-submit python jobs/09_build_silver_massey.py
"""

from src.common.logging import get_logger
from src.common.paths import bronze_path, silver_path
from src.common.spark import build_spark
from src.features.massey import build_massey_consensus

logger = get_logger(__name__)


def main() -> None:
    spark = build_spark("march-mania-09-silver-massey")

    try:
        massey_raw = spark.read.parquet(bronze_path("m/rankings/massey_ordinals"))
    except Exception as e:
        # Non-bloquant : le job suivant utilisera MasseyDiff = null
        logger.warning("MMasseyOrdinals.csv absent du Bronze — job ignoré. (%s)", e)
        return

    consensus = build_massey_consensus(massey_raw)

    out_path = silver_path("M/massey_consensus")
    consensus.write.mode("overwrite").parquet(out_path)
    logger.info("Massey consensus écrit — lignes=%d → %s", consensus.count(), out_path)


if __name__ == "__main__":
    main()
