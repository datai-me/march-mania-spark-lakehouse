"""
Job 08 — Silver Seeds : numéros de tête de série
══════════════════════════════════════════════════
Parse les seeds de tournoi (ex : "W01" → région W, seed 1).
Le numéro de seed est un signal fort pour prédire les matchups de tournoi.

Sortie : silver/march_mania/<league>/tourney_seeds_parsed/

Usage :
    docker compose run --rm spark-submit python jobs/08_build_silver_seeds.py
"""

import yaml

from src.common.logging import get_logger
from src.common.paths import bronze_path, silver_path
from src.common.spark import build_spark
from src.features.seeds import build_seeds

logger = get_logger(__name__)


def main() -> None:
    spark = build_spark("march-mania-08-silver-seeds")
    cfg = yaml.safe_load(open("/opt/project/conf/pipeline.yml", encoding="utf-8"))

    league = str(cfg.get("competition", {}).get("league", "M")).upper()
    if league not in {"M", "W"}:
        raise ValueError(f"competition.league doit être 'M' ou 'W', reçu : {league!r}")

    bronze_ds = "m/tournament/seeds" if league == "M" else "w/tournament/seeds"
    seeds = spark.read.parquet(bronze_path(bronze_ds))
    parsed = build_seeds(seeds)

    out_path = silver_path(f"{league}/tourney_seeds_parsed")
    parsed.write.mode("overwrite").parquet(out_path)
    logger.info("Seeds parsés — league=%s lignes=%d → %s", league, parsed.count(), out_path)


if __name__ == "__main__":
    main()
