"""
Job 05 — Silver ELO : ratings par saison
══════════════════════════════════════════
Calcule des ratings ELO réinitialisés à chaque saison (pas de continuité inter-saisons).
L'ELO est un bon prédicteur car il capture la force relative des équipes de façon dynamique.

Paramètres (conf/pipeline.yml → elo) :
    initial_rating : 1500.0 (rating de départ de chaque équipe)
    k_factor       : 20.0   (amplitude des ajustements)

Sortie : silver/march_mania/<league>/elo_ratings/

Usage :
    docker compose run --rm spark-submit python jobs/05_build_silver_elo.py
"""

import yaml

from src.common.logging import get_logger
from src.common.paths import bronze_path, silver_path
from src.common.spark import build_spark
from src.features.elo import build_elo_per_season

logger = get_logger(__name__)


def _load_config(cfg_path: str = "/opt/project/conf/pipeline.yml") -> dict:
    return yaml.safe_load(open(cfg_path, encoding="utf-8"))


def main() -> None:
    spark = build_spark("march-mania-05-silver-elo")
    cfg = _load_config()

    league = str(cfg.get("competition", {}).get("league", "M")).upper()
    if league not in {"M", "W"}:
        raise ValueError(f"competition.league doit être 'M' ou 'W', reçu : {league!r}")

    bronze_ds = "m/regular_season/compact" if league == "M" else "w/regular_season/compact"
    regular = spark.read.parquet(bronze_path(bronze_ds))

    elo_cfg = cfg.get("elo", {})
    elo = build_elo_per_season(
        regular,
        initial_rating=float(elo_cfg.get("initial_rating", 1500.0)),
        k_factor=float(elo_cfg.get("k_factor", 20.0)),
    )

    out_path = silver_path(f"{league}/elo_ratings")
    logger.info("Écriture Silver ELO : %s", out_path)
    elo.write.mode("overwrite").parquet(out_path)
    logger.info("ELO terminé — league=%s lignes=%d", league, elo.count())


if __name__ == "__main__":
    main()
