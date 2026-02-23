"""
Job 02 — Silver : statistiques équipe-saison
═════════════════════════════════════════════
Calcule les agrégats saison par équipe (taux de victoire, écart de points moyen…)
à partir des résultats réguliers Bronze.

Config : conf/pipeline.yml → competition.league (M ou W)

Sortie : silver/march_mania/<league>/team_season_stats/

Usage :
    docker compose run --rm spark-submit python jobs/02_build_silver_features.py
"""

import yaml

from src.common.logging import get_logger
from src.common.paths import bronze_path, silver_path
from src.common.spark import build_spark
from src.features.basketball_features import build_team_season_stats

logger = get_logger(__name__)


def _load_league(cfg_path: str = "/opt/project/conf/pipeline.yml") -> str:
    cfg = yaml.safe_load(open(cfg_path, encoding="utf-8"))
    league = str(cfg.get("competition", {}).get("league", "M")).upper()
    if league not in {"M", "W"}:
        raise ValueError(f"competition.league doit être 'M' ou 'W', reçu : {league!r}")
    return league


def main() -> None:
    spark = build_spark("march-mania-02-silver")
    league = _load_league()

    bronze_ds = "m/regular_season/compact" if league == "M" else "w/regular_season/compact"
    regular = spark.read.parquet(bronze_path(bronze_ds))

    team_stats = build_team_season_stats(regular)

    out_path = silver_path(f"{league}/team_season_stats")
    logger.info("Écriture Silver team-season stats : %s", out_path)
    team_stats.write.mode("overwrite").parquet(out_path)

    logger.info("Silver features terminé (league=%s)", league)


if __name__ == "__main__":
    main()
