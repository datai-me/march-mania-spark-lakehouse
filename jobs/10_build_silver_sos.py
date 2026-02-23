"""
Job 10 — Silver SOS : force du calendrier (Strength of Schedule)
═════════════════════════════════════════════════════════════════
Calcule pour chaque équipe la difficulté moyenne de ses adversaires sur la saison :
    SOS_OppWinRate : taux de victoire moyen des adversaires
    SOS_OppElo     : ELO moyen des adversaires

Prérequis : jobs 02 (team_season_stats) et 05 (elo_ratings)

Sortie : silver/march_mania/<league>/strength_of_schedule/

Usage :
    docker compose run --rm spark-submit python jobs/10_build_silver_sos.py
"""

import yaml

from src.common.logging import get_logger
from src.common.paths import bronze_path, silver_path
from src.common.spark import build_spark
from src.features.rolling import build_long_game_table
from src.features.sos import build_sos

logger = get_logger(__name__)


def main() -> None:
    spark = build_spark("march-mania-10-silver-sos")
    cfg = yaml.safe_load(open("/opt/project/conf/pipeline.yml", encoding="utf-8"))

    league = str(cfg.get("competition", {}).get("league", "M")).upper()
    if league not in {"M", "W"}:
        raise ValueError(f"competition.league doit être 'M' ou 'W', reçu : {league!r}")

    bronze_ds = "m/regular_season/compact" if league == "M" else "w/regular_season/compact"
    regular    = spark.read.parquet(bronze_path(bronze_ds))
    team_stats = spark.read.parquet(silver_path(f"{league}/team_season_stats"))
    elo        = spark.read.parquet(silver_path(f"{league}/elo_ratings"))

    long_games = build_long_game_table(regular)
    sos = build_sos(long_games, team_stats, elo)

    out_path = silver_path(f"{league}/strength_of_schedule")
    sos.write.mode("overwrite").parquet(out_path)
    logger.info("SOS écrit — league=%s lignes=%d → %s", league, sos.count(), out_path)


if __name__ == "__main__":
    main()
