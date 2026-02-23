"""
Job 06 — Silver Rolling : momentum des derniers matchs
════════════════════════════════════════════════════════
Calcule des features glissantes causales sur les N derniers matchs de chaque équipe
(taux de victoire, écart de points moyen). "Causal" = on n'utilise que les matchs
passés, pas ceux à venir.

Deux sorties :
    rolling_per_game        : une ligne par équipe × match (historique complet)
    rolling_last_per_season : dernière valeur de la saison par équipe (utilisée en Gold)

Config : conf/pipeline.yml → rolling.window_last_n_games (défaut : 10)

Usage :
    docker compose run --rm spark-submit python jobs/06_build_silver_rolling.py
"""

import yaml
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.common.logging import get_logger
from src.common.paths import bronze_path, silver_path
from src.common.spark import build_spark
from src.features.rolling import build_long_game_table, build_rolling_features

logger = get_logger(__name__)


def main() -> None:
    spark = build_spark("march-mania-06-silver-rolling")
    cfg = yaml.safe_load(open("/opt/project/conf/pipeline.yml", encoding="utf-8"))

    league = str(cfg.get("competition", {}).get("league", "M")).upper()
    if league not in {"M", "W"}:
        raise ValueError(f"competition.league doit être 'M' ou 'W', reçu : {league!r}")

    last_n = int(cfg.get("rolling", {}).get("window_last_n_games", 10))
    bronze_ds = "m/regular_season/compact" if league == "M" else "w/regular_season/compact"

    regular = spark.read.parquet(bronze_path(bronze_ds))
    long_games = build_long_game_table(regular)          # format long : 1 ligne par équipe × match
    roll = build_rolling_features(long_games, last_n=last_n)

    # Écriture des features par match
    out1 = silver_path(f"{league}/rolling_per_game")
    roll.write.mode("overwrite").parquet(out1)
    logger.info("Rolling par match écrit : %s", out1)

    # Dernière observation de la saison par équipe (snapshot de fin de saison)
    w = Window.partitionBy("Season", "TeamID").orderBy(F.col("DayNum").desc())
    cols_to_drop = ["rn", "OpponentID", "PointsFor", "PointsAgainst", "Win", "PointDiff"]
    last = (
        roll.withColumn("rn", F.row_number().over(w))
            .filter(F.col("rn") == 1)
            .drop(*cols_to_drop)
    )

    out2 = silver_path(f"{league}/rolling_last_per_season")
    last.write.mode("overwrite").parquet(out2)
    logger.info("Rolling dernière saison écrit : %s", out2)

    logger.info("Rolling terminé — league=%s per_game=%d last=%d", league, roll.count(), last.count())


if __name__ == "__main__":
    main()
