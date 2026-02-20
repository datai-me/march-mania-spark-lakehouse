"""Job 06 — Build rolling momentum features (Silver).

We create:
1) per-team-per-game rolling features (causal)
2) per-team-per-season last-available rolling features (for submissions without DayNum)

Inputs:
- Bronze regular season compact results

Outputs:
- s3a://<bucket>/silver/march_mania/rolling_per_game/
- s3a://<bucket>/silver/march_mania/rolling_last_per_season/

Run:
    docker compose run --rm spark-submit python jobs/06_build_silver_rolling.py
"""

import yaml
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.common.spark import build_spark
from src.common.paths import bronze_path, silver_path
from src.common.logging import get_logger
from src.features.rolling import build_long_game_table, build_rolling_features

logger = get_logger(__name__)


def main() -> None:
    spark = build_spark("march-mania-06-silver-rolling")

    cfg = yaml.safe_load(open("/opt/project/conf/pipeline.yml", "r", encoding="utf-8"))
    last_n = int(cfg.get("rolling", {}).get("window_last_n_games", 10))

    regular = spark.read.parquet(bronze_path("regular_season_compact_results"))

    long_games = build_long_game_table(regular)
    roll = build_rolling_features(long_games, last_n=last_n)

    out1 = silver_path("rolling_per_game")
    logger.info("Writing Silver rolling per game: %s", out1)
    roll.write.mode("overwrite").parquet(out1)

    # Build last rolling per team-season (latest DayNum row)
    w = Window.partitionBy("Season", "TeamID").orderBy(F.col("DayNum").desc())
    last = (roll
            .withColumn("rn", F.row_number().over(w))
            .filter(F.col("rn") == 1)
            .drop("rn", "OpponentID", "PointsFor", "PointsAgainst", "Win", "PointDiff"))

    out2 = silver_path("rolling_last_per_season")
    logger.info("Writing Silver rolling last per season: %s", out2)
    last.write.mode("overwrite").parquet(out2)

    logger.info("Silver rolling complete. per_game=%d, last=%d", roll.count(), last.count())


if __name__ == "__main__":
    main()
