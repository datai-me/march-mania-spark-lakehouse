"""Job 03 — Build Gold training dataset (matchups with features).

Gold is a curated, analysis-ready dataset tailored to a specific product use case.
Here: supervised learning where label = Team1 win?

This script creates matchups from historical regular season results:
- Team1 = WTeamID, Team2 = LTeamID, label = 1
- plus the swapped rows: Team1 = LTeamID, Team2 = WTeamID, label = 0

Then it joins Silver team-season stats and computes feature deltas.

Outputs:
- s3a://<bucket>/gold/march_mania/training_matchups/

Run:
    docker compose run --rm spark-submit python jobs/03_build_gold_training_set.py
"""

from pyspark.sql import functions as F

from src.common.spark import build_spark
from src.common.paths import bronze_path, silver_path, gold_path
from src.common.logging import get_logger
from src.features.basketball_features import build_matchup_features

logger = get_logger(__name__)


def main() -> None:
    spark = build_spark("march-mania-03-gold")

    regular = spark.read.parquet(bronze_path("regular_season_compact_results"))
    team_stats = spark.read.parquet(silver_path("team_season_stats"))

    # Create labeled matchups (Team1 vs Team2)
    w = (
        regular
        .select(
            F.col("Season").cast("int").alias("Season"),
            F.col("WTeamID").cast("int").alias("Team1"),
            F.col("LTeamID").cast("int").alias("Team2"),
        )
        .withColumn("label", F.lit(1))
    )

    l = (
        regular
        .select(
            F.col("Season").cast("int").alias("Season"),
            F.col("LTeamID").cast("int").alias("Team1"),
            F.col("WTeamID").cast("int").alias("Team2"),
        )
        .withColumn("label", F.lit(0))
    )

    matchups = w.unionByName(l)

    # Join season-team aggregates and compute deltas
    features = build_matchup_features(matchups, team_stats)

    # Drop rows with missing stats (can happen if a team has no regular season rows)
    features = features.dropna(subset=["WinRateDiff", "AvgPointDiffDiff"])

    out_path = gold_path("training_matchups")
    logger.info("Writing Gold training set: %s", out_path)
    features.write.mode("overwrite").parquet(out_path)

    logger.info("Gold dataset complete. Row count: %s", features.count())


if __name__ == "__main__":
    main()
