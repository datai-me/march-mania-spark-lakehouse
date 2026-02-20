"""Reusable basketball feature engineering utilities for March Machine Learning Mania.

These feature functions are intentionally simple and explainable (good for exams / interviews).
You can extend them with:
- ELO per season
- rolling averages (window functions)
- strength of schedule
- team embeddings
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_team_season_stats(regular_season_results: DataFrame) -> DataFrame:
    """Compute basic team-season aggregates from compact regular season results.

    Expected columns (Kaggle typical):
    - Season
    - WTeamID, LTeamID
    - WScore, LScore

    Returns:
        DataFrame with per-team, per-season stats:
        - games, wins, losses, win_rate
        - avg_points_for, avg_points_against, avg_point_diff
    """
    wins = (
        regular_season_results
        .select(
            F.col("Season").cast("int").alias("Season"),
            F.col("WTeamID").cast("int").alias("TeamID"),
            F.col("WScore").cast("int").alias("PointsFor"),
            F.col("LScore").cast("int").alias("PointsAgainst"),
        )
        .withColumn("Win", F.lit(1))
        .withColumn("Loss", F.lit(0))
    )

    losses = (
        regular_season_results
        .select(
            F.col("Season").cast("int").alias("Season"),
            F.col("LTeamID").cast("int").alias("TeamID"),
            F.col("LScore").cast("int").alias("PointsFor"),
            F.col("WScore").cast("int").alias("PointsAgainst"),
        )
        .withColumn("Win", F.lit(0))
        .withColumn("Loss", F.lit(1))
    )

    all_games = wins.unionByName(losses)

    team_stats = (
        all_games
        .groupBy("Season", "TeamID")
        .agg(
            F.count("*").alias("Games"),
            F.sum("Win").alias("Wins"),
            F.sum("Loss").alias("Losses"),
            F.avg("PointsFor").alias("AvgPointsFor"),
            F.avg("PointsAgainst").alias("AvgPointsAgainst"),
            F.avg((F.col("PointsFor") - F.col("PointsAgainst"))).alias("AvgPointDiff"),
        )
        .withColumn("WinRate", F.col("Wins") / F.col("Games"))
    )

    return team_stats


def build_matchup_features(matchups: DataFrame, team_stats: DataFrame) -> DataFrame:
    """Join team-season stats for Team1 and Team2 and compute deltas.

    matchups expected columns:
    - Season, Team1, Team2
    Optionally:
    - label (1 if Team1 wins, 0 if Team1 loses) for training

    Returns:
        DataFrame with features:
        - WinRateDiff, AvgPointDiffDiff, etc.
    """
    t1 = team_stats.select(
        "Season",
        F.col("TeamID").alias("Team1"),
        F.col("WinRate").alias("T1_WinRate"),
        F.col("AvgPointDiff").alias("T1_AvgPointDiff"),
    )

    t2 = team_stats.select(
        "Season",
        F.col("TeamID").alias("Team2"),
        F.col("WinRate").alias("T2_WinRate"),
        F.col("AvgPointDiff").alias("T2_AvgPointDiff"),
    )

    out = (
        matchups
        .join(t1, on=["Season", "Team1"], how="left")
        .join(t2, on=["Season", "Team2"], how="left")
        .withColumn("WinRateDiff", F.col("T1_WinRate") - F.col("T2_WinRate"))
        .withColumn("AvgPointDiffDiff", F.col("T1_AvgPointDiff") - F.col("T2_AvgPointDiff"))
    )

    return out
