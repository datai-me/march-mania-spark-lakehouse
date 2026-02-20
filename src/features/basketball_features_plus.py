"""Feature assembly for matchups (Team1 vs Team2) including:
- season aggregates (WinRate, AvgPointDiff)
- ELO ratings
- rolling momentum features (last N games)

This keeps the Gold builder concise.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def attach_team_features(
    matchups: DataFrame,
    team_stats: DataFrame,
    elo: DataFrame,
    rolling_last: DataFrame,
) -> DataFrame:
    """Attach features for Team1 and Team2 and compute deltas.

    Args:
        matchups: DataFrame(Season, Team1, Team2, [label], [DayNum])
        team_stats: DataFrame(Season, TeamID, WinRate, AvgPointDiff, ...)
        elo: DataFrame(Season, TeamID, Elo)
        rolling_last: DataFrame(Season, TeamID, DayNum, RollWinRate, RollAvgPointDiff, ...)

    Returns:
        Matchup DataFrame with feature columns and diffs.
    """
    # Basic aggregates
    t1_stats = team_stats.select(
        "Season",
        F.col("TeamID").alias("Team1"),
        F.col("WinRate").alias("T1_WinRate"),
        F.col("AvgPointDiff").alias("T1_AvgPointDiff"),
    )
    t2_stats = team_stats.select(
        "Season",
        F.col("TeamID").alias("Team2"),
        F.col("WinRate").alias("T2_WinRate"),
        F.col("AvgPointDiff").alias("T2_AvgPointDiff"),
    )

    # ELO
    t1_elo = elo.select("Season", F.col("TeamID").alias("Team1"), F.col("Elo").alias("T1_Elo"))
    t2_elo = elo.select("Season", F.col("TeamID").alias("Team2"), F.col("Elo").alias("T2_Elo"))

    # Rolling: if matchups contains DayNum, we can join on exact DayNum.
    # For submission templates without DayNum, we will join on Season+Team only later (fallback in job 04/07).
    t1_roll = rolling_last.select(
        "Season",
        F.col("TeamID").alias("Team1"),
        "DayNum",
        F.col("RollWinRate").alias("T1_RollWinRate"),
        F.col("RollAvgPointDiff").alias("T1_RollAvgPointDiff"),
    )
    t2_roll = rolling_last.select(
        "Season",
        F.col("TeamID").alias("Team2"),
        "DayNum",
        F.col("RollWinRate").alias("T2_RollWinRate"),
        F.col("RollAvgPointDiff").alias("T2_RollAvgPointDiff"),
    )

    out = (matchups
           .join(t1_stats, on=["Season", "Team1"], how="left")
           .join(t2_stats, on=["Season", "Team2"], how="left")
           .join(t1_elo, on=["Season", "Team1"], how="left")
           .join(t2_elo, on=["Season", "Team2"], how="left"))

    if "DayNum" in matchups.columns:
        out = (out
               .join(t1_roll, on=["Season", "Team1", "DayNum"], how="left")
               .join(t2_roll, on=["Season", "Team2", "DayNum"], how="left"))
    else:
        # Keep rolling cols nullable (filled later if desired)
        out = (out
               .withColumn("T1_RollWinRate", F.lit(None).cast("double"))
               .withColumn("T2_RollWinRate", F.lit(None).cast("double"))
               .withColumn("T1_RollAvgPointDiff", F.lit(None).cast("double"))
               .withColumn("T2_RollAvgPointDiff", F.lit(None).cast("double")))

    # Deltas
    out = (out
           .withColumn("WinRateDiff", F.col("T1_WinRate") - F.col("T2_WinRate"))
           .withColumn("AvgPointDiffDiff", F.col("T1_AvgPointDiff") - F.col("T2_AvgPointDiff"))
           .withColumn("EloDiff", F.col("T1_Elo") - F.col("T2_Elo"))
           .withColumn("RollWinRateDiff", F.col("T1_RollWinRate") - F.col("T2_RollWinRate"))
           .withColumn("RollAvgPointDiffDiff", F.col("T1_RollAvgPointDiff") - F.col("T2_RollAvgPointDiff"))
           )

    return out
