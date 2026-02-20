"""Rolling window features (momentum) built with Spark window functions."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_long_game_table(regular_season_results: DataFrame) -> DataFrame:
    """Transform compact results into a long table: one row per team per game.

    Output columns:
    - Season, DayNum, TeamID, OpponentID
    - PointsFor, PointsAgainst, Win (1/0), PointDiff
    """
    w = regular_season_results.select(
        F.col("Season").cast("int").alias("Season"),
        F.col("DayNum").cast("int").alias("DayNum"),
        F.col("WTeamID").cast("int").alias("TeamID"),
        F.col("LTeamID").cast("int").alias("OpponentID"),
        F.col("WScore").cast("int").alias("PointsFor"),
        F.col("LScore").cast("int").alias("PointsAgainst"),
    ).withColumn("Win", F.lit(1))

    l = regular_season_results.select(
        F.col("Season").cast("int").alias("Season"),
        F.col("DayNum").cast("int").alias("DayNum"),
        F.col("LTeamID").cast("int").alias("TeamID"),
        F.col("WTeamID").cast("int").alias("OpponentID"),
        F.col("LScore").cast("int").alias("PointsFor"),
        F.col("WScore").cast("int").alias("PointsAgainst"),
    ).withColumn("Win", F.lit(0))

    df = w.unionByName(l).withColumn("PointDiff", F.col("PointsFor") - F.col("PointsAgainst"))
    return df


def build_rolling_features(long_games: DataFrame, last_n: int = 10) -> DataFrame:
    """Compute rolling features per team per season using last N games.

    Features are *causal* because we use rowsBetween(-N, -1) (exclude current game).
    """
    win = Window.partitionBy("Season", "TeamID").orderBy("DayNum").rowsBetween(-last_n, -1)

    out = (long_games
           .withColumn("RollWinRate", F.avg("Win").over(win))
           .withColumn("RollAvgPointDiff", F.avg("PointDiff").over(win))
           .withColumn("RollAvgPointsFor", F.avg("PointsFor").over(win))
           .withColumn("RollAvgPointsAgainst", F.avg("PointsAgainst").over(win)))

    return out
