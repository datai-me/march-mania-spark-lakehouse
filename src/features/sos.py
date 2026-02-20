"""Strength of Schedule (SOS) features.

We compute, per Season & Team:
- average opponent WinRate
- average opponent ELO

This captures schedule difficulty.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_sos(long_games: DataFrame, team_stats: DataFrame, elo: DataFrame) -> DataFrame:
    """Compute SOS features.

    Args:
        long_games: DataFrame from build_long_game_table (Season, TeamID, OpponentID, ...)
        team_stats: DataFrame(Season, TeamID, WinRate, ...)
        elo: DataFrame(Season, TeamID, Elo)

    Returns:
        DataFrame(Season, TeamID, SOS_OppWinRate:double, SOS_OppElo:double)
    """
    opp_wr = team_stats.select(
        "Season",
        F.col("TeamID").alias("OpponentID"),
        F.col("WinRate").alias("OppWinRate"),
    )
    opp_elo = elo.select(
        "Season",
        F.col("TeamID").alias("OpponentID"),
        F.col("Elo").alias("OppElo"),
    )

    enriched = (long_games
                .join(opp_wr, on=["Season", "OpponentID"], how="left")
                .join(opp_elo, on=["Season", "OpponentID"], how="left"))

    out = (enriched
           .groupBy("Season", "TeamID")
           .agg(
               F.avg("OppWinRate").alias("SOS_OppWinRate"),
               F.avg("OppElo").alias("SOS_OppElo"),
           ))
    return out
