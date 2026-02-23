"""
rolling.py — Features de momentum (rolling window)
════════════════════════════════════════════════════
Calcule des statistiques glissantes causales sur les N derniers matchs.

"Causal" signifie qu'on n'utilise que les matchs PASSÉS pour calculer
les features du match courant — garantie anti-leakage.

Deux fonctions :
    build_long_game_table    : transforme le format compact (1 ligne/match)
                               en format long (1 ligne par équipe × match)
    build_rolling_features   : calcule les fenêtres glissantes sur le format long
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_long_game_table(regular_season_results: DataFrame) -> DataFrame:
    """
    Transforme les résultats compacts en table longue : une ligne par équipe × match.

    Entrée (compact) : 1 ligne = 1 match avec vainqueur et perdant
    Sortie (long)    : 2 lignes = 1 pour le vainqueur, 1 pour le perdant

    Colonnes de sortie :
        Season, DayNum, TeamID, OpponentID,
        PointsFor, PointsAgainst, Win (1/0), PointDiff
    """
    # Vue vainqueurs
    winners = regular_season_results.select(
        F.col("Season").cast("int"),
        F.col("DayNum").cast("int"),
        F.col("WTeamID").cast("int").alias("TeamID"),
        F.col("LTeamID").cast("int").alias("OpponentID"),
        F.col("WScore").cast("int").alias("PointsFor"),
        F.col("LScore").cast("int").alias("PointsAgainst"),
        F.lit(1).alias("Win"),
    )
    # Vue perdants
    losers = regular_season_results.select(
        F.col("Season").cast("int"),
        F.col("DayNum").cast("int"),
        F.col("LTeamID").cast("int").alias("TeamID"),
        F.col("WTeamID").cast("int").alias("OpponentID"),
        F.col("LScore").cast("int").alias("PointsFor"),
        F.col("WScore").cast("int").alias("PointsAgainst"),
        F.lit(0).alias("Win"),
    )

    return (
        winners.unionByName(losers)
        .withColumn("PointDiff", F.col("PointsFor") - F.col("PointsAgainst"))
    )


def build_rolling_features(long_games: DataFrame, last_n: int = 10) -> DataFrame:
    """
    Calcule des features glissantes causales sur les N derniers matchs par équipe.

    Fenêtre : rowsBetween(-last_n, -1)
        → exclut le match courant (causal)
        → inclut les last_n matchs précédents dans la même saison

    Features calculées (par équipe, par match) :
        RollWinRate          : taux de victoire sur les N derniers matchs
        RollAvgPointDiff     : écart de points moyen sur les N derniers matchs
        RollAvgPointsFor     : points marqués en moyenne
        RollAvgPointsAgainst : points encaissés en moyenne

    Args:
        long_games : sortie de build_long_game_table
        last_n     : taille de la fenêtre glissante (défaut 10)

    Returns:
        long_games enrichi des colonnes Roll*
    """
    # Fenêtre par saison × équipe, triée chronologiquement, excluant le match courant
    window = (
        Window
        .partitionBy("Season", "TeamID")
        .orderBy("DayNum")
        .rowsBetween(-last_n, -1)
    )

    return (
        long_games
        .withColumn("RollWinRate",          F.avg("Win").over(window))
        .withColumn("RollAvgPointDiff",     F.avg("PointDiff").over(window))
        .withColumn("RollAvgPointsFor",     F.avg("PointsFor").over(window))
        .withColumn("RollAvgPointsAgainst", F.avg("PointsAgainst").over(window))
    )
