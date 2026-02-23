"""
basketball_features.py — Agrégats équipe-saison (baseline)
════════════════════════════════════════════════════════════
Fonctions de feature engineering simples et explicables :
    build_team_season_stats  : agrégats saison par équipe (WinRate, PointDiff…)
    build_matchup_features   : deltas Team1 − Team2 pour le modèle baseline

Ces fonctions couvrent le job 02 et le job 04 (baseline).
Pour les features avancées (ELO, rolling, seeds…), voir basketball_features_plus.py.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Colonnes requises en entrée (compact results Kaggle)
_REQUIRED_COLS = {"Season", "WTeamID", "LTeamID", "WScore", "LScore"}


def _assert_cols(df: DataFrame, required: set[str], caller: str) -> None:
    """Lève ValueError si des colonnes requises sont absentes — fail fast."""
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"[{caller}] Colonnes manquantes : {sorted(missing)}")


def build_team_season_stats(regular_season_results: DataFrame) -> DataFrame:
    """
    Calcule les statistiques agrégées par équipe et par saison.

    Entrée (compact results Kaggle) :
        Season, WTeamID, LTeamID, WScore, LScore

    Sortie :
        Season, TeamID, Games, Wins, Losses, WinRate,
        AvgPointsFor, AvgPointsAgainst, AvgPointDiff

    Stratégie : on crée deux vues (vainqueurs + perdants) puis on union
    pour traiter chaque équipe symétriquement en une seule agrégation.
    """
    _assert_cols(regular_season_results, _REQUIRED_COLS, "build_team_season_stats")

    # Vue vainqueurs : Win=1
    winners = (
        regular_season_results.select(
            F.col("Season").cast("int"),
            F.col("WTeamID").cast("int").alias("TeamID"),
            F.col("WScore").cast("int").alias("PointsFor"),
            F.col("LScore").cast("int").alias("PointsAgainst"),
            F.lit(1).alias("Win"),
        )
    )
    # Vue perdants : Win=0
    losers = (
        regular_season_results.select(
            F.col("Season").cast("int"),
            F.col("LTeamID").cast("int").alias("TeamID"),
            F.col("LScore").cast("int").alias("PointsFor"),
            F.col("WScore").cast("int").alias("PointsAgainst"),
            F.lit(0).alias("Win"),
        )
    )

    return (
        winners.unionByName(losers)
        .groupBy("Season", "TeamID")
        .agg(
            F.count("*").alias("Games"),
            F.sum("Win").alias("Wins"),
            (F.count("*") - F.sum("Win")).alias("Losses"),
            F.avg("PointsFor").alias("AvgPointsFor"),
            F.avg("PointsAgainst").alias("AvgPointsAgainst"),
            F.avg(F.col("PointsFor") - F.col("PointsAgainst")).alias("AvgPointDiff"),
        )
        # WinRate calculé après agg pour éviter de compter deux fois
        .withColumn("WinRate", F.col("Wins") / F.col("Games"))
    )


def build_matchup_features(matchups: DataFrame, team_stats: DataFrame) -> DataFrame:
    """
    Jointure team_stats pour Team1 et Team2, puis calcul des deltas.

    Utilisé par le job 04 (baseline) où seules les stats saison de base
    sont disponibles (sans ELO ni rolling).

    Entrée matchups : Season, Team1, Team2, [label]
    Entrée team_stats : Season, TeamID, WinRate, AvgPointDiff, ...

    Sortie : matchups enrichi avec WinRateDiff, AvgPointDiffDiff
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

    return (
        matchups
        .join(t1, on=["Season", "Team1"], how="left")
        .join(t2, on=["Season", "Team2"], how="left")
        .withColumn("WinRateDiff",      F.col("T1_WinRate")     - F.col("T2_WinRate"))
        .withColumn("AvgPointDiffDiff", F.col("T1_AvgPointDiff") - F.col("T2_AvgPointDiff"))
    )
