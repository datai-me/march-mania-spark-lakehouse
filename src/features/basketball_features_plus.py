"""
basketball_features_plus.py — Assembly complète des features matchup
═════════════════════════════════════════════════════════════════════
Combine les 4 sources Silver en une seule fonction :
    1. Statistiques saison (WinRate, AvgPointDiff)
    2. ELO de fin de saison
    3. Rolling momentum (derniers N matchs)
    → Deltas Team1 − Team2 pour chaque feature

Utilisé par les jobs 03 (Gold), 07 (backtest) et 12 (ensemble).
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _side_stats(team_stats: DataFrame, side: str) -> DataFrame:
    """Prépare les colonnes team_stats pour Team1 ou Team2."""
    alias = side  # "Team1" ou "Team2"
    return team_stats.select(
        "Season",
        F.col("TeamID").alias(alias),
        F.col("WinRate").alias(f"{side[0]}{side[1]}_WinRate"),        # T1_ ou T2_
        F.col("AvgPointDiff").alias(f"{side[0]}{side[1]}_AvgPointDiff"),
    )


def attach_team_features(
    matchups: DataFrame,
    team_stats: DataFrame,
    elo: DataFrame,
    rolling_last: DataFrame,
) -> DataFrame:
    """
    Enrichit les matchups (Team1 vs Team2) avec les features Silver et calcule
    les deltas (Team1 − Team2).

    Args:
        matchups     : DataFrame(Season, Team1, Team2, [label])
        team_stats   : DataFrame(Season, TeamID, WinRate, AvgPointDiff, ...)
        elo          : DataFrame(Season, TeamID, Elo)
        rolling_last : DataFrame(Season, TeamID, RollWinRate, RollAvgPointDiff, ...)
                       ← dernière valeur par saison (snapshot de fin de saison)

    Returns:
        matchups enrichi avec les colonnes de features et leurs deltas.

    Note sur le rolling :
        On joint sur (Season, TeamID) uniquement — pas sur DayNum.
        Raison : rolling_last contient déjà le snapshot de FIN DE SAISON,
        donc la jointure sur DayNum est inutile et causerait des lignes
        manquantes pour les matchups de soumission (qui n'ont pas de DayNum).
        L'anti-leakage est garanti par la construction de rolling_last dans le job 06
        (qui utilise les matchs de la saison régulière uniquement).
    """
    # ── Stats saison ──────────────────────────────────────────────────────────
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

    # ── ELO ───────────────────────────────────────────────────────────────────
    t1_elo = elo.select("Season", F.col("TeamID").alias("Team1"), F.col("Elo").alias("T1_Elo"))
    t2_elo = elo.select("Season", F.col("TeamID").alias("Team2"), F.col("Elo").alias("T2_Elo"))

    # ── Rolling momentum ──────────────────────────────────────────────────────
    # Jointure sur (Season, TeamID) uniquement — rolling_last = snapshot fin de saison.
    # Ne pas joindre sur DayNum : inutile ici et bloque les soumissions sans DayNum.
    t1_roll = rolling_last.select(
        "Season",
        F.col("TeamID").alias("Team1"),
        F.col("RollWinRate").alias("T1_RollWinRate"),
        F.col("RollAvgPointDiff").alias("T1_RollAvgPointDiff"),
    )
    t2_roll = rolling_last.select(
        "Season",
        F.col("TeamID").alias("Team2"),
        F.col("RollWinRate").alias("T2_RollWinRate"),
        F.col("RollAvgPointDiff").alias("T2_RollAvgPointDiff"),
    )

    # ── Jointures séquentielles (toutes en left pour préserver les matchups) ──
    out = (
        matchups
        .join(t1_stats, on=["Season", "Team1"], how="left")
        .join(t2_stats, on=["Season", "Team2"], how="left")
        .join(t1_elo,   on=["Season", "Team1"], how="left")
        .join(t2_elo,   on=["Season", "Team2"], how="left")
        .join(t1_roll,  on=["Season", "Team1"], how="left")
        .join(t2_roll,  on=["Season", "Team2"], how="left")
    )

    # ── Deltas : Team1 − Team2 ────────────────────────────────────────────────
    # Un delta positif signifie que Team1 est avantagé sur cette feature.
    return (
        out
        .withColumn("WinRateDiff",          F.col("T1_WinRate")          - F.col("T2_WinRate"))
        .withColumn("AvgPointDiffDiff",     F.col("T1_AvgPointDiff")     - F.col("T2_AvgPointDiff"))
        .withColumn("EloDiff",              F.col("T1_Elo")              - F.col("T2_Elo"))
        .withColumn("RollWinRateDiff",      F.col("T1_RollWinRate")      - F.col("T2_RollWinRate"))
        .withColumn("RollAvgPointDiffDiff", F.col("T1_RollAvgPointDiff") - F.col("T2_RollAvgPointDiff"))
    )
