"""
sos.py — Force du calendrier (Strength of Schedule)
═════════════════════════════════════════════════════
Mesure la difficulté du calendrier d'une équipe sur la saison :
    SOS_OppWinRate : taux de victoire moyen des adversaires
    SOS_OppElo     : rating ELO moyen des adversaires

Interprétation : un SOS élevé signifie un calendrier difficile.
Le delta SOS entre deux équipes peut améliorer la prédiction des matchups de tournoi
où les équipes de conférences faibles sont sous-évaluées par d'autres modèles.

Prérequis : team_stats (job 02) et elo (job 05) doivent être calculés avant ce job.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_sos(
    long_games: DataFrame,
    team_stats: DataFrame,
    elo: DataFrame,
) -> DataFrame:
    """
    Calcule les features SOS pour chaque équipe × saison.

    Args:
        long_games   : sortie de build_long_game_table (Season, TeamID, OpponentID, ...)
        team_stats   : DataFrame(Season, TeamID, WinRate, ...)
        elo          : DataFrame(Season, TeamID, Elo)

    Returns:
        DataFrame(Season, TeamID, SOS_OppWinRate:double, SOS_OppElo:double)

    Note : on joint les stats de l'ADVERSAIRE (OpponentID), pas de l'équipe elle-même.
    """
    # Stats des adversaires — renommage pour jointure sur OpponentID
    opp_winrate = team_stats.select(
        "Season",
        F.col("TeamID").alias("OpponentID"),
        F.col("WinRate").alias("OppWinRate"),
    )
    opp_elo = elo.select(
        "Season",
        F.col("TeamID").alias("OpponentID"),
        F.col("Elo").alias("OppElo"),
    )

    # Jointure sur les adversaires, puis agrégation par équipe
    return (
        long_games
        .select("Season", "TeamID", "OpponentID")   # colonnes minimales nécessaires
        .join(opp_winrate, on=["Season", "OpponentID"], how="left")
        .join(opp_elo,     on=["Season", "OpponentID"], how="left")
        .groupBy("Season", "TeamID")
        .agg(
            F.avg("OppWinRate").alias("SOS_OppWinRate"),
            F.avg("OppElo").alias("SOS_OppElo"),
        )
    )
