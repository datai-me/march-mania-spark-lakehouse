"""
elo.py — Ratings ELO par saison (Pandas UDF)
═════════════════════════════════════════════
Calcule un rating ELO réinitialisé à chaque saison, à partir des résultats
de saison régulière triés par DayNum (ordre chronologique garanti).

Pourquoi ELO ?
    - Capture la force relative de chaque équipe en un seul scalaire.
    - Simple, explicable, fort signal pour LogLoss.
    - Robuste face aux équipes rares (rating initial = fallback naturel).

Implémentation :
    - Pandas UDF groupée par saison → scalable sans collecte sur le driver.
    - Tri par DayNum dans le UDF → anti-leakage garanti (pas de futur utilisé).
    - Résultat : un rating final par équipe à la FIN de la saison régulière.
"""

from __future__ import annotations

from typing import Iterator, Tuple

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StructField, StructType

# Colonnes requises en entrée
_REQUIRED = {"Season", "DayNum", "WTeamID", "LTeamID"}


# ── Formules ELO ──────────────────────────────────────────────────────────────

def _expected_score(ra: float, rb: float) -> float:
    """Probabilité de victoire de A contre B selon la formule ELO standard."""
    return 1.0 / (1.0 + 10.0 ** ((rb - ra) / 400.0))


def _update_ratings(ra: float, rb: float, winner_score: float, k: float) -> Tuple[float, float]:
    """
    Met à jour les ratings après un match.

    Args:
        ra, rb          : ratings actuels de A (vainqueur) et B (perdant)
        winner_score    : 1.0 si A gagne, 0.0 si B gagne
        k               : facteur d'ajustement (sensibilité)

    Returns:
        (nouveau_ra, nouveau_rb)
    """
    ea = _expected_score(ra, rb)
    new_ra = ra + k * (winner_score - ea)
    new_rb = rb + k * ((1.0 - winner_score) - (1.0 - ea))
    return new_ra, new_rb


# ── Fonction principale ────────────────────────────────────────────────────────

def build_elo_per_season(
    regular_season_results: DataFrame,
    initial_rating: float = 1500.0,
    k_factor: float = 20.0,
) -> DataFrame:
    """
    Calcule le rating ELO final de chaque équipe à la fin de la saison régulière.

    Args:
        regular_season_results : DataFrame Spark (Season, DayNum, WTeamID, LTeamID, ...)
        initial_rating         : rating de départ de chaque équipe (défaut 1500)
        k_factor               : amplitude des ajustements par match (défaut 20)

    Returns:
        DataFrame(Season:int, TeamID:int, Elo:double)
    """
    missing = _REQUIRED - set(regular_season_results.columns)
    if missing:
        raise ValueError(f"[build_elo_per_season] Colonnes manquantes : {sorted(missing)}")

    # Sélection minimale — limite le shuffle entre workers
    games = regular_season_results.select(
        F.col("Season").cast("int"),
        F.col("DayNum").cast("int"),
        F.col("WTeamID").cast("int"),
        F.col("LTeamID").cast("int"),
    )

    # Schéma de sortie déclaré pour le Pandas UDF
    schema = StructType([
        StructField("Season", IntegerType(), nullable=False),
        StructField("TeamID", IntegerType(), nullable=False),
        StructField("Elo",    DoubleType(),  nullable=False),
    ])

    def _compute_season_elo(pdf_iter: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        """
        UDF Pandas appliquée par saison.
        Les matchs sont triés chronologiquement avant traitement.
        """
        for pdf in pdf_iter:
            if pdf.empty:
                continue

            season = int(pdf["Season"].iloc[0])
            # Tri stablement pour résultats reproductibles quand DayNum est égal
            pdf = pdf.sort_values("DayNum", kind="mergesort")

            ratings: dict[int, float] = {}

            for _, row in pdf.iterrows():
                winner = int(row["WTeamID"])
                loser  = int(row["LTeamID"])
                rw = ratings.get(winner, initial_rating)
                rl = ratings.get(loser,  initial_rating)
                ratings[winner], ratings[loser] = _update_ratings(rw, rl, 1.0, k_factor)

            yield pd.DataFrame({
                "Season": season,
                "TeamID": list(ratings.keys()),
                "Elo":    list(ratings.values()),
            })

    return games.groupBy("Season").applyInPandas(_compute_season_elo, schema=schema)
