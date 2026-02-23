"""
massey.py — Consensus des rankings Massey (Hommes uniquement)
══════════════════════════════════════════════════════════════
MMasseyOrdinals.csv contient des rankings produits par de nombreux systèmes
différents (BPI, KenPom, Sagarin, etc.) à différentes dates de la saison.

Stratégie de consensus :
    1. Pour chaque saison × équipe × système : garder le ranking LE PLUS RÉCENT
       (évite de surpondérer les systèmes qui publient souvent).
    2. Agréger sur tous les systèmes : moyenne et médiane des rangs.

Résultat : MasseyMeanRank — un rang consensuel stable et robuste.
    → Rang faible = meilleure équipe (ex: rang 1 = #1 national).
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

_REQUIRED = {"Season", "TeamID", "SystemName", "RankingDayNum", "OrdinalRank"}


def build_massey_consensus(massey: DataFrame) -> DataFrame:
    """
    Construit un ranking consensus à partir des ordinals Massey.

    Entrée (MMasseyOrdinals.csv) :
        Season, TeamID, SystemName, RankingDayNum, OrdinalRank

    Sortie :
        Season:int, TeamID:int,
        MasseyMeanRank:double   ← moyenne des rangs (feature principale)
        MasseyMedianRank:double ← médiane des rangs (feature robuste)
        Systems:int             ← nombre de systèmes couvrant cette équipe/saison

    Args:
        massey : DataFrame Bronze (MMasseyOrdinals.csv)
    """
    missing = _REQUIRED - set(massey.columns)
    if missing:
        raise ValueError(f"[build_massey_consensus] Colonnes manquantes : {sorted(missing)}")

    df = massey.select(
        F.col("Season").cast("int"),
        F.col("TeamID").cast("int"),
        F.col("SystemName"),
        F.col("RankingDayNum").cast("int"),
        F.col("OrdinalRank").cast("int"),
    )

    # Étape 1 : garder uniquement le ranking le plus récent par système × saison × équipe
    # row_number() dans une fenêtre descendante sur RankingDayNum → rn=1 = plus récent
    latest_window = Window.partitionBy("Season", "TeamID", "SystemName").orderBy(
        F.col("RankingDayNum").desc()
    )
    latest = (
        df.withColumn("rn", F.row_number().over(latest_window))
          .filter(F.col("rn") == 1)
          .drop("rn", "RankingDayNum")  # plus nécessaires après filtrage
    )

    # Étape 2 : agréger sur tous les systèmes disponibles pour la saison × équipe
    return (
        latest
        .groupBy("Season", "TeamID")
        .agg(
            F.avg("OrdinalRank").alias("MasseyMeanRank"),
            F.expr("percentile_approx(OrdinalRank, 0.5)").alias("MasseyMedianRank"),
            F.count("*").alias("Systems"),
        )
    )
