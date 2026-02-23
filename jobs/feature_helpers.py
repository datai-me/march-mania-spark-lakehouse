"""
feature_helpers.py — Utilitaires partagés entre les jobs 03, 07 et 12.

Ce module centralise les jointures optionnelles (Seeds, Massey, SOS) qui
étaient copiées-collées dans chaque job. Toute modification ici se propage
automatiquement partout.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.logging import get_logger
from src.common.paths import silver_path

logger = get_logger(__name__)


# ─── Seeds ────────────────────────────────────────────────────────────────────

def attach_seed_diff(df: DataFrame, league: str, spark: SparkSession) -> DataFrame:
    """
    Ajoute la colonne `SeedDiff` = SeedNum(Team1) − SeedNum(Team2).
    Si les seeds ne sont pas disponibles en Silver, `SeedDiff` est mis à null.
    """
    try:
        seeds = spark.read.parquet(silver_path(f"{league}/tourney_seeds_parsed"))
        t1 = seeds.select("Season", F.col("TeamID").alias("Team1"), F.col("SeedNum").alias("T1_SeedNum"))
        t2 = seeds.select("Season", F.col("TeamID").alias("Team2"), F.col("SeedNum").alias("T2_SeedNum"))
        return (
            df.join(t1, on=["Season", "Team1"], how="left")
              .join(t2, on=["Season", "Team2"], how="left")
              .withColumn("SeedDiff", F.col("T1_SeedNum") - F.col("T2_SeedNum"))
        )
    except Exception as e:
        logger.warning("Seeds non disponibles (%s). SeedDiff = null. Lancez le job 08.", e)
        return df.withColumn("SeedDiff", F.lit(None).cast("double"))


# ─── Massey (Hommes uniquement) ───────────────────────────────────────────────

def attach_massey_diff(df: DataFrame, league: str, spark: SparkSession) -> DataFrame:
    """
    Ajoute `MasseyDiff` = MasseyMeanRank(Team1) − MasseyMeanRank(Team2).
    Disponible uniquement pour les hommes (league='M').
    Retourne `MasseyDiff = null` pour les femmes ou si le fichier est absent.
    """
    if league != "M":
        return df.withColumn("MasseyDiff", F.lit(None).cast("double"))
    try:
        massey = spark.read.parquet(silver_path("M/massey_consensus"))
        t1 = massey.select("Season", F.col("TeamID").alias("Team1"), F.col("MasseyMeanRank").alias("T1_Massey"))
        t2 = massey.select("Season", F.col("TeamID").alias("Team2"), F.col("MasseyMeanRank").alias("T2_Massey"))
        return (
            df.join(t1, on=["Season", "Team1"], how="left")
              .join(t2, on=["Season", "Team2"], how="left")
              .withColumn("MasseyDiff", F.col("T1_Massey") - F.col("T2_Massey"))
        )
    except Exception as e:
        logger.warning("Massey non disponible (%s). MasseyDiff = null. Lancez le job 09.", e)
        return df.withColumn("MasseyDiff", F.lit(None).cast("double"))


# ─── Strength of Schedule (SOS) ───────────────────────────────────────────────

def attach_sos_diff(df: DataFrame, league: str, spark: SparkSession) -> DataFrame:
    """
    Ajoute `SOSWinRateDiff` et `SOSEloDiff`.
    Si SOS non disponible, les deux colonnes sont mises à null.
    """
    try:
        sos = spark.read.parquet(silver_path(f"{league}/strength_of_schedule"))
        t1 = sos.select("Season",
                        F.col("TeamID").alias("Team1"),
                        F.col("SOS_OppWinRate").alias("T1_SOS_OppWinRate"),
                        F.col("SOS_OppElo").alias("T1_SOS_OppElo"))
        t2 = sos.select("Season",
                        F.col("TeamID").alias("Team2"),
                        F.col("SOS_OppWinRate").alias("T2_SOS_OppWinRate"),
                        F.col("SOS_OppElo").alias("T2_SOS_OppElo"))
        return (
            df.join(t1, on=["Season", "Team1"], how="left")
              .join(t2, on=["Season", "Team2"], how="left")
              .withColumn("SOSWinRateDiff", F.col("T1_SOS_OppWinRate") - F.col("T2_SOS_OppWinRate"))
              .withColumn("SOSEloDiff",     F.col("T1_SOS_OppElo")     - F.col("T2_SOS_OppElo"))
        )
    except Exception as e:
        logger.warning("SOS non disponible (%s). SOS diffs = null. Lancez le job 10.", e)
        return (
            df.withColumn("SOSWinRateDiff", F.lit(None).cast("double"))
              .withColumn("SOSEloDiff",     F.lit(None).cast("double"))
        )


# ─── Raccourci : toutes les features optionnelles d'un coup ───────────────────

def attach_optional_features(df: DataFrame, league: str, spark: SparkSession) -> DataFrame:
    """
    Applique séquentiellement Seeds → Massey → SOS.
    Utiliser dans les jobs 03, 07 et 12 pour remplacer les 3 blocs try/except.
    """
    df = attach_seed_diff(df, league, spark)
    df = attach_massey_diff(df, league, spark)
    df = attach_sos_diff(df, league, spark)
    return df
