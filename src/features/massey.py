"""Massey Ordinals feature engineering (Men's dataset).

MMasseyOrdinals.csv contains multiple ranking systems over time.

We build a simple, strong feature:
- For each Season, TeamID:
  - Take the latest DayNum available per SystemName
  - Then average OrdinalRank across systems

This produces a robust consensus ranking.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_massey_consensus(massey: DataFrame) -> DataFrame:
    """Build consensus ranking features.

    Expected columns (typical Kaggle):
    - Season, TeamID, SystemName, RankingDayNum, OrdinalRank

    Returns:
        DataFrame(Season:int, TeamID:int, MasseyMeanRank:double, MasseyMedianRank:double, Systems:int)
    """
    needed = {"Season", "TeamID", "SystemName", "RankingDayNum", "OrdinalRank"}
    missing = needed - set(massey.columns)
    if missing:
        raise ValueError(f"Missing required columns for Massey: {sorted(missing)}")

    df = (massey
          .select(
              F.col("Season").cast("int").alias("Season"),
              F.col("TeamID").cast("int").alias("TeamID"),
              F.col("SystemName").alias("SystemName"),
              F.col("RankingDayNum").cast("int").alias("RankingDayNum"),
              F.col("OrdinalRank").cast("int").alias("OrdinalRank"),
          ))

    # Latest ranking per system within season/team
    w = Window.partitionBy("Season", "TeamID", "SystemName").orderBy(F.col("RankingDayNum").desc())
    latest = df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")

    # Aggregate across systems
    out = (latest
           .groupBy("Season", "TeamID")
           .agg(
               F.avg("OrdinalRank").alias("MasseyMeanRank"),
               F.expr("percentile_approx(OrdinalRank, 0.5)").alias("MasseyMedianRank"),
               F.count("*").alias("Systems"),
           ))
    return out
