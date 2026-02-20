"""Seed feature engineering.

Kaggle seeds are strings like:
- "W01", "X16", etc.
Sometimes they may include play-in suffixes (e.g., "W16a", "W16b").

We parse:
- Region (first char)
- SeedNum (integer)
"""

import re
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

_SEED_RE = r"^([A-Z])(\d{2})"


def build_seeds(seeds_df: DataFrame) -> DataFrame:
    """Parse tournament seeds.

    Expected columns:
    - Season, TeamID, Seed

    Returns:
        DataFrame(Season:int, TeamID:int, Seed:str, SeedRegion:str, SeedNum:int)
    """
    seed = F.col("Seed")
    region = F.regexp_extract(seed, _SEED_RE, 1)
    num = F.regexp_extract(seed, _SEED_RE, 2)

    out = (seeds_df
           .select(
               F.col("Season").cast("int").alias("Season"),
               F.col("TeamID").cast("int").alias("TeamID"),
               F.col("Seed").alias("Seed"),
           )
           .withColumn("SeedRegion", region)
           .withColumn("SeedNum", num.cast("int")))
    return out
