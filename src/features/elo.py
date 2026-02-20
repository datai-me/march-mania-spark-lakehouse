"""ELO feature engineering for NCAA match prediction.

We compute **season-scoped ELO** (reset each season) using regular season results.

Why ELO?
- Captures team strength in a single number.
- Often provides a strong uplift in LogLoss competitions.

Implementation notes (local / docker):
- Kaggle March Mania datasets are not huge, so we can safely compute ELO per season
  using a Pandas UDF grouped by Season.
- This is reproducible and explainable (great for portfolio/exam).
"""

from __future__ import annotations

from typing import Iterator, Tuple

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, DoubleType
)


def _expected_score(ra: float, rb: float) -> float:
    return 1.0 / (1.0 + 10.0 ** ((rb - ra) / 400.0))


def _update(ra: float, rb: float, sa: float, k: float) -> Tuple[float, float]:
    ea = _expected_score(ra, rb)
    eb = 1.0 - ea
    ra2 = ra + k * (sa - ea)
    rb2 = rb + k * ((1.0 - sa) - eb)
    return ra2, rb2


def build_elo_per_season(
    regular_season_results: DataFrame,
    initial_rating: float = 1500.0,
    k_factor: float = 20.0,
) -> DataFrame:
    """Compute final ELO rating per team per season (after all regular season games).

    Expected columns:
    - Season, DayNum, WTeamID, LTeamID

    Returns:
        DataFrame(Season:int, TeamID:int, Elo:double)
    """
    # Ensure required columns exist (fail fast in jobs)
    needed = {"Season", "DayNum", "WTeamID", "LTeamID"}
    missing = needed - set(regular_season_results.columns)
    if missing:
        raise ValueError(f"Missing required columns for ELO: {sorted(missing)}")

    # We only need these columns and must sort games by day to avoid leakage inside season.
    games = (regular_season_results
             .select(
                 F.col("Season").cast("int").alias("Season"),
                 F.col("DayNum").cast("int").alias("DayNum"),
                 F.col("WTeamID").cast("int").alias("WTeamID"),
                 F.col("LTeamID").cast("int").alias("LTeamID"),
             ))

    schema = StructType([
        StructField("Season", IntegerType(), False),
        StructField("TeamID", IntegerType(), False),
        StructField("Elo", DoubleType(), False),
    ])

    def per_season(pdf_iter: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        for pdf in pdf_iter:
            if pdf.empty:
                continue
            season = int(pdf["Season"].iloc[0])
            pdf = pdf.sort_values("DayNum", kind="mergesort")

            ratings: dict[int, float] = {}

            def get_rating(t: int) -> float:
                return ratings.get(int(t), initial_rating)

            for _, row in pdf.iterrows():
                w = int(row["WTeamID"])
                l = int(row["LTeamID"])
                rw = get_rating(w)
                rl = get_rating(l)
                # Winner score = 1
                rw2, rl2 = _update(rw, rl, 1.0, k_factor)
                ratings[w] = rw2
                ratings[l] = rl2

            out = pd.DataFrame({
                "Season": [season] * len(ratings),
                "TeamID": list(ratings.keys()),
                "Elo": list(ratings.values()),
            })
            yield out

    return games.groupBy("Season").applyInPandas(per_season, schema=schema)
