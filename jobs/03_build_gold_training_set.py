"""Job 03 — Build Gold training dataset (matchups with features), league-aware.

Gold is the ML-ready dataset:
- label (Team1 win?)
- feature deltas including:
  - season aggregates (WinRateDiff, AvgPointDiffDiff)
  - EloDiff
  - Rolling diffs (filled later if desired)
  - SeedDiff (if seeds available)
  - MasseyDiff (Men only, if available)
  - SOS diffs

Outputs:
- s3a://<bucket>/gold/march_mania/<league>/training_matchups/

Run:
    docker compose run --rm spark-submit python jobs/03_build_gold_training_set.py
"""

import yaml
from pyspark.sql import functions as F

from src.common.spark import build_spark
from src.common.paths import bronze_path, silver_path, gold_path
from src.common.logging import get_logger
from src.features.basketball_features_plus import attach_team_features

logger = get_logger(__name__)


def main() -> None:
    spark = build_spark("march-mania-03-gold")

    cfg = yaml.safe_load(open("/opt/project/conf/pipeline.yml", "r", encoding="utf-8"))
    league = str(cfg.get("competition", {}).get("league", "M")).upper()
    if league not in {"M", "W"}:
        raise ValueError("competition.league must be 'M' or 'W'")

    bronze_ds = "m/regular_season/compact" if league == "M" else "w/regular_season/compact"
    regular = spark.read.parquet(bronze_path(bronze_ds))

    team_stats = spark.read.parquet(silver_path(f"{league}/team_season_stats"))
    elo = spark.read.parquet(silver_path(f"{league}/elo_ratings"))
    rolling_last = spark.read.parquet(silver_path(f"{league}/rolling_last_per_season"))

    # Optional seeds
    seeds_path = silver_path(f"{league}/tourney_seeds_parsed")
    try:
        seeds = spark.read.parquet(seeds_path)
    except Exception:
        seeds = None
        logger.warning("Seeds not found at %s (run job 08). SeedDiff will be null.", seeds_path)

    # Optional Massey (Men only)
    massey = None
    if league == "M":
        massey_path = silver_path("M/massey_consensus")
        try:
            massey = spark.read.parquet(massey_path)
        except Exception:
            logger.warning("Massey consensus not found at %s (run job 09). MasseyDiff will be null.", massey_path)

    # Optional SOS
    sos_path = silver_path(f"{league}/strength_of_schedule")
    try:
        sos = spark.read.parquet(sos_path)
    except Exception:
        sos = None
        logger.warning("SOS not found at %s (run job 10). SOS diffs will be null.", sos_path)

    # Create labeled matchups
    w = (
        regular.select(
            F.col("Season").cast("int").alias("Season"),
            F.col("WTeamID").cast("int").alias("Team1"),
            F.col("LTeamID").cast("int").alias("Team2"),
        ).withColumn("label", F.lit(1))
    )
    l = (
        regular.select(
            F.col("Season").cast("int").alias("Season"),
            F.col("LTeamID").cast("int").alias("Team1"),
            F.col("WTeamID").cast("int").alias("Team2"),
        ).withColumn("label", F.lit(0))
    )
    matchups = w.unionByName(l)

    feats = attach_team_features(matchups, team_stats, elo, rolling_last)

    # Seeds diff
    if seeds is not None:
        t1_seed = seeds.select("Season", F.col("TeamID").alias("Team1"), F.col("SeedNum").alias("T1_SeedNum"))
        t2_seed = seeds.select("Season", F.col("TeamID").alias("Team2"), F.col("SeedNum").alias("T2_SeedNum"))
        feats = (feats
                 .join(t1_seed, on=["Season", "Team1"], how="left")
                 .join(t2_seed, on=["Season", "Team2"], how="left")
                 .withColumn("SeedDiff", F.col("T1_SeedNum") - F.col("T2_SeedNum")))
    else:
        feats = feats.withColumn("SeedDiff", F.lit(None).cast("double"))

    # Massey diff (Men)
    if massey is not None:
        t1_m = massey.select("Season", F.col("TeamID").alias("Team1"), F.col("MasseyMeanRank").alias("T1_Massey"))
        t2_m = massey.select("Season", F.col("TeamID").alias("Team2"), F.col("MasseyMeanRank").alias("T2_Massey"))
        feats = (feats
                 .join(t1_m, on=["Season", "Team1"], how="left")
                 .join(t2_m, on=["Season", "Team2"], how="left")
                 .withColumn("MasseyDiff", F.col("T1_Massey") - F.col("T2_Massey")))
    else:
        feats = feats.withColumn("MasseyDiff", F.lit(None).cast("double"))

    # SOS diffs
    if sos is not None:
        t1_s = sos.select("Season", F.col("TeamID").alias("Team1"),
                          F.col("SOS_OppWinRate").alias("T1_SOS_OppWinRate"),
                          F.col("SOS_OppElo").alias("T1_SOS_OppElo"))
        t2_s = sos.select("Season", F.col("TeamID").alias("Team2"),
                          F.col("SOS_OppWinRate").alias("T2_SOS_OppWinRate"),
                          F.col("SOS_OppElo").alias("T2_SOS_OppElo"))
        feats = (feats
                 .join(t1_s, on=["Season", "Team1"], how="left")
                 .join(t2_s, on=["Season", "Team2"], how="left")
                 .withColumn("SOSWinRateDiff", F.col("T1_SOS_OppWinRate") - F.col("T2_SOS_OppWinRate"))
                 .withColumn("SOSEloDiff", F.col("T1_SOS_OppElo") - F.col("T2_SOS_OppElo")))
    else:
        feats = (feats
                 .withColumn("SOSWinRateDiff", F.lit(None).cast("double"))
                 .withColumn("SOSEloDiff", F.lit(None).cast("double")))

    feats = feats.dropna(subset=["WinRateDiff", "AvgPointDiffDiff", "EloDiff"])

    out_path = gold_path(f"{league}/training_matchups")
    logger.info("Writing Gold training set: %s", out_path)
    feats.write.mode("overwrite").parquet(out_path)

    logger.info("Gold dataset complete. league=%s rows=%d", league, feats.count())


if __name__ == "__main__":
    main()
