"""Job 07 — Expert mode: rolling-season backtest + submission export (league-aware).

This job assumes you have run:
- 01 Bronze ingest
- 02 Silver team stats
- 05 Silver ELO
- 06 Silver rolling (optional but recommended)
- 08 Silver seeds (recommended)
- 09 Silver Massey (Men only, optional)
- 10 Silver SOS (recommended)
- 03 Gold training set (now includes many diffs)

It then:
- runs a rolling-season backtest (train <= season-1, validate = season)
- trains a final model on all seasons
- exports Kaggle submission file:
  - artifacts/submission_expert.csv
  - artifacts/backtest_metrics.csv

Run:
    docker compose run --rm spark-submit python jobs/07_backtest_and_export_blend.py
"""

from pathlib import Path
import csv
import yaml

from pyspark.sql import functions as F

from src.common.spark import build_spark
from src.common.paths import gold_path, silver_path, LOCAL_INPUT_DIR
from src.common.logging import get_logger
from src.ml.modeling import build_pipeline, evaluate
from src.features.basketball_features_plus import attach_team_features

logger = get_logger(__name__)

SUBMISSION_CANDIDATES = [
    "SampleSubmissionStage1.csv",
    "SampleSubmissionStage2.csv",
    "MSampleSubmissionStage1.csv",
    "MSampleSubmissionStage2.csv",
    "WSampleSubmissionStage1.csv",
    "WSampleSubmissionStage2.csv",
    "sample_submission.csv",
]


def find_submission_file(input_dir: Path):
    for name in SUBMISSION_CANDIDATES:
        p = input_dir / name
        if p.exists():
            return p
    return None


def main() -> None:
    spark = build_spark("march-mania-07-expert")

    cfg = yaml.safe_load(open("/opt/project/conf/pipeline.yml", "r", encoding="utf-8"))
    league = str(cfg.get("competition", {}).get("league", "M")).upper()
    if league not in {"M", "W"}:
        raise ValueError("competition.league must be 'M' or 'W'")

    feats = spark.read.parquet(gold_path(f"{league}/training_matchups")).cache()

    seasons = sorted([r["Season"] for r in feats.select("Season").distinct().collect()])
    bt = cfg.get("backtest", {})
    min_train = int(bt.get("min_train_season", seasons[0]))
    max_val = int(bt.get("max_val_season", seasons[-1]))

    metrics_rows = [("val_season", "train_rows", "val_rows", "auc", "logloss")]
    for s in seasons:
        if s < min_train or s > max_val:
            continue
        train = feats.filter(F.col("Season") < F.lit(s))
        val = feats.filter(F.col("Season") == F.lit(s))
        if train.count() == 0 or val.count() == 0:
            continue

        model = build_pipeline().fit(train)
        m = evaluate(model, val)
        metrics_rows.append((str(s), str(train.count()), str(val.count()), f"{m['auc']:.4f}", f"{m['logloss']:.5f}"))
        logger.info("Backtest season=%d AUC=%.4f LogLoss=%.5f", s, m["auc"], m["logloss"])

    out_metrics = "/opt/project/artifacts/backtest_metrics.csv"
    with open(out_metrics, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(metrics_rows)
    logger.info("Backtest metrics written: %s", out_metrics)

    final_model = build_pipeline().fit(feats)

    # Submission template
    input_dir = Path(LOCAL_INPUT_DIR)
    sub_file = find_submission_file(input_dir)
    if sub_file is None:
        logger.warning("No sample submission file found in %s. Put SampleSubmissionStage1/2.csv in data/input/", input_dir)
        return

    logger.info("Reading submission template: %s", sub_file)
    sub = spark.read.option("header", True).csv(str(sub_file))
    parts = F.split(F.col("ID"), "_")
    matchups_sub = sub.select(
        F.col("ID"),
        parts.getItem(0).cast("int").alias("Season"),
        parts.getItem(1).cast("int").alias("Team1"),
        parts.getItem(2).cast("int").alias("Team2"),
    )

    # Attach features for submission (use latest per season for rolling; handled in attach + gold builder approach)
    team_stats = spark.read.parquet(silver_path(f"{league}/team_season_stats"))
    elo = spark.read.parquet(silver_path(f"{league}/elo_ratings"))
    rolling_last = spark.read.parquet(silver_path(f"{league}/rolling_last_per_season"))

    feats_sub = attach_team_features(matchups_sub, team_stats, elo, rolling_last)

    # Seeds
    try:
        seeds = spark.read.parquet(silver_path(f"{league}/tourney_seeds_parsed"))
        t1_seed = seeds.select("Season", F.col("TeamID").alias("Team1"), F.col("SeedNum").alias("T1_SeedNum"))
        t2_seed = seeds.select("Season", F.col("TeamID").alias("Team2"), F.col("SeedNum").alias("T2_SeedNum"))
        feats_sub = (feats_sub
                     .join(t1_seed, on=["Season", "Team1"], how="left")
                     .join(t2_seed, on=["Season", "Team2"], how="left")
                     .withColumn("SeedDiff", F.col("T1_SeedNum") - F.col("T2_SeedNum")))
    except Exception:
        feats_sub = feats_sub.withColumn("SeedDiff", F.lit(None).cast("double"))

    # Massey (Men)
    if league == "M":
        try:
            massey = spark.read.parquet(silver_path("M/massey_consensus"))
            t1_m = massey.select("Season", F.col("TeamID").alias("Team1"), F.col("MasseyMeanRank").alias("T1_Massey"))
            t2_m = massey.select("Season", F.col("TeamID").alias("Team2"), F.col("MasseyMeanRank").alias("T2_Massey"))
            feats_sub = (feats_sub
                         .join(t1_m, on=["Season", "Team1"], how="left")
                         .join(t2_m, on=["Season", "Team2"], how="left")
                         .withColumn("MasseyDiff", F.col("T1_Massey") - F.col("T2_Massey")))
        except Exception:
            feats_sub = feats_sub.withColumn("MasseyDiff", F.lit(None).cast("double"))
    else:
        feats_sub = feats_sub.withColumn("MasseyDiff", F.lit(None).cast("double"))

    # SOS
    try:
        sos = spark.read.parquet(silver_path(f"{league}/strength_of_schedule"))
        t1_s = sos.select("Season", F.col("TeamID").alias("Team1"),
                          F.col("SOS_OppWinRate").alias("T1_SOS_OppWinRate"),
                          F.col("SOS_OppElo").alias("T1_SOS_OppElo"))
        t2_s = sos.select("Season", F.col("TeamID").alias("Team2"),
                          F.col("SOS_OppWinRate").alias("T2_SOS_OppWinRate"),
                          F.col("SOS_OppElo").alias("T2_SOS_OppElo"))
        feats_sub = (feats_sub
                     .join(t1_s, on=["Season", "Team1"], how="left")
                     .join(t2_s, on=["Season", "Team2"], how="left")
                     .withColumn("SOSWinRateDiff", F.col("T1_SOS_OppWinRate") - F.col("T2_SOS_OppWinRate"))
                     .withColumn("SOSEloDiff", F.col("T1_SOS_OppElo") - F.col("T2_SOS_OppElo")))
    except Exception:
        feats_sub = (feats_sub
                     .withColumn("SOSWinRateDiff", F.lit(None).cast("double"))
                     .withColumn("SOSEloDiff", F.lit(None).cast("double")))

    scored = final_model.transform(feats_sub).select("ID", F.col("probability").getItem(1).alias("Pred"))

    out_dir = "/opt/project/artifacts/submission_expert_tmp"
    out_file = "/opt/project/artifacts/submission_expert.csv"
    scored.coalesce(1).write.mode("overwrite").option("header", True).csv(out_dir)

    import glob, os, shutil
    part = glob.glob(os.path.join(out_dir, "part-*.csv"))
    if not part:
        raise RuntimeError("No part file found after writing submission.")
    shutil.copy(part[0], out_file)
    logger.info("Expert submission exported: %s", out_file)


if __name__ == "__main__":
    main()
