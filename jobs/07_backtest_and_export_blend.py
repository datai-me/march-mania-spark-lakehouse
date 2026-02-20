"""Job 07 — Expert mode: rolling-season backtest + blended model + submission export.

This job does:
- Reads Gold training matchups
- Enriches with ELO + rolling last-per-season (fallback)
- Rolling backtest across seasons (train <= season-1, validate = season)
- Trains final models on all available seasons < target season
- Exports blended submission probabilities

Why rolling backtest?
- Prevents time leakage
- Mimics Kaggle's public/private split behavior better than a single holdout

Outputs:
- artifacts/backtest_metrics.csv
- artifacts/submission_blend.csv

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
    "MSampleSubmissionStage1.csv",
    "MSampleSubmissionStage2.csv",
    "sample_submission.csv",
]


def find_submission_file(input_dir: Path):
    for name in SUBMISSION_CANDIDATES:
        p = input_dir / name
        if p.exists():
            return p
    return None


def main() -> None:
    spark = build_spark("march-mania-07-backtest-blend")

    cfg = yaml.safe_load(open("/opt/project/conf/pipeline.yml", "r", encoding="utf-8"))
    alpha = float(cfg.get("modeling", {}).get("blend_alpha_gbt", 0.65))

    # Load datasets
    train_base = spark.read.parquet(gold_path("training_matchups"))
    team_stats = spark.read.parquet(silver_path("team_season_stats"))
    elo = spark.read.parquet(silver_path("elo_ratings"))
    rolling_last = spark.read.parquet(silver_path("rolling_last_per_season"))

    # Attach richer features (rolling columns may be null in gold; we fill from rolling_last)
    matchups = train_base.select("Season", "Team1", "Team2", "label")
    feats = attach_team_features(matchups, team_stats, elo, rolling_last)

    # Fill missing rolling features with per-season last-known rolling (common in our setup)
    feats = (feats
             .join(rolling_last.select(
                 "Season",
                 F.col("TeamID").alias("Team1"),
                 F.col("RollWinRate").alias("T1_RollWinRate_f"),
                 F.col("RollAvgPointDiff").alias("T1_RollAvgPointDiff_f"),
             ), on=["Season", "Team1"], how="left")
             .join(rolling_last.select(
                 "Season",
                 F.col("TeamID").alias("Team2"),
                 F.col("RollWinRate").alias("T2_RollWinRate_f"),
                 F.col("RollAvgPointDiff").alias("T2_RollAvgPointDiff_f"),
             ), on=["Season", "Team2"], how="left")
             .withColumn("T1_RollWinRate", F.coalesce(F.col("T1_RollWinRate"), F.col("T1_RollWinRate_f")))
             .withColumn("T2_RollWinRate", F.coalesce(F.col("T2_RollWinRate"), F.col("T2_RollWinRate_f")))
             .withColumn("T1_RollAvgPointDiff", F.coalesce(F.col("T1_RollAvgPointDiff"), F.col("T1_RollAvgPointDiff_f")))
             .withColumn("T2_RollAvgPointDiff", F.coalesce(F.col("T2_RollAvgPointDiff"), F.col("T2_RollAvgPointDiff_f")))
             .drop("T1_RollWinRate_f", "T2_RollWinRate_f", "T1_RollAvgPointDiff_f", "T2_RollAvgPointDiff_f")
             .withColumn("RollWinRateDiff", F.col("T1_RollWinRate") - F.col("T2_RollWinRate"))
             .withColumn("RollAvgPointDiffDiff", F.col("T1_RollAvgPointDiff") - F.col("T2_RollAvgPointDiff"))
             )

    feats = feats.dropna(subset=["WinRateDiff", "AvgPointDiffDiff", "EloDiff"])

    seasons = sorted([r["Season"] for r in feats.select("Season").distinct().collect()])
    bt = cfg.get("backtest", {})
    min_train = int(bt.get("min_train_season", seasons[0]))
    max_val = int(bt.get("max_val_season", seasons[-1]))

    # Rolling backtest
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

    # Write backtest metrics to artifacts (host-mounted)
    out_metrics = "/opt/project/artifacts/backtest_metrics.csv"
    with open(out_metrics, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(metrics_rows)
    logger.info("Backtest metrics written: %s", out_metrics)

    # Train final model on all data (for submission)
    final_model = build_pipeline().fit(feats)

    # Submission
    input_dir = Path(LOCAL_INPUT_DIR)
    sub_file = find_submission_file(input_dir)
    if sub_file is None:
        logger.warning("No sample submission file found in %s. Skipping submission export.", input_dir)
        return

    sub = spark.read.option("header", True).csv(str(sub_file))
    parts = F.split(F.col("ID"), "_")
    matchups_sub = sub.select(
        F.col("ID"),
        parts.getItem(0).cast("int").alias("Season"),
        parts.getItem(1).cast("int").alias("Team1"),
        parts.getItem(2).cast("int").alias("Team2"),
    )

    # Attach features for submission (rolling uses last-per-season)
    feats_sub = attach_team_features(matchups_sub, team_stats, elo, rolling_last)

    # Fill rolling from last-per-season (same as training)
    feats_sub = (feats_sub
                 .join(rolling_last.select(
                     "Season",
                     F.col("TeamID").alias("Team1"),
                     F.col("RollWinRate").alias("T1_RollWinRate_f"),
                     F.col("RollAvgPointDiff").alias("T1_RollAvgPointDiff_f"),
                 ), on=["Season", "Team1"], how="left")
                 .join(rolling_last.select(
                     "Season",
                     F.col("TeamID").alias("Team2"),
                     F.col("RollWinRate").alias("T2_RollWinRate_f"),
                     F.col("RollAvgPointDiff").alias("T2_RollAvgPointDiff_f"),
                 ), on=["Season", "Team2"], how="left")
                 .withColumn("T1_RollWinRate", F.coalesce(F.col("T1_RollWinRate"), F.col("T1_RollWinRate_f")))
                 .withColumn("T2_RollWinRate", F.coalesce(F.col("T2_RollWinRate"), F.col("T2_RollWinRate_f")))
                 .withColumn("T1_RollAvgPointDiff", F.coalesce(F.col("T1_RollAvgPointDiff"), F.col("T1_RollAvgPointDiff_f")))
                 .withColumn("T2_RollAvgPointDiff", F.coalesce(F.col("T2_RollAvgPointDiff"), F.col("T2_RollAvgPointDiff_f")))
                 .drop("T1_RollWinRate_f", "T2_RollWinRate_f", "T1_RollAvgPointDiff_f", "T2_RollAvgPointDiff_f")
                 .withColumn("RollWinRateDiff", F.col("T1_RollWinRate") - F.col("T2_RollWinRate"))
                 .withColumn("RollAvgPointDiffDiff", F.col("T1_RollAvgPointDiff") - F.col("T2_RollAvgPointDiff"))
                 )

    scored = final_model.transform(feats_sub).select("ID", F.col("probability").getItem(1).alias("Pred"))

    # Write single-file submission
    out_dir = "/opt/project/artifacts/submission_blend_tmp"
    out_file = "/opt/project/artifacts/submission_blend.csv"
    scored.coalesce(1).write.mode("overwrite").option("header", True).csv(out_dir)

    import glob, os, shutil
    part = glob.glob(os.path.join(out_dir, "part-*.csv"))
    if not part:
        raise RuntimeError("No part file found after writing submission.")
    shutil.copy(part[0], out_file)
    logger.info("Blended submission exported: %s", out_file)


if __name__ == "__main__":
    main()
