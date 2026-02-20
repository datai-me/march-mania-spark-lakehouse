"""Job 04 — Train a baseline model and export a Kaggle submission.

This job:
1) Reads the Gold training dataset: `gold/training_matchups`
2) Splits *by season* to reduce time-series leakage
3) Trains a baseline Logistic Regression model (Spark MLlib)
4) Generates predictions for a Kaggle submission file if available

Expected input files in `data/input/`:
- Gold is produced by previous jobs.
- Submission template file depends on Kaggle competition, common names are:
  - MSampleSubmissionStage1.csv
  - MSampleSubmissionStage2.csv
  - sample_submission.csv

This job will try to find one of those file names automatically.

How submission rows usually look:
- Column: `ID` formatted like `2026_1101_1234` (Season_Team1_Team2)
- Column: `Pred` = predicted probability Team1 beats Team2

Run:
    docker compose run --rm spark-submit python jobs/04_train_and_export_submission.py

Output:
- `artifacts/submission.csv` (mounted to your host)
"""

from pathlib import Path
from typing import Optional, Tuple

from pyspark.sql import functions as F

from src.common.spark import build_spark
from src.common.paths import gold_path, silver_path, LOCAL_INPUT_DIR
from src.common.logging import get_logger
from src.ml.modeling import build_pipeline, evaluate
from src.features.basketball_features import build_matchup_features

logger = get_logger(__name__)


SUBMISSION_CANDIDATES = [
    "MSampleSubmissionStage1.csv",
    "MSampleSubmissionStage2.csv",
    "sample_submission.csv",
]


def find_submission_file(input_dir: Path) -> Optional[Path]:
    for name in SUBMISSION_CANDIDATES:
        p = input_dir / name
        if p.exists():
            return p
    return None


def parse_id(id_value: str) -> Tuple[int, int, int]:
    """Parse Kaggle matchup ID: Season_Team1_Team2."""
    parts = id_value.split("_")
    if len(parts) != 3:
        raise ValueError(f"Unexpected ID format: {id_value}")
    return int(parts[0]), int(parts[1]), int(parts[2])


def main() -> None:
    spark = build_spark("march-mania-04-train-export")

    # Load Gold training set
    train_df = spark.read.parquet(gold_path("training_matchups"))

    # Season-based split to reduce leakage:
    # - keep the latest season as validation (or last available season)
    seasons = [r["Season"] for r in train_df.select("Season").distinct().collect()]
    if not seasons:
        raise RuntimeError("No seasons found in gold training dataset.")
    max_season = max(seasons)
    val_season = max_season

    train = train_df.filter(F.col("Season") < F.lit(val_season))
    val = train_df.filter(F.col("Season") == F.lit(val_season))

    logger.info("Training rows=%d, Validation rows=%d (val_season=%d)", train.count(), val.count(), val_season)

    pipeline = build_pipeline()
    model = pipeline.fit(train)

    if val.count() > 0:
        metrics = evaluate(model, val)
        logger.info("Validation metrics: AUC=%.4f, LogLoss=%.5f", metrics["auc"], metrics["logloss"])
    else:
        logger.warning("No validation rows found for season=%d; skipping eval.", val_season)

    # Try to build submission
    input_dir = Path(LOCAL_INPUT_DIR)
    sub_file = find_submission_file(input_dir)
    if sub_file is None:
        logger.warning("No sample submission file found in %s. Skipping submission export.", input_dir)
        logger.warning("Put one of %s into data/input/ to enable submission export.", SUBMISSION_CANDIDATES)
        return

    logger.info("Reading submission template: %s", sub_file)
    sub = spark.read.option("header", True).csv(str(sub_file))

    # Parse ID into Season/Team1/Team2
    # We do it in Spark using split for scalability.
    parts = F.split(F.col("ID"), "_")
    matchups = (
        sub.select(
            F.col("ID"),
            parts.getItem(0).cast("int").alias("Season"),
            parts.getItem(1).cast("int").alias("Team1"),
            parts.getItem(2).cast("int").alias("Team2"),
        )
    )

    # Load Silver stats and compute matchup features (same logic as training)
    team_stats = spark.read.parquet(silver_path("team_season_stats"))
    feats = build_matchup_features(matchups, team_stats)

    # Predict
    scored = model.transform(feats).select("ID", F.col("probability").getItem(1).alias("Pred"))

    # Write to artifacts as a single CSV file (Kaggle expects a single file)
    out_dir = "/opt/project/artifacts/submission_tmp"
    out_file = "/opt/project/artifacts/submission.csv"

    logger.info("Writing submission to %s", out_file)

    scored.coalesce(1).write.mode("overwrite").option("header", True).csv(out_dir)

    # Rename part file to submission.csv (inside container filesystem)
    # The host sees it via the mounted ./artifacts folder.
    import os, glob, shutil
    part = glob.glob(os.path.join(out_dir, "part-*.csv"))
    if not part:
        raise RuntimeError("No part file found after writing submission.")
    shutil.copy(part[0], out_file)
    logger.info("Submission exported: %s", out_file)


if __name__ == "__main__":
    main()
