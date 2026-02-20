"""Job 12 — Train an ensemble (LogReg + GBT) and export Kaggle submission.

Ensemble idea:
- Train Logistic Regression + GBT on all available data
- Blend probabilities:
    Pred = alpha * Pred_GBT + (1-alpha) * Pred_LR

Why:
- Blending usually stabilizes LogLoss and improves leaderboard robustness.

Inputs:
- Gold training_matchups
- SampleSubmissionStage1/2.csv in data/input/

Outputs:
- artifacts/submission_ensemble.csv

Run:
  docker compose run --rm spark-submit python jobs/12_train_ensemble_export.py
"""

import json
from pathlib import Path

import yaml
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression, GBTClassifier
from pyspark.ml import Pipeline

from src.common.spark import build_spark
from src.common.paths import gold_path, silver_path, LOCAL_INPUT_DIR
from src.common.logging import get_logger
from src.ml.modeling import FEATURE_COLS
from src.features.basketball_features_plus import attach_team_features

logger = get_logger(__name__)

SUBMISSION_CANDIDATES = [
    "SampleSubmissionStage1.csv",
    "SampleSubmissionStage2.csv",
    "sample_submission.csv",
]


def find_submission_file(input_dir: Path):
    for name in SUBMISSION_CANDIDATES:
        p = input_dir / name
        if p.exists():
            return p
    return None


def _load_hpo_params() -> dict:
    p = "/opt/project/artifacts/hpo_best_params.json"
    try:
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _fit_lr(train_df, params: dict):
    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features")
    lr = LogisticRegression(featuresCol="features", labelCol="label", probabilityCol="probability",
                            maxIter=int(params.get("maxIter", 80)),
                            regParam=float(params.get("regParam", 0.05)),
                            elasticNetParam=float(params.get("elasticNetParam", 0.0)))
    pipe = Pipeline(stages=[assembler, lr])
    return pipe.fit(train_df)


def _fit_gbt(train_df, params: dict):
    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features")
    gbt = GBTClassifier(featuresCol="features", labelCol="label",
                        maxIter=int(params.get("maxIter", 120)),
                        maxDepth=int(params.get("maxDepth", 5)),
                        subsamplingRate=float(params.get("subsamplingRate", 0.8)),
                        seed=42)
    pipe = Pipeline(stages=[assembler, gbt])
    return pipe.fit(train_df)


def main() -> None:
    spark = build_spark("march-mania-12-ensemble-export")

    cfg = yaml.safe_load(open("/opt/project/conf/pipeline.yml", "r", encoding="utf-8"))
    league = str(cfg.get("competition", {}).get("league", "M")).upper()
    alpha = float(cfg.get("modeling", {}).get("blend_alpha_gbt", 0.65))

    train_df = spark.read.parquet(gold_path(f"{league}/training_matchups")).cache()

    hpo = _load_hpo_params()
    lr_params = (hpo.get("logreg", {}) or {}).get("params", {})
    gbt_params = (hpo.get("gbt", {}) or {}).get("params", {})

    logger.info("Training LR with params=%s", lr_params)
    lr_model = _fit_lr(train_df, lr_params)

    logger.info("Training GBT with params=%s", gbt_params)
    gbt_model = _fit_gbt(train_df, gbt_params)

    # Submission template
    input_dir = Path(LOCAL_INPUT_DIR)
    sub_file = find_submission_file(input_dir)
    if sub_file is None:
        logger.warning("No sample submission file found in %s. Put SampleSubmissionStage1/2.csv in data/input/", input_dir)
        return

    sub = spark.read.option("header", True).csv(str(sub_file))
    parts = F.split(F.col("ID"), "_")
    matchups_sub = sub.select(
        F.col("ID"),
        parts.getItem(0).cast("int").alias("Season"),
        parts.getItem(1).cast("int").alias("Team1"),
        parts.getItem(2).cast("int").alias("Team2"),
    )

    # Build features for submission (same as expert job)
    team_stats = spark.read.parquet(silver_path(f"{league}/team_season_stats"))
    elo = spark.read.parquet(silver_path(f"{league}/elo_ratings"))
    rolling_last = spark.read.parquet(silver_path(f"{league}/rolling_last_per_season"))

    feats_sub = attach_team_features(matchups_sub, team_stats, elo, rolling_last)

    # Optional diffs (seeds, massey, sos) for better parity with training features
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

    # Predict with both models
    lr_scored = lr_model.transform(feats_sub).select("ID", F.col("probability").getItem(1).alias("p_lr"))
    gbt_scored = gbt_model.transform(feats_sub).select("ID", F.col("probability").getItem(1).alias("p_gbt"))

    blended = (lr_scored.join(gbt_scored, on="ID", how="inner")
               .withColumn("Pred", F.lit(alpha) * F.col("p_gbt") + (F.lit(1.0 - alpha)) * F.col("p_lr"))
               .select("ID", "Pred"))

    # Write single-file CSV
    out_dir = "/opt/project/artifacts/submission_ensemble_tmp"
    out_file = "/opt/project/artifacts/submission_ensemble.csv"
    blended.coalesce(1).write.mode("overwrite").option("header", True).csv(out_dir)

    import glob, os, shutil
    part = glob.glob(os.path.join(out_dir, "part-*.csv"))
    if not part:
        raise RuntimeError("No part file found after writing submission.")
    shutil.copy(part[0], out_file)
    logger.info("Ensemble submission exported: %s", out_file)


if __name__ == "__main__":
    main()
