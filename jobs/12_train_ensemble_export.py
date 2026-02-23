"""
Job 12 — Ensemble final : LR + GBT → soumission Kaggle
════════════════════════════════════════════════════════
Entraîne deux modèles sur tout le dataset Gold :
    - Logistic Regression
    - Gradient Boosted Trees

Puis blende leurs prédictions :
    Pred = alpha × P(GBT) + (1 − alpha) × P(LR)

Le blend stabilise le LogLoss et réduit la variance par rapport à un modèle seul.
alpha est configurable dans conf/pipeline.yml → modeling.blend_alpha_gbt (défaut : 0.65)

Si le job 11 (HPO) a été lancé, les meilleurs hyperparamètres sont chargés
automatiquement depuis artifacts/hpo_best_params.json.

Sortie : artifacts/submission_ensemble.csv

Usage :
    docker compose run --rm spark-submit python jobs/12_train_ensemble_export.py
"""

import glob
import json
import os
import shutil
from pathlib import Path

import yaml
from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier, LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import functions as F

from jobs.feature_helpers import attach_optional_features
from src.common.logging import get_logger
from src.common.paths import LOCAL_INPUT_DIR, gold_path, silver_path
from src.common.spark import build_spark
from src.features.basketball_features_plus import attach_team_features
from src.ml.modeling import FEATURE_COLS

logger = get_logger(__name__)

SUBMISSION_CANDIDATES = [
    "SampleSubmissionStage1.csv", "SampleSubmissionStage2.csv",
    "sample_submission.csv",
]


def find_submission_file(input_dir: Path) -> Path | None:
    for name in SUBMISSION_CANDIDATES:
        p = input_dir / name
        if p.exists():
            return p
    return None


def load_hpo_params() -> dict:
    """Charge les meilleurs hyperparamètres depuis le job 11. Retourne {} si absent."""
    try:
        with open("/opt/project/artifacts/hpo_best_params.json", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        logger.warning("hpo_best_params.json absent — utilisation des hyperparamètres par défaut.")
        return {}


def build_lr_pipeline(train_df, params: dict):
    """Construit et entraîne un pipeline Logistic Regression."""
    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features")
    lr = LogisticRegression(
        featuresCol="features", labelCol="label", probabilityCol="probability",
        maxIter=int(params.get("maxIter", 80)),
        regParam=float(params.get("regParam", 0.05)),
        elasticNetParam=float(params.get("elasticNetParam", 0.0)),
    )
    return Pipeline(stages=[assembler, lr]).fit(train_df)


def build_gbt_pipeline(train_df, params: dict):
    """Construit et entraîne un pipeline GBT."""
    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features")
    gbt = GBTClassifier(
        featuresCol="features", labelCol="label",
        maxIter=int(params.get("maxIter", 120)),
        maxDepth=int(params.get("maxDepth", 5)),
        subsamplingRate=float(params.get("subsamplingRate", 0.8)),
        seed=42,
    )
    return Pipeline(stages=[assembler, gbt]).fit(train_df)


def export_single_csv(df, tmp_dir: str, out_file: str) -> None:
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(tmp_dir)
    parts = glob.glob(os.path.join(tmp_dir, "part-*.csv"))
    if not parts:
        raise RuntimeError(f"Aucun part-file trouvé dans {tmp_dir}")
    shutil.copy(parts[0], out_file)
    logger.info("Soumission ensemble exportée : %s", out_file)


def main() -> None:
    spark = build_spark("march-mania-12-ensemble-export")
    cfg = yaml.safe_load(open("/opt/project/conf/pipeline.yml", encoding="utf-8"))

    league = str(cfg.get("competition", {}).get("league", "M")).upper()
    alpha  = float(cfg.get("modeling", {}).get("blend_alpha_gbt", 0.65))

    train_df = spark.read.parquet(gold_path(f"{league}/training_matchups")).cache()

    # ── Entraînement des deux modèles ─────────────────────────────────────────
    hpo = load_hpo_params()
    lr_params  = (hpo.get("logreg", {}) or {}).get("params", {})
    gbt_params = (hpo.get("gbt",    {}) or {}).get("params", {})

    logger.info("Entraînement LR  (params=%s)", lr_params)
    lr_model = build_lr_pipeline(train_df, lr_params)

    logger.info("Entraînement GBT (params=%s)", gbt_params)
    gbt_model = build_gbt_pipeline(train_df, gbt_params)

    # ── Fichier de soumission ──────────────────────────────────────────────────
    sub_file = find_submission_file(Path(LOCAL_INPUT_DIR))
    if sub_file is None:
        logger.warning("Pas de fichier soumission dans %s — export ignoré.", LOCAL_INPUT_DIR)
        return

    sub = spark.read.option("header", True).csv(str(sub_file))
    parts_col = F.split(F.col("ID"), "_")
    matchups_sub = sub.select(
        "ID",
        parts_col.getItem(0).cast("int").alias("Season"),
        parts_col.getItem(1).cast("int").alias("Team1"),
        parts_col.getItem(2).cast("int").alias("Team2"),
    )

    # Features soumission : même pipeline que Gold
    team_stats   = spark.read.parquet(silver_path(f"{league}/team_season_stats"))
    elo          = spark.read.parquet(silver_path(f"{league}/elo_ratings"))
    rolling_last = spark.read.parquet(silver_path(f"{league}/rolling_last_per_season"))

    feats_sub = attach_team_features(matchups_sub, team_stats, elo, rolling_last)
    feats_sub = attach_optional_features(feats_sub, league, spark)

    # ── Blend des prédictions ─────────────────────────────────────────────────
    lr_scored  = lr_model.transform(feats_sub).select("ID",  F.col("probability").getItem(1).alias("p_lr"))
    gbt_scored = gbt_model.transform(feats_sub).select("ID", F.col("probability").getItem(1).alias("p_gbt"))

    blended = (
        lr_scored.join(gbt_scored, on="ID", how="inner")
        .withColumn("Pred", F.lit(alpha) * F.col("p_gbt") + F.lit(1.0 - alpha) * F.col("p_lr"))
        .select("ID", "Pred")
    )

    export_single_csv(blended, "/opt/project/artifacts/submission_ensemble_tmp", "/opt/project/artifacts/submission_ensemble.csv")


if __name__ == "__main__":
    main()
