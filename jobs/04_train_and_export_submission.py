"""
Job 04 — Entraînement baseline + export soumission Kaggle
══════════════════════════════════════════════════════════
Modèle : Logistic Regression (Spark MLlib)
Validation : split par saison (dernière saison = validation) pour éviter la fuite de données.

Fichiers de soumission attendus dans data/input/ (ordre de priorité) :
    MSampleSubmissionStage1.csv / MSampleSubmissionStage2.csv / sample_submission.csv

Format colonne ID : "2026_1101_1234"  (Season_Team1_Team2)

Sortie : artifacts/submission.csv

Usage :
    docker compose run --rm spark-submit python jobs/04_train_and_export_submission.py
"""

import glob
import os
import shutil
from pathlib import Path

from pyspark.sql import functions as F

from src.common.logging import get_logger
from src.common.paths import LOCAL_INPUT_DIR, gold_path, silver_path
from src.common.spark import build_spark
from src.features.basketball_features import build_matchup_features
from src.ml.modeling import build_pipeline, evaluate

logger = get_logger(__name__)

SUBMISSION_CANDIDATES = [
    "MSampleSubmissionStage1.csv",
    "MSampleSubmissionStage2.csv",
    "sample_submission.csv",
]


def find_submission_file(input_dir: Path) -> Path | None:
    """Cherche le premier fichier de soumission disponible dans input_dir."""
    for name in SUBMISSION_CANDIDATES:
        p = input_dir / name
        if p.exists():
            return p
    return None


def export_single_csv(df, tmp_dir: str, out_file: str) -> None:
    """Écrit un DataFrame en un seul fichier CSV (Kaggle n'accepte pas les multi-parts)."""
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(tmp_dir)
    parts = glob.glob(os.path.join(tmp_dir, "part-*.csv"))
    if not parts:
        raise RuntimeError(f"Aucun part-file trouvé dans {tmp_dir}")
    shutil.copy(parts[0], out_file)
    logger.info("Soumission exportée : %s", out_file)


def main() -> None:
    spark = build_spark("march-mania-04-train-export")

    # ── Split train/val par saison ─────────────────────────────────────────────
    train_df = spark.read.parquet(gold_path("training_matchups"))
    seasons = [r["Season"] for r in train_df.select("Season").distinct().collect()]
    val_season = max(seasons)

    train = train_df.filter(F.col("Season") < val_season)
    val   = train_df.filter(F.col("Season") == val_season)
    logger.info("Train=%d lignes | Val=%d lignes (saison val=%d)", train.count(), val.count(), val_season)

    # ── Entraînement ──────────────────────────────────────────────────────────
    model = build_pipeline().fit(train)
    if val.count() > 0:
        m = evaluate(model, val)
        logger.info("Validation → AUC=%.4f | LogLoss=%.5f", m["auc"], m["logloss"])

    # ── Export soumission ──────────────────────────────────────────────────────
    sub_file = find_submission_file(Path(LOCAL_INPUT_DIR))
    if sub_file is None:
        logger.warning("Aucun fichier de soumission trouvé dans %s. Export annulé.", LOCAL_INPUT_DIR)
        return

    sub = spark.read.option("header", True).csv(str(sub_file))
    parts_col = F.split(F.col("ID"), "_")
    matchups = sub.select(
        "ID",
        parts_col.getItem(0).cast("int").alias("Season"),
        parts_col.getItem(1).cast("int").alias("Team1"),
        parts_col.getItem(2).cast("int").alias("Team2"),
    )

    team_stats = spark.read.parquet(silver_path("team_season_stats"))
    feats = build_matchup_features(matchups, team_stats)
    scored = model.transform(feats).select("ID", F.col("probability").getItem(1).alias("Pred"))

    export_single_csv(scored, "/opt/project/artifacts/submission_tmp", "/opt/project/artifacts/submission.csv")


if __name__ == "__main__":
    main()
