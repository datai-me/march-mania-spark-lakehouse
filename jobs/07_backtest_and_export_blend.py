"""
Job 07 — Backtest rolling + export soumission expert
══════════════════════════════════════════════════════
Stratégie :
    Pour chaque saison S :
        - Entraîne sur toutes les saisons < S
        - Évalue sur la saison S
    Exporte les métriques dans artifacts/backtest_metrics.csv
    Entraîne un modèle final sur TOUTES les saisons
    Exporte la soumission dans artifacts/submission_expert.csv

Prérequis : jobs 01-06 + 08 (seeds) + 09 (massey, optionnel) + 10 (SOS, optionnel) + 03 (gold)

Config : conf/pipeline.yml → backtest.min_train_season / max_val_season

Usage :
    docker compose run --rm spark-submit python jobs/07_backtest_and_export_blend.py
"""

import csv
import glob
import os
import shutil
from pathlib import Path

import yaml
from pyspark.sql import functions as F

from jobs.feature_helpers import attach_optional_features
from src.common.logging import get_logger
from src.common.paths import LOCAL_INPUT_DIR, gold_path, silver_path
from src.common.spark import build_spark
from src.features.basketball_features_plus import attach_team_features
from src.ml.modeling import build_pipeline, evaluate

logger = get_logger(__name__)

SUBMISSION_CANDIDATES = [
    "SampleSubmissionStage1.csv", "SampleSubmissionStage2.csv",
    "MSampleSubmissionStage1.csv", "MSampleSubmissionStage2.csv",
    "WSampleSubmissionStage1.csv", "WSampleSubmissionStage2.csv",
    "sample_submission.csv",
]


def find_submission_file(input_dir: Path) -> Path | None:
    for name in SUBMISSION_CANDIDATES:
        p = input_dir / name
        if p.exists():
            return p
    return None


def export_single_csv(df, tmp_dir: str, out_file: str) -> None:
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(tmp_dir)
    parts = glob.glob(os.path.join(tmp_dir, "part-*.csv"))
    if not parts:
        raise RuntimeError(f"Aucun part-file trouvé dans {tmp_dir}")
    shutil.copy(parts[0], out_file)
    logger.info("Fichier exporté : %s", out_file)


def build_submission_features(spark, matchups_sub, league: str):
    """Reconstruit les features pour les matchups de soumission (même logique que Gold)."""
    team_stats   = spark.read.parquet(silver_path(f"{league}/team_season_stats"))
    elo          = spark.read.parquet(silver_path(f"{league}/elo_ratings"))
    rolling_last = spark.read.parquet(silver_path(f"{league}/rolling_last_per_season"))

    feats = attach_team_features(matchups_sub, team_stats, elo, rolling_last)
    return attach_optional_features(feats, league, spark)


def main() -> None:
    spark = build_spark("march-mania-07-expert")
    cfg = yaml.safe_load(open("/opt/project/conf/pipeline.yml", encoding="utf-8"))

    league = str(cfg.get("competition", {}).get("league", "M")).upper()
    if league not in {"M", "W"}:
        raise ValueError(f"competition.league doit être 'M' ou 'W', reçu : {league!r}")

    feats = spark.read.parquet(gold_path(f"{league}/training_matchups")).cache()
    seasons = sorted(r["Season"] for r in feats.select("Season").distinct().collect())

    bt = cfg.get("backtest", {})
    min_train = int(bt.get("min_train_season", seasons[0]))
    max_val   = int(bt.get("max_val_season",   seasons[-1]))

    # ── Backtest rolling ──────────────────────────────────────────────────────
    metrics_rows = [("val_season", "train_rows", "val_rows", "auc", "logloss")]
    for s in seasons:
        if not (min_train <= s <= max_val):
            continue
        train = feats.filter(F.col("Season") < s)
        val   = feats.filter(F.col("Season") == s)
        if train.count() == 0 or val.count() == 0:
            continue
        m = evaluate(build_pipeline().fit(train), val)
        metrics_rows.append((str(s), str(train.count()), str(val.count()), f"{m['auc']:.4f}", f"{m['logloss']:.5f}"))
        logger.info("Backtest saison=%d AUC=%.4f LogLoss=%.5f", s, m["auc"], m["logloss"])

    out_metrics = "/opt/project/artifacts/backtest_metrics.csv"
    with open(out_metrics, "w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerows(metrics_rows)
    logger.info("Métriques backtest : %s", out_metrics)

    # ── Modèle final sur tout le dataset ──────────────────────────────────────
    final_model = build_pipeline().fit(feats)

    # ── Export soumission ──────────────────────────────────────────────────────
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

    feats_sub = build_submission_features(spark, matchups_sub, league)
    scored = final_model.transform(feats_sub).select("ID", F.col("probability").getItem(1).alias("Pred"))
    export_single_csv(scored, "/opt/project/artifacts/submission_expert_tmp", "/opt/project/artifacts/submission_expert.csv")


if __name__ == "__main__":
    main()
