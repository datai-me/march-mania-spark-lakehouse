"""
Job 11 — HPO : optimisation des hyperparamètres
═════════════════════════════════════════════════
Tune Logistic Regression et GBT via TrainValidationSplit.
Utilise la dernière saison comme holdout pour évaluer les modèles.

Sortie : artifacts/hpo_best_params.json
    → Relu par le job 12 pour initialiser les modèles de l'ensemble.

Usage :
    docker compose run --rm spark-submit python jobs/11_hpo_backtest.py
"""

import json

import yaml
from pyspark.sql import functions as F

from src.common.logging import get_logger
from src.common.paths import gold_path
from src.common.spark import build_spark
from src.ml.modeling import FEATURE_COLS, evaluate
from src.ml.tuning import tune_gbt, tune_logreg

logger = get_logger(__name__)


def main() -> None:
    spark = build_spark("march-mania-11-hpo")
    cfg = yaml.safe_load(open("/opt/project/conf/pipeline.yml", encoding="utf-8"))
    league = str(cfg.get("competition", {}).get("league", "M")).upper()

    df = spark.read.parquet(gold_path(f"{league}/training_matchups")).cache()
    val_season = max(r["Season"] for r in df.select("Season").distinct().collect())

    train = df.filter(F.col("Season") < val_season)
    val   = df.filter(F.col("Season") == val_season)
    logger.info("HPO — train=%d val=%d (val_season=%d)", train.count(), val.count(), val_season)

    # Tuning des deux modèles
    lr_model, lr_params  = tune_logreg(train, FEATURE_COLS)
    gbt_model, gbt_params = tune_gbt(train, FEATURE_COLS)

    # Évaluation sur le holdout
    lr_metrics  = evaluate(lr_model, val)
    gbt_metrics = evaluate(gbt_model, val)

    result = {
        "league":       league,
        "val_season":   int(val_season),
        "feature_cols": FEATURE_COLS,
        "logreg":       {"params": lr_params,  "metrics": lr_metrics},
        "gbt":          {"params": gbt_params, "metrics": gbt_metrics},
    }

    out_path = "/opt/project/artifacts/hpo_best_params.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)

    logger.info("HPO terminé — LR AUC=%.4f | GBT AUC=%.4f", lr_metrics["auc"], gbt_metrics["auc"])
    logger.info("Résultats sauvegardés : %s", out_path)


if __name__ == "__main__":
    main()
