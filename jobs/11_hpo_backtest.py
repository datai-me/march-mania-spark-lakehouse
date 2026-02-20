"""Job 11 — Hyperparameter tuning (HPO) + validation report.

Purpose:
- Tune Logistic Regression and GBT models quickly (TrainValidationSplit)
- Evaluate on a season-based holdout (latest season)
- Save best params to `artifacts/hpo_best_params.json`

Run:
  docker compose run --rm spark-submit python jobs/11_hpo_backtest.py
"""

import json
import yaml
from pyspark.sql import functions as F

from src.common.spark import build_spark
from src.common.paths import gold_path
from src.common.logging import get_logger
from src.ml.modeling import FEATURE_COLS, evaluate
from src.ml.tuning import tune_logreg, tune_gbt

logger = get_logger(__name__)


def main() -> None:
    spark = build_spark("march-mania-11-hpo")

    cfg = yaml.safe_load(open("/opt/project/conf/pipeline.yml", "r", encoding="utf-8"))
    league = str(cfg.get("competition", {}).get("league", "M")).upper()

    df = spark.read.parquet(gold_path(f"{league}/training_matchups")).cache()
    seasons = [r["Season"] for r in df.select("Season").distinct().collect()]
    val_season = max(seasons)

    train = df.filter(F.col("Season") < F.lit(val_season))
    val = df.filter(F.col("Season") == F.lit(val_season))

    logger.info("HPO split: train=%d val=%d (val_season=%d)", train.count(), val.count(), val_season)

    # Tune LR and GBT on training set
    lr_model, lr_params = tune_logreg(train, FEATURE_COLS)
    gbt_model, gbt_params = tune_gbt(train, FEATURE_COLS)

    # Evaluate on holdout season
    lr_metrics = evaluate(lr_model, val)
    gbt_metrics = evaluate(gbt_model, val)

    out = {
        "league": league,
        "val_season": int(val_season),
        "feature_cols": FEATURE_COLS,
        "logreg": {"params": lr_params, "metrics": lr_metrics},
        "gbt": {"params": gbt_params, "metrics": gbt_metrics},
    }

    out_path = "/opt/project/artifacts/hpo_best_params.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)

    logger.info("Saved HPO results: %s", out_path)
    logger.info("LR   AUC=%.4f LogLoss=%.5f", lr_metrics["auc"], lr_metrics["logloss"])
    logger.info("GBT  AUC=%.4f LogLoss=%.5f", gbt_metrics["auc"], gbt_metrics["logloss"])


if __name__ == "__main__":
    main()
