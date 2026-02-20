"""Hyperparameter tuning utilities (Spark MLlib).

We use TrainValidationSplit (faster than full CrossValidator) because:
- Local docker demo should finish in reasonable time.
- Kaggle datasets are moderate-sized.

Outputs are saved under artifacts/ as JSON.
"""

from __future__ import annotations

from typing import Dict, Tuple

from pyspark.sql import DataFrame
from pyspark.ml import Pipeline
from pyspark.ml.tuning import TrainValidationSplit, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.classification import LogisticRegression, GBTClassifier
from pyspark.ml.feature import VectorAssembler


def tune_logreg(df: DataFrame, feature_cols: list[str]) -> Tuple[object, Dict]:
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    lr = LogisticRegression(featuresCol="features", labelCol="label", probabilityCol="probability", maxIter=60)

    pipe = Pipeline(stages=[assembler, lr])

    grid = (ParamGridBuilder()
            .addGrid(lr.regParam, [0.0, 0.02, 0.05, 0.1])
            .addGrid(lr.elasticNetParam, [0.0, 0.5])
            .build())

    evaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
    tvs = TrainValidationSplit(estimator=pipe, estimatorParamMaps=grid, evaluator=evaluator, trainRatio=0.8, parallelism=2)
    model = tvs.fit(df)

    best = model.bestModel
    best_lr = best.stages[-1]
    params = {"regParam": float(best_lr.getRegParam()), "elasticNetParam": float(best_lr.getElasticNetParam())}
    return best, params


def tune_gbt(df: DataFrame, feature_cols: list[str]) -> Tuple[object, Dict]:
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    gbt = GBTClassifier(featuresCol="features", labelCol="label", maxIter=120, seed=42)

    pipe = Pipeline(stages=[assembler, gbt])

    grid = (ParamGridBuilder()
            .addGrid(gbt.maxDepth, [3, 5])
            .addGrid(gbt.maxIter, [80, 120])
            .addGrid(gbt.subsamplingRate, [0.7, 0.9])
            .build())

    evaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
    tvs = TrainValidationSplit(estimator=pipe, estimatorParamMaps=grid, evaluator=evaluator, trainRatio=0.8, parallelism=2)
    model = tvs.fit(df)

    best = model.bestModel
    best_gbt = best.stages[-1]
    params = {
        "maxDepth": int(best_gbt.getMaxDepth()),
        "maxIter": int(best_gbt.getMaxIter()),
        "subsamplingRate": float(best_gbt.getSubsamplingRate()),
    }
    return best, params
