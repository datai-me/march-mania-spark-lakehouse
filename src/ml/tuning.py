"""
tuning.py — Hyperparameter Optimization (HPO) via TrainValidationSplit
═══════════════════════════════════════════════════════════════════════
Tune Logistic Regression et GBT indépendamment avec des grilles de paramètres.

Pourquoi TrainValidationSplit plutôt que CrossValidator ?
    - CV k-fold est k× plus lent (k entraînements par combinaison de paramètres)
    - Pour nos datasets (quelques centaines de milliers de lignes), TVS 80/20
      donne des résultats stables à une fraction du coût
    - parallelism=2 : entraîne 2 combinaisons en parallèle (adapté à un worker)

Les meilleurs paramètres sont retournés sous forme de dict pour être
sérialisés en JSON et rechargés par le job 12.
"""

from __future__ import annotations

from typing import Dict, Tuple

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import GBTClassifier, LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
from pyspark.sql import DataFrame

# Évaluateur partagé entre LR et GBT (AUC sur rawPrediction vecteur)
_EVALUATOR = BinaryClassificationEvaluator(
    labelCol="label",
    rawPredictionCol="rawPrediction",
    metricName="areaUnderROC",
)


def _make_assembler(feature_cols: list[str]) -> VectorAssembler:
    """VectorAssembler commun — handleInvalid pour tolérer les nulls optionnels."""
    return VectorAssembler(
        inputCols=feature_cols,
        outputCol="features",
        handleInvalid="keep",
    )


def tune_logreg(
    df: DataFrame,
    feature_cols: list[str],
) -> Tuple[PipelineModel, Dict[str, float]]:
    """
    Optimise les hyperparamètres de la Logistic Regression.

    Grille testée (4 × 2 = 8 combinaisons) :
        regParam       : [0.0, 0.02, 0.05, 0.1]   → force de la régularisation L2
        elasticNetParam: [0.0, 0.5]               → 0.0 = L2 pure, 0.5 = mix L1/L2

    Args:
        df           : DataFrame d'entraînement avec features + label
        feature_cols : liste des colonnes de features (= FEATURE_COLS de modeling.py)

    Returns:
        (meilleur_modèle, dict_des_meilleurs_paramètres)
    """
    assembler = _make_assembler(feature_cols)
    lr = LogisticRegression(
        featuresCol="features",
        labelCol="label",
        probabilityCol="probability",
        maxIter=60,
    )
    pipe = Pipeline(stages=[assembler, lr])

    grid = (
        ParamGridBuilder()
        .addGrid(lr.regParam,        [0.0, 0.02, 0.05, 0.1])
        .addGrid(lr.elasticNetParam, [0.0, 0.5])
        .build()
    )

    tvs = TrainValidationSplit(
        estimator=pipe,
        estimatorParamMaps=grid,
        evaluator=_EVALUATOR,
        trainRatio=0.8,
        parallelism=2,
    )
    model = tvs.fit(df)

    best_lr = model.bestModel.stages[-1]
    best_params = {
        "regParam":        float(best_lr.getRegParam()),
        "elasticNetParam": float(best_lr.getElasticNetParam()),
        "maxIter":         int(best_lr.getMaxIter()),
    }
    return model.bestModel, best_params


def tune_gbt(
    df: DataFrame,
    feature_cols: list[str],
) -> Tuple[PipelineModel, Dict]:
    """
    Optimise les hyperparamètres du Gradient Boosted Trees classifier.

    Grille testée (2 × 2 × 2 = 8 combinaisons) :
        maxDepth        : [3, 5]        → profondeur max des arbres
        maxIter         : [80, 120]     → nombre d'arbres dans l'ensemble
        subsamplingRate : [0.7, 0.9]    → fraction des données par arbre (bagging)

    Note : GBT est plus lent que LR — la grille est volontairement petite
    pour rester raisonnable sur un cluster local.

    Args:
        df           : DataFrame d'entraînement avec features + label
        feature_cols : liste des colonnes de features

    Returns:
        (meilleur_modèle, dict_des_meilleurs_paramètres)
    """
    assembler = _make_assembler(feature_cols)
    gbt = GBTClassifier(
        featuresCol="features",
        labelCol="label",
        maxIter=120,
        seed=42,            # reproductibilité
    )
    pipe = Pipeline(stages=[assembler, gbt])

    grid = (
        ParamGridBuilder()
        .addGrid(gbt.maxDepth,        [3, 5])
        .addGrid(gbt.maxIter,         [80, 120])
        .addGrid(gbt.subsamplingRate, [0.7, 0.9])
        .build()
    )

    tvs = TrainValidationSplit(
        estimator=pipe,
        estimatorParamMaps=grid,
        evaluator=_EVALUATOR,
        trainRatio=0.8,
        parallelism=2,
    )
    model = tvs.fit(df)

    best_gbt = model.bestModel.stages[-1]
    best_params = {
        "maxDepth":        int(best_gbt.getMaxDepth()),
        "maxIter":         int(best_gbt.getMaxIter()),
        "subsamplingRate": float(best_gbt.getSubsamplingRate()),
    }
    return model.bestModel, best_params
