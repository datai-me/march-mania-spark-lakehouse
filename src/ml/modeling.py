"""
modeling.py — Pipeline ML et évaluation (Spark MLlib)
═══════════════════════════════════════════════════════
Logistic Regression baseline + évaluation AUC / LogLoss.

Ce module est le cœur de la couche ML :
    FEATURE_COLS    : liste canonique des features (partagée avec tuning.py)
    build_pipeline  : pipeline VectorAssembler + LogisticRegression
    evaluate        : AUC + LogLoss sur un DataFrame labellisé

Notes sur les métriques :
    - AUC  : diagnostic rapide, non pénalisé par la calibration
    - LogLoss : métrique Kaggle principale — pénalise fortement les prédictions
      trop confiantes et incorrectes ; la calibration du modèle est cruciale.

Note sur le bug AUC corrigé :
    L'original passait une colonne `double` (p1) comme rawPrediction dans
    BinaryClassificationEvaluator, qui attend un vecteur Dense ou une valeur
    scalaire via rawPredictionCol. Ici on utilise directement `probability`
    (vecteur [p0, p1]) avec le bon metricName pour éviter toute ambiguïté.
"""

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler

# ── Features canoniques ───────────────────────────────────────────────────────
# Ordre fixe pour reproductibilité — ne pas modifier sans mettre à jour tuning.py.
# Les features null (seeds absents, massey absent…) sont tolérées par VectorAssembler
# grâce à handleInvalid="keep" configuré dans build_pipeline.
FEATURE_COLS: list[str] = [
    # Features saison (Tier 1 — fort signal)
    "WinRateDiff",
    "AvgPointDiffDiff",
    "EloDiff",
    # Rolling momentum (Tier 1)
    "RollWinRateDiff",
    "RollAvgPointDiffDiff",
    # Seeds (Tier 1 — meilleur signal pour le tournoi)
    "SeedDiff",
    # Massey rankings (Tier 2 — Hommes uniquement, null pour Femmes)
    "MasseyDiff",
    # Strength of Schedule (Tier 2)
    "SOSWinRateDiff",
    "SOSEloDiff",
]


def build_pipeline() -> Pipeline:
    """
    Crée le pipeline ML baseline : VectorAssembler → LogisticRegression.

    Choix de la Logistic Regression :
        - Excellente baseline pour les tâches de probabilité (Kaggle)
        - Rapide à entraîner, interprétable (coefficients)
        - Bien calibrée → bon LogLoss de base

    handleInvalid="keep" dans VectorAssembler :
        → les features null sont remplacées par 0.0 (équipes sans seed, etc.)
        → évite le crash si une feature optionnelle est absente

    Returns:
        Pipeline Spark ML non entraîné.
    """
    assembler = VectorAssembler(
        inputCols=FEATURE_COLS,
        outputCol="features",
        handleInvalid="keep",   # null → 0.0 (équipes sans seed, massey, etc.)
    )
    lr = LogisticRegression(
        featuresCol="features",
        labelCol="label",
        predictionCol="prediction",
        probabilityCol="probability",
        maxIter=50,
        regParam=0.05,
        elasticNetParam=0.0,
    )
    return Pipeline(stages=[assembler, lr])


# Clipping pour éviter log(0) dans le calcul du LogLoss
_EPS = 1e-15


def evaluate(model, df: DataFrame) -> dict[str, float]:
    """
    Évalue un modèle entraîné sur un DataFrame labellisé.

    Métriques retournées :
        auc     : Area Under ROC Curve (0.5 = aléatoire, 1.0 = parfait)
        logloss : Cross-Entropy Loss (métrique Kaggle, plus bas = mieux)

    Args:
        model : PipelineModel Spark ML entraîné (sortie de pipeline.fit())
        df    : DataFrame avec colonnes features + 'label' (0 ou 1)

    Returns:
        {"auc": float, "logloss": float}

    Note sur l'AUC :
        BinaryClassificationEvaluator attend rawPredictionCol (vecteur de scores).
        On utilise la colonne 'rawPrediction' générée par LogisticRegression,
        qui est un vecteur [score_0, score_1] — correct pour areaUnderROC.
    """
    scored = model.transform(df)

    # ── AUC ───────────────────────────────────────────────────────────────────
    # rawPrediction = vecteur de logits [logit_0, logit_1] — format attendu
    auc_evaluator = BinaryClassificationEvaluator(
        labelCol="label",
        rawPredictionCol="rawPrediction",   # BUG FIX : pas "p1" (double), mais le vecteur
        metricName="areaUnderROC",
    )
    auc = float(auc_evaluator.evaluate(scored))

    # ── LogLoss ───────────────────────────────────────────────────────────────
    # p1 = probabilité prédite que label=1
    # Clipping [eps, 1-eps] pour éviter log(0) → NaN
    logloss = (
        scored
        .withColumn("p1", F.col("probability").getItem(1).cast("double"))
        .withColumn("p1c", F.least(F.greatest(F.col("p1"), F.lit(_EPS)), F.lit(1.0 - _EPS)))
        .select(
            (
                -(F.col("label") * F.log(F.col("p1c"))
                  + (1 - F.col("label")) * F.log(1 - F.col("p1c")))
            ).alias("ll")
        )
        .agg(F.avg("ll").alias("logloss"))
        .collect()[0]["logloss"]
    )

    return {"auc": auc, "logloss": float(logloss)}
