"""ML utilities for training and inference using Spark MLlib.

This module keeps ML logic reusable and testable.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator


FEATURE_COLS = [
    "WinRateDiff",
    "AvgPointDiffDiff",
    "EloDiff",
    "RollWinRateDiff",
    "RollAvgPointDiffDiff",
    "SeedDiff",
    "MasseyDiff",
    "SOSWinRateDiff",
    "SOSEloDiff",
]


def build_pipeline() -> Pipeline:
    """Create a simple, strong baseline model pipeline.

    Logistic Regression is often a great baseline for Kaggle probability tasks
    (and is easy to interpret + fast).

    Returns:
        Spark ML Pipeline
    """
    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features")
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


def evaluate(model, df: DataFrame) -> dict:
    """Evaluate the model on a labeled dataset.

    We report:
    - AUC (useful diagnostic)
    - LogLoss approximation via manual computation (closer to Kaggle)
      Note: Kaggle logloss uses predicted probability of the true class.

    Args:
        model: fitted PipelineModel
        df: labeled dataframe with 'label' and feature columns

    Returns:
        dict with metrics
    """
    scored = model.transform(df).select("label", "probability")
    # probability is a vector [p0, p1]
    scored = scored.withColumn("p1", F.col("probability").getItem(1).cast("double"))

    # AUC
    auc = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="p1", metricName="areaUnderROC")         .evaluate(scored.select("label", "p1").withColumnRenamed("p1", "rawPrediction"))

    # LogLoss (stable clipping)
    eps = 1e-15
    scored = scored.withColumn("p1c", F.when(F.col("p1") < eps, eps)
                               .when(F.col("p1") > 1 - eps, 1 - eps)
                               .otherwise(F.col("p1")))
    logloss = scored.select(
        (- (F.col("label") * F.log(F.col("p1c")) + (1 - F.col("label")) * F.log(1 - F.col("p1c")))).alias("ll")
    ).agg(F.avg("ll").alias("logloss")).collect()[0]["logloss"]

    return {"auc": float(auc), "logloss": float(logloss)}
