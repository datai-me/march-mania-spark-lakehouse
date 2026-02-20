"""Job 01 — Ingest Kaggle CSV files into the Bronze layer (Parquet) in MinIO.

Why:
- CSV is slow and expensive to read repeatedly.
- Parquet is columnar, compressed, and Spark-friendly.
- Bronze is immutable: if you need to re-run, overwrite the Bronze dataset for that competition run.

Input:
- CSV files located under /opt/project/data/input (mounted from ./data/input)

Output:
- s3a://<bucket>/bronze/march_mania/<dataset_name>/

Run:
    docker compose run --rm spark-submit python jobs/01_ingest_bronze.py
"""

from pathlib import Path

from pyspark.sql import functions as F

from src.common.spark import build_spark
from src.common.paths import LOCAL_INPUT_DIR, bronze_path
from src.common.logging import get_logger

logger = get_logger(__name__)


# A minimal mapping of expected Kaggle file names to logical dataset names.
# Add/rename to match the actual files you downloaded for the competition.
DATASETS = {
    "MRegularSeasonCompactResults.csv": "regular_season_compact_results",
    "MTeams.csv": "teams",
    # Add more Kaggle files here as needed:
    # "MNCAATourneyCompactResults.csv": "tourney_compact_results",
    # "MSeasons.csv": "seasons",
}


def main() -> None:
    spark = build_spark("march-mania-01-bronze")

    input_dir = Path(LOCAL_INPUT_DIR)

    for filename, dataset_name in DATASETS.items():
        file_path = input_dir / filename
        if not file_path.exists():
            logger.warning("Missing input file: %s (skip)", file_path)
            continue

        logger.info("Reading CSV: %s", file_path)

        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(str(file_path))
        )

        # Optional: basic cleanup — trim string columns to avoid weird trailing spaces.
        for c, t in df.dtypes:
            if t == "string":
                df = df.withColumn(c, F.trim(F.col(c)))

        out_path = bronze_path(dataset_name)
        logger.info("Writing Bronze Parquet: %s", out_path)

        df.write.mode("overwrite").parquet(out_path)

    logger.info("Bronze ingestion complete.")


if __name__ == "__main__":
    main()
