"""Job 01 — Ingest **all** Kaggle CSV files into Bronze (Parquet) in MinIO.

This is enterprise-style ingestion:
- It scans `data/input/` for `*.csv`
- Uses a dataset registry to map filenames → lake paths (men/women/reference/etc.)
- Unknown CSVs are still ingested under `bronze/.../misc/<filename_stem>/`

Why:
- Kaggle March Mania packs include many CSVs (20+). This job ingests them all in one run.
- Parquet is faster and cheaper for repeated reads.
- Bronze is treated as raw (minimal transforms).

Run:
    docker compose run --rm spark-submit python jobs/01_ingest_bronze.py
"""

from pathlib import Path

from pyspark.sql import functions as F

from src.common.spark import build_spark
from src.common.paths import LOCAL_INPUT_DIR, bronze_path, bronze_misc_path
from src.common.datasets import spec_for_filename
from src.common.logging import get_logger

logger = get_logger(__name__)


def _safe_stem(name: str) -> str:
    return name.replace(".csv", "").replace(" ", "_").lower()


def main() -> None:
    spark = build_spark("march-mania-01-bronze")

    input_dir = Path(LOCAL_INPUT_DIR)
    csv_files = sorted(input_dir.glob("*.csv"))

    if not csv_files:
        logger.warning("No CSV files found under %s. Put Kaggle files into data/input/.", input_dir)
        return

    for file_path in csv_files:
        filename = file_path.name
        spec = spec_for_filename(filename)

        logger.info("Reading CSV: %s", file_path)

        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(str(file_path))
        )

        # Trim string columns
        for c, t in df.dtypes:
            if t == "string":
                df = df.withColumn(c, F.trim(F.col(c)))

        if spec:
            out_path = f"{bronze_path(spec.lake_subpath)}"
        else:
            out_path = bronze_misc_path(_safe_stem(filename))
            logger.warning("Unknown CSV '%s' → ingesting to %s", filename, out_path)

        logger.info("Writing Bronze Parquet: %s", out_path)
        df.write.mode("overwrite").parquet(out_path)

    logger.info("Bronze ingestion complete. Files ingested=%d", len(csv_files))


if __name__ == "__main__":
    main()
