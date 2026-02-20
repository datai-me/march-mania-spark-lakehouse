"""Job 09 — Build Silver Massey consensus rankings (Men only).

If the file is not present in Bronze, the job logs a warning and exits.

Outputs:
- s3a://<bucket>/silver/march_mania/M/massey_consensus/

Run:
    docker compose run --rm spark-submit python jobs/09_build_silver_massey.py
"""

from pyspark.sql.utils import AnalysisException

from src.common.spark import build_spark
from src.common.paths import bronze_path, silver_path
from src.common.logging import get_logger
from src.features.massey import build_massey_consensus

logger = get_logger(__name__)


def main() -> None:
    spark = build_spark("march-mania-09-silver-massey")

    try:
        massey = spark.read.parquet(bronze_path("m/rankings/massey_ordinals"))
    except Exception as e:
        logger.warning("Massey ordinals not found in Bronze (did you ingest MMasseyOrdinals.csv?). Skip. err=%s", e)
        return

    consensus = build_massey_consensus(massey)

    out_path = silver_path("M/massey_consensus")
    logger.info("Writing Silver Massey consensus: %s", out_path)
    consensus.write.mode("overwrite").parquet(out_path)

    logger.info("Silver Massey complete. rows=%d", consensus.count())


if __name__ == "__main__":
    main()
