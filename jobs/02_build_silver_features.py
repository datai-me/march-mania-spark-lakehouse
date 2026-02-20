"""Job 02 — Build Silver features (team-season stats) from Bronze datasets.

Silver contains cleaned and enriched datasets ready for reuse across analytics and ML.

Inputs:
- Bronze regular season compact results

Outputs:
- s3a://<bucket>/silver/march_mania/team_season_stats/

Run:
    docker compose run --rm spark-submit python jobs/02_build_silver_features.py
"""

from src.common.spark import build_spark
from src.common.paths import bronze_path, silver_path
from src.common.logging import get_logger
from src.features.basketball_features import build_team_season_stats

logger = get_logger(__name__)


def main() -> None:
    spark = build_spark("march-mania-02-silver")

    regular = spark.read.parquet(bronze_path("regular_season_compact_results"))

    # Build explainable aggregates (good baseline)
    team_stats = build_team_season_stats(regular)

    out_path = silver_path("team_season_stats")
    logger.info("Writing Silver team-season stats: %s", out_path)
    team_stats.write.mode("overwrite").parquet(out_path)

    logger.info("Silver features complete.")


if __name__ == "__main__":
    main()
