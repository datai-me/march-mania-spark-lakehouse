"""Job 05 — Compute season-scoped ELO ratings (Silver).

Inputs:
- Bronze regular season compact results

Outputs:
- s3a://<bucket>/silver/march_mania/elo_ratings/

Run:
    docker compose run --rm spark-submit python jobs/05_build_silver_elo.py
"""

import yaml

from src.common.spark import build_spark
from src.common.paths import bronze_path, silver_path
from src.common.logging import get_logger
from src.features.elo import build_elo_per_season

logger = get_logger(__name__)


def main() -> None:
    spark = build_spark("march-mania-05-silver-elo")

    cfg = yaml.safe_load(open("/opt/project/conf/pipeline.yml", "r", encoding="utf-8"))

    regular = spark.read.parquet(bronze_path("regular_season_compact_results"))

    elo_cfg = cfg.get("elo", {})
    elo = build_elo_per_season(
        regular,
        initial_rating=float(elo_cfg.get("initial_rating", 1500.0)),
        k_factor=float(elo_cfg.get("k_factor", 20.0)),
    )

    out_path = silver_path("elo_ratings")
    logger.info("Writing Silver ELO ratings: %s", out_path)
    elo.write.mode("overwrite").parquet(out_path)

    logger.info("Silver ELO complete. Rows=%d", elo.count())


if __name__ == "__main__":
    main()
