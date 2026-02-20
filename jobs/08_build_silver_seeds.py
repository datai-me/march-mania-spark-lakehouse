"""Job 08 — Build Silver tournament seed features (league-aware).

Outputs:
- s3a://<bucket>/silver/march_mania/<league>/tourney_seeds_parsed/

Run:
    docker compose run --rm spark-submit python jobs/08_build_silver_seeds.py
"""

import yaml

from src.common.spark import build_spark
from src.common.paths import bronze_path, silver_path
from src.common.logging import get_logger
from src.features.seeds import build_seeds

logger = get_logger(__name__)


def main() -> None:
    spark = build_spark("march-mania-08-silver-seeds")

    cfg = yaml.safe_load(open("/opt/project/conf/pipeline.yml", "r", encoding="utf-8"))
    league = str(cfg.get("competition", {}).get("league", "M")).upper()
    if league not in {"M", "W"}:
        raise ValueError("competition.league must be 'M' or 'W'")

    bronze_ds = "m/tournament/seeds" if league == "M" else "w/tournament/seeds"
    seeds = spark.read.parquet(bronze_path(bronze_ds))

    parsed = build_seeds(seeds)

    out_path = silver_path(f"{league}/tourney_seeds_parsed")
    logger.info("Writing Silver parsed seeds: %s", out_path)
    parsed.write.mode("overwrite").parquet(out_path)

    logger.info("Silver seeds complete. league=%s rows=%d", league, parsed.count())


if __name__ == "__main__":
    main()
