"""Job 02 — Build Silver features (team-season stats) from Bronze datasets.

Silver contains cleaned and enriched datasets ready for reuse across analytics and ML.

This job supports both Men's (M) and Women's (W) leagues via `conf/pipeline.yml`:
- competition.league: "M" or "W"

Outputs:
- s3a://<bucket>/silver/march_mania/<league>/team_season_stats/

Run:
    docker compose run --rm spark-submit python jobs/02_build_silver_features.py
"""

import yaml

from src.common.spark import build_spark
from src.common.paths import bronze_path, silver_path
from src.common.logging import get_logger
from src.features.basketball_features import build_team_season_stats

logger = get_logger(__name__)


def main() -> None:
    spark = build_spark("march-mania-02-silver")

    cfg = yaml.safe_load(open("/opt/project/conf/pipeline.yml", "r", encoding="utf-8"))
    league = str(cfg.get("competition", {}).get("league", "M")).upper()
    if league not in {"M", "W"}:
        raise ValueError("competition.league must be 'M' or 'W'")

    bronze_ds = "m/regular_season/compact" if league == "M" else "w/regular_season/compact"
    regular = spark.read.parquet(bronze_path(bronze_ds))

    team_stats = build_team_season_stats(regular)

    out_path = silver_path(f"{league}/team_season_stats")
    logger.info("Writing Silver team-season stats: %s", out_path)
    team_stats.write.mode("overwrite").parquet(out_path)

    logger.info("Silver features complete (league=%s).", league)


if __name__ == "__main__":
    main()
