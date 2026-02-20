"""Job 10 — Build Silver Strength of Schedule (SOS), league-aware.

Outputs:
- s3a://<bucket>/silver/march_mania/<league>/strength_of_schedule/

Run:
    docker compose run --rm spark-submit python jobs/10_build_silver_sos.py
"""

import yaml

from src.common.spark import build_spark
from src.common.paths import bronze_path, silver_path
from src.common.logging import get_logger
from src.features.rolling import build_long_game_table
from src.features.sos import build_sos

logger = get_logger(__name__)


def main() -> None:
    spark = build_spark("march-mania-10-silver-sos")

    cfg = yaml.safe_load(open("/opt/project/conf/pipeline.yml", "r", encoding="utf-8"))
    league = str(cfg.get("competition", {}).get("league", "M")).upper()
    if league not in {"M", "W"}:
        raise ValueError("competition.league must be 'M' or 'W'")

    bronze_ds = "m/regular_season/compact" if league == "M" else "w/regular_season/compact"
    regular = spark.read.parquet(bronze_path(bronze_ds))

    team_stats = spark.read.parquet(silver_path(f"{league}/team_season_stats"))
    elo = spark.read.parquet(silver_path(f"{league}/elo_ratings"))

    long_games = build_long_game_table(regular)
    sos = build_sos(long_games, team_stats, elo)

    out_path = silver_path(f"{league}/strength_of_schedule")
    logger.info("Writing Silver SOS: %s", out_path)
    sos.write.mode("overwrite").parquet(out_path)

    logger.info("Silver SOS complete. league=%s rows=%d", league, sos.count())


if __name__ == "__main__":
    main()
