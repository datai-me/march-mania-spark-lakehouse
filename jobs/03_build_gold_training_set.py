"""
Job 03 — Gold : dataset d'entraînement ML
══════════════════════════════════════════
Assemble le dataset final pour le ML à partir des couches Silver :
- Matchups étiquetés (victoire = 1 / défaite = 0) depuis la saison régulière
- Deltas de features : WinRate, PointDiff, Elo, Rolling, Seed, Massey, SOS

Les features optionnelles (Seed, Massey, SOS) sont mises à null si les jobs
amont n'ont pas encore tourné — pas de crash, juste un avertissement.

Config : conf/pipeline.yml → competition.league (M ou W)

Sortie : gold/march_mania/<league>/training_matchups/

Usage :
    docker compose run --rm spark-submit python jobs/03_build_gold_training_set.py
"""

import yaml
from pyspark.sql import functions as F

from jobs.feature_helpers import attach_optional_features  # helper partagé
from src.common.logging import get_logger
from src.common.paths import bronze_path, gold_path, silver_path
from src.common.spark import build_spark
from src.features.basketball_features_plus import attach_team_features

logger = get_logger(__name__)


def _load_league(cfg_path: str = "/opt/project/conf/pipeline.yml") -> str:
    cfg = yaml.safe_load(open(cfg_path, encoding="utf-8"))
    league = str(cfg.get("competition", {}).get("league", "M")).upper()
    if league not in {"M", "W"}:
        raise ValueError(f"competition.league doit être 'M' ou 'W', reçu : {league!r}")
    return league


def build_labeled_matchups(regular):
    """
    Crée un DataFrame de matchups avec étiquette binaire.
    Chaque match génère 2 lignes : vainqueur (label=1) et perdant (label=0).
    Cette symétrie aide le modèle à ne pas biaiser sur l'ordre des équipes.
    """
    winners = (
        regular.select(
            F.col("Season").cast("int"),
            F.col("WTeamID").cast("int").alias("Team1"),
            F.col("LTeamID").cast("int").alias("Team2"),
        ).withColumn("label", F.lit(1))
    )
    losers = (
        regular.select(
            F.col("Season").cast("int"),
            F.col("LTeamID").cast("int").alias("Team1"),
            F.col("WTeamID").cast("int").alias("Team2"),
        ).withColumn("label", F.lit(0))
    )
    # BUG FIX : la version originale utilisait 'l' (undefined) au lieu de 'losers'
    return winners.unionByName(losers)


def main() -> None:
    spark = build_spark("march-mania-03-gold")
    league = _load_league()

    # ── Lecture des Silver requis ──────────────────────────────────────────────
    bronze_ds = "m/regular_season/compact" if league == "M" else "w/regular_season/compact"
    regular     = spark.read.parquet(bronze_path(bronze_ds))
    team_stats  = spark.read.parquet(silver_path(f"{league}/team_season_stats"))
    elo         = spark.read.parquet(silver_path(f"{league}/elo_ratings"))
    rolling_last = spark.read.parquet(silver_path(f"{league}/rolling_last_per_season"))

    # ── Construction du dataset ────────────────────────────────────────────────
    matchups = build_labeled_matchups(regular)
    feats = attach_team_features(matchups, team_stats, elo, rolling_last)

    # Features optionnelles (Seeds, Massey, SOS) via helper centralisé
    feats = attach_optional_features(feats, league, spark)

    # Supprimer les lignes sans features essentielles (équipes sans stats Silver)
    feats = feats.dropna(subset=["WinRateDiff", "AvgPointDiffDiff", "EloDiff"])

    out_path = gold_path(f"{league}/training_matchups")
    logger.info("Écriture Gold dataset : %s", out_path)
    feats.write.mode("overwrite").parquet(out_path)

    logger.info("Gold terminé — league=%s lignes=%d", league, feats.count())


if __name__ == "__main__":
    main()
