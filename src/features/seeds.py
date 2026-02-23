"""
seeds.py — Parsing des têtes de série du tournoi NCAA
═══════════════════════════════════════════════════════
Les seeds Kaggle sont des chaînes comme "W01", "X16a", "Z16b".

Format : <Région><Numéro>[Lettre play-in optionnelle]
    Région : W, X, Y, Z  (4 régions du bracket NCAA)
    Numéro : 01–16 (1 = meilleure tête de série)
    Lettre : 'a' ou 'b' pour les matchs de play-in (optionnel)

On extrait :
    SeedRegion : W/X/Y/Z
    SeedNum    : entier 1–16 (signal fort pour prédire les matchups)
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Regex : capture (lettre région) + (2 chiffres du numéro)
# Le suffixe play-in optionnel ('a'/'b') est ignoré volontairement.
_SEED_PATTERN = r"^([A-Z])(\d{2})"


def build_seeds(seeds_df: DataFrame) -> DataFrame:
    """
    Parse les seeds du tournoi NCAA en features numériques.

    Entrée (MNCAATourneySeeds.csv ou WNCAATourneySeeds.csv) :
        Season, TeamID, Seed

    Sortie :
        Season:int, TeamID:int, Seed:str,
        SeedRegion:str  ← W / X / Y / Z
        SeedNum:int     ← 1 à 16 (feature principale)

    Note : les équipes sans seed (non qualifiées) n'apparaissent pas dans ce fichier.
    Les jointures sur ce dataset doivent donc utiliser `how="left"` pour conserver
    toutes les équipes.
    """
    seed_col = F.col("Seed")

    return (
        seeds_df
        .select(
            F.col("Season").cast("int"),
            F.col("TeamID").cast("int"),
            seed_col.alias("Seed"),
        )
        .withColumn("SeedRegion", F.regexp_extract(seed_col, _SEED_PATTERN, 1))
        .withColumn("SeedNum",    F.regexp_extract(seed_col, _SEED_PATTERN, 2).cast("int"))
    )
