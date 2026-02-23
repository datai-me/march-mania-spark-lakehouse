"""
datasets.py — Registre des fichiers Kaggle March Mania
═══════════════════════════════════════════════════════
Mappe chaque nom de fichier CSV Kaggle vers son chemin logique dans le lakehouse.

Pourquoi un registre centralisé ?
- Le job d'ingestion Bronze peut ingérer n'importe quel CSV sans conditions if/else
- Les chemins Silver/Gold s'y réfèrent par logique (pas par nom de fichier)
- Les fichiers inconnus sont toujours ingérés (dans misc/) sans crash

Leagues : M = Hommes | W = Femmes | U = Universel (partagé)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional


@dataclass(frozen=True)
class DatasetSpec:
    """Spécification d'un dataset Kaggle dans le lakehouse."""
    dataset_name: str   # identifiant logique
    lake_subpath: str   # chemin sous bronze/march_mania/
    kind: str           # reference | regular_season | tournament | rankings | submission | secondary
    league: str         # M | W | U


# ── Registre principal ────────────────────────────────────────────────────────
# Tout CSV Kaggle connu → chemin Bronze logique.
# Les fichiers absents du registre sont ingérés dans bronze/march_mania/misc/.
DATASETS: Dict[str, DatasetSpec] = {

    # ── Référence universelle ──────────────────────────────────────────────────
    "Cities.csv":              DatasetSpec("cities",       "reference/cities",       "reference", "U"),
    "Conferences.csv":         DatasetSpec("conferences",  "reference/conferences",  "reference", "U"),

    # ── Référence Hommes ──────────────────────────────────────────────────────
    "MTeams.csv":              DatasetSpec("m_teams",              "m/reference/teams",              "reference", "M"),
    "MSeasons.csv":            DatasetSpec("m_seasons",            "m/reference/seasons",            "reference", "M"),
    "MTeamSpellings.csv":      DatasetSpec("m_team_spellings",     "m/reference/team_spellings",     "reference", "M"),
    "MTeamCoaches.csv":        DatasetSpec("m_team_coaches",       "m/reference/team_coaches",       "reference", "M"),
    "MTeamConferences.csv":    DatasetSpec("m_team_conferences",   "m/reference/team_conferences",   "reference", "M"),
    "MGameCities.csv":         DatasetSpec("m_game_cities",        "m/reference/game_cities",        "reference", "M"),

    # ── Référence Femmes ──────────────────────────────────────────────────────
    "WTeams.csv":              DatasetSpec("w_teams",              "w/reference/teams",              "reference", "W"),
    "WSeasons.csv":            DatasetSpec("w_seasons",            "w/reference/seasons",            "reference", "W"),
    "WTeamSpellings.csv":      DatasetSpec("w_team_spellings",     "w/reference/team_spellings",     "reference", "W"),
    "WTeamConferences.csv":    DatasetSpec("w_team_conferences",   "w/reference/team_conferences",   "reference", "W"),
    "WGameCities.csv":         DatasetSpec("w_game_cities",        "w/reference/game_cities",        "reference", "W"),

    # ── Saison régulière ──────────────────────────────────────────────────────
    "MRegularSeasonCompactResults.csv":  DatasetSpec("m_regular_compact",  "m/regular_season/compact",  "regular_season", "M"),
    "MRegularSeasonDetailedResults.csv": DatasetSpec("m_regular_detailed", "m/regular_season/detailed", "regular_season", "M"),
    "WRegularSeasonCompactResults.csv":  DatasetSpec("w_regular_compact",  "w/regular_season/compact",  "regular_season", "W"),
    "WRegularSeasonDetailedResults.csv": DatasetSpec("w_regular_detailed", "w/regular_season/detailed", "regular_season", "W"),

    # ── Tournoi NCAA ──────────────────────────────────────────────────────────
    "MNCAATourneyCompactResults.csv":    DatasetSpec("m_tourney_compact",          "m/tournament/compact",          "tournament", "M"),
    "MNCAATourneyDetailedResults.csv":   DatasetSpec("m_tourney_detailed",         "m/tournament/detailed",         "tournament", "M"),
    "MNCAATourneySeeds.csv":             DatasetSpec("m_tourney_seeds",            "m/tournament/seeds",            "tournament", "M"),
    "MNCAATourneySlots.csv":             DatasetSpec("m_tourney_slots",            "m/tournament/slots",            "tournament", "M"),
    "MNCAATourneySeedRoundSlots.csv":    DatasetSpec("m_tourney_seed_round_slots", "m/tournament/seed_round_slots", "tournament", "M"),
    "MConferenceTourneyGames.csv":       DatasetSpec("m_conf_tourney_games",       "m/tournament/conference_games", "tournament", "M"),

    "WNCAATourneyCompactResults.csv":    DatasetSpec("w_tourney_compact",          "w/tournament/compact",          "tournament", "W"),
    "WNCAATourneyDetailedResults.csv":   DatasetSpec("w_tourney_detailed",         "w/tournament/detailed",         "tournament", "W"),
    "WNCAATourneySeeds.csv":             DatasetSpec("w_tourney_seeds",            "w/tournament/seeds",            "tournament", "W"),
    "WNCAATourneySlots.csv":             DatasetSpec("w_tourney_slots",            "w/tournament/slots",            "tournament", "W"),
    "WConferenceTourneyGames.csv":       DatasetSpec("w_conf_tourney_games",       "w/tournament/conference_games", "tournament", "W"),

    # ── Rankings (Hommes uniquement) ──────────────────────────────────────────
    "MMasseyOrdinals.csv":               DatasetSpec("m_massey_ordinals", "m/rankings/massey_ordinals", "rankings", "M"),

    # ── Tournois secondaires ──────────────────────────────────────────────────
    "MSecondaryTourneyCompactResults.csv": DatasetSpec("m_secondary_compact", "m/secondary/compact", "secondary", "M"),
    "MSecondaryTourneyTeams.csv":          DatasetSpec("m_secondary_teams",   "m/secondary/teams",   "secondary", "M"),
    "WSecondaryTourneyCompactResults.csv": DatasetSpec("w_secondary_compact", "w/secondary/compact", "secondary", "W"),
    "WSecondaryTourneyTeams.csv":          DatasetSpec("w_secondary_teams",   "w/secondary/teams",   "secondary", "W"),

    # ── Soumissions Kaggle (noms variables selon l'année) ────────────────────
    # Stage 1 = soumission préliminaire (toutes les combinaisons possibles)
    # Stage 2 = soumission finale (après premier tour du tournoi)
    "SampleSubmissionStage1.csv":  DatasetSpec("sample_submission_stage1",   "submission/stage1",   "submission", "U"),
    "SampleSubmissionStage2.csv":  DatasetSpec("sample_submission_stage2",   "submission/stage2",   "submission", "U"),
    "MSampleSubmissionStage1.csv": DatasetSpec("m_sample_submission_stage1", "m/submission/stage1", "submission", "M"),
    "MSampleSubmissionStage2.csv": DatasetSpec("m_sample_submission_stage2", "m/submission/stage2", "submission", "M"),
    "WSampleSubmissionStage1.csv": DatasetSpec("w_sample_submission_stage1", "w/submission/stage1", "submission", "W"),
    "WSampleSubmissionStage2.csv": DatasetSpec("w_sample_submission_stage2", "w/submission/stage2", "submission", "W"),
}


def spec_for_filename(filename: str) -> Optional[DatasetSpec]:
    """Retourne la spec d'un fichier CSV, ou None si inconnu (ingestion dans misc/)."""
    return DATASETS.get(filename)
