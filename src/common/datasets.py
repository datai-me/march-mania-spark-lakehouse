"""Dataset registry and helpers.

We support both Men's (M*) and Women's (W*) March Mania files.
The ingestion job auto-detects and ingests any CSV found in `data/input/`.

This module maps filenames to lakehouse logical locations.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional

@dataclass(frozen=True)
class DatasetSpec:
    dataset_name: str          # logical dataset name
    lake_subpath: str          # subpath under bronze/march_mania/
    kind: str                  # reference | regular_season | tournament | rankings | submission | secondary
    league: str                # M | W | U (universal)


# Minimal but comprehensive mapping for common Kaggle March Mania data.
# Any unknown CSV will still be ingested into bronze/march_mania/misc/<filename_stem>
DATASETS: Dict[str, DatasetSpec] = {
    # Universal reference
    "Cities.csv": DatasetSpec("cities", "reference/cities", "reference", "U"),
    "Conferences.csv": DatasetSpec("conferences", "reference/conferences", "reference", "U"),

    # Men's reference
    "MTeams.csv": DatasetSpec("m_teams", "m/reference/teams", "reference", "M"),
    "MTeamSpellings.csv": DatasetSpec("m_team_spellings", "m/reference/team_spellings", "reference", "M"),
    "MTeamCoaches.csv": DatasetSpec("m_team_coaches", "m/reference/team_coaches", "reference", "M"),
    "MTeamConferences.csv": DatasetSpec("m_team_conferences", "m/reference/team_conferences", "reference", "M"),
    "MGameCities.csv": DatasetSpec("m_game_cities", "m/reference/game_cities", "reference", "M"),
    "MSeasons.csv": DatasetSpec("m_seasons", "m/reference/seasons", "reference", "M"),

    # Women's reference
    "WTeams.csv": DatasetSpec("w_teams", "w/reference/teams", "reference", "W"),
    "WTeamSpellings.csv": DatasetSpec("w_team_spellings", "w/reference/team_spellings", "reference", "W"),
    "WTeamConferences.csv": DatasetSpec("w_team_conferences", "w/reference/team_conferences", "reference", "W"),
    "WGameCities.csv": DatasetSpec("w_game_cities", "w/reference/game_cities", "reference", "W"),
    "WSeasons.csv": DatasetSpec("w_seasons", "w/reference/seasons", "reference", "W"),

    # Regular season results
    "MRegularSeasonCompactResults.csv": DatasetSpec("m_regular_compact", "m/regular_season/compact", "regular_season", "M"),
    "MRegularSeasonDetailedResults.csv": DatasetSpec("m_regular_detailed", "m/regular_season/detailed", "regular_season", "M"),
    "WRegularSeasonCompactResults.csv": DatasetSpec("w_regular_compact", "w/regular_season/compact", "regular_season", "W"),
    "WRegularSeasonDetailedResults.csv": DatasetSpec("w_regular_detailed", "w/regular_season/detailed", "regular_season", "W"),

    # Tournament results
    "MNCAATourneyCompactResults.csv": DatasetSpec("m_tourney_compact", "m/tournament/compact", "tournament", "M"),
    "MNCAATourneyDetailedResults.csv": DatasetSpec("m_tourney_detailed", "m/tournament/detailed", "tournament", "M"),
    "WNCAATourneyCompactResults.csv": DatasetSpec("w_tourney_compact", "w/tournament/compact", "tournament", "W"),
    "WNCAATourneyDetailedResults.csv": DatasetSpec("w_tourney_detailed", "w/tournament/detailed", "tournament", "W"),

    # Seeds and slots
    "MNCAATourneySeeds.csv": DatasetSpec("m_tourney_seeds", "m/tournament/seeds", "tournament", "M"),
    "MNCAATourneySlots.csv": DatasetSpec("m_tourney_slots", "m/tournament/slots", "tournament", "M"),
    "MNCAATourneySeedRoundSlots.csv": DatasetSpec("m_tourney_seed_round_slots", "m/tournament/seed_round_slots", "tournament", "M"),
    "WNCAATourneySeeds.csv": DatasetSpec("w_tourney_seeds", "w/tournament/seeds", "tournament", "W"),
    "WNCAATourneySlots.csv": DatasetSpec("w_tourney_slots", "w/tournament/slots", "tournament", "W"),

    # Conference tourneys
    "MConferenceTourneyGames.csv": DatasetSpec("m_conf_tourney_games", "m/tournament/conference_tourney_games", "tournament", "M"),
    "WConferenceTourneyGames.csv": DatasetSpec("w_conf_tourney_games", "w/tournament/conference_tourney_games", "tournament", "W"),

    # Rankings
    "MMasseyOrdinals.csv": DatasetSpec("m_massey_ordinals", "m/rankings/massey_ordinals", "rankings", "M"),

    # Secondary tourneys
    "MSecondaryTourneyCompactResults.csv": DatasetSpec("m_secondary_compact", "m/secondary/compact", "secondary", "M"),
    "MSecondaryTourneyTeams.csv": DatasetSpec("m_secondary_teams", "m/secondary/teams", "secondary", "M"),
    "WSecondaryTourneyCompactResults.csv": DatasetSpec("w_secondary_compact", "w/secondary/compact", "secondary", "W"),
    "WSecondaryTourneyTeams.csv": DatasetSpec("w_secondary_teams", "w/secondary/teams", "secondary", "W"),

    # Sample submissions (names vary by year)
    "SampleSubmissionStage1.csv": DatasetSpec("sample_submission_stage1", "submission/stage1", "submission", "U"),
    "SampleSubmissionStage2.csv": DatasetSpec("sample_submission_stage2", "submission/stage2", "submission", "U"),
}

def spec_for_filename(filename: str) -> Optional[DatasetSpec]:
    return DATASETS.get(filename)
