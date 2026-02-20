# Kaggle March Mania — Feature Guide (Practical)

This repo ingests 20+ CSVs for Men's and Women's competitions. Here are **high-signal features** you should prioritize.

## Tier 1 (biggest gains)
1) **SeedDiff** (from *MNCAATourneySeeds.csv / WNCAATourneySeeds.csv*)
   - Parse SeedNum and compute: `SeedDiff = SeedNum(Team1) - SeedNum(Team2)`

2) **EloDiff** (from regular season compact results)
   - Season-reset ELO is robust and often improves LogLoss.

3) **Massey consensus rank** (Men, from *MMasseyOrdinals.csv*)
   - Use the *latest* ranking per system per season/team and average across systems.
   - Feature: `MasseyDiff`

4) **Rolling momentum** (causal)
   - Rolling win rate and point differential over last N games (N=10)
   - Features: `RollWinRateDiff`, `RollAvgPointDiffDiff`

## Tier 2 (often helpful)
5) **Strength of Schedule (SOS)**
   - Average opponent win rate and opponent ELO across the season.
   - Features: `SOSWinRateDiff`, `SOSEloDiff`

6) **Conference strength**
   - From *MTeamConferences.csv / WTeamConferences.csv*:
   - Compute average Elo or win rate per conference.
   - Feature: `ConferenceStrengthDiff`

## Tier 3 (advanced / optional)
7) **Detailed boxscore rates** (from RegularSeasonDetailedResults)
   - eFG%, TO%, OR% per team-season
   - Useful but more work; avoid leakage (use regular season only).

8) **City / travel** (from GameCities + Cities)
   - Distance travelled / neutral sites
   - Can add small improvements.

## Anti-leakage rules
- Do NOT use tournament game results to build features for predicting that tournament.
- Use season-based splits for validation (rolling backtests).

## Ensemble tip
- Blend GBT + Logistic Regression (alpha ~ 0.6–0.7) for more stable LogLoss.
