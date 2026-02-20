# ==========================================================
# March Mania - LOCAL FULL Runner (No Docker)
# ----------------------------------------------------------
# Purpose:
#   Full local run without Docker/MinIO (filesystem lakehouse).
#   This is the "enterprise" pipeline, but executed on a single machine.
#
# What it runs (full):
#   - Bronze ingest (all CSV -> Parquet)
#   - Silver team season stats
#   - Silver ELO
#   - Silver Rolling momentum features
#   - Silver Seeds parsing
#   - Silver Massey consensus (Men only; will gracefully skip if missing)
#   - Silver Strength of Schedule (SOS)
#   - Gold training set build
#   - Expert rolling-season backtest + submission_expert.csv
#   - HPO quick tuning + hpo_best_params.json
#   - Ensemble training + submission_ensemble.csv
#
# How it avoids code modifications:
#   It uses local overrides via PYTHONPATH:
#     local_overrides/src/common/spark.py  -> master local[*]
#     local_overrides/src/common/paths.py  -> writes to ./lakehouse/...
#
# Inputs:
#   Put all Kaggle CSV files in: scripts/csv_source/
#
# Outputs:
#   artifacts/submission_ensemble.csv
#   artifacts/submission_expert.csv
#   artifacts/backtest_metrics.csv
#   artifacts/hpo_best_params.json
#   artifacts/logs/*.log
#
# Usage:
#   Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
#   .\scripts\run_local_full.ps1
# ==========================================================

$ErrorActionPreference = "Stop"

Write-Host "====================================="
Write-Host "March Mania LOCAL FULL - START"
Write-Host "====================================="

$scriptDir    = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot  = Split-Path -Parent $scriptDir

$csvSource    = Join-Path $scriptDir "csv_source"
$dataInput    = Join-Path $projectRoot "data\input"
$overrides    = Join-Path $projectRoot "local_overrides"
$venvDir      = Join-Path $projectRoot "venv"

# Ensure dirs exist
foreach ($p in @($csvSource, $dataInput, (Join-Path $projectRoot "artifacts"), (Join-Path $projectRoot "lakehouse"))) {
  if (!(Test-Path $p)) { New-Item -ItemType Directory -Path $p | Out-Null }
}

# Check overrides
if (!(Test-Path (Join-Path $overrides "src\common\spark.py")) -or !(Test-Path (Join-Path $overrides "src\common\paths.py"))) {
  Write-Host "ERROR: local_overrides missing."
  Write-Host "Expected:"
  Write-Host "  local_overrides/src/common/spark.py"
  Write-Host "  local_overrides/src/common/paths.py"
  exit 1
}

# Check CSVs
$csvFiles = Get-ChildItem -Path $csvSource -Filter *.csv -ErrorAction SilentlyContinue
if ($null -eq $csvFiles -or $csvFiles.Count -eq 0) {
  Write-Host "ERROR: No CSV files in $csvSource"
  Write-Host "Put your Kaggle CSV files into scripts\csv_source\ then rerun."
  exit 1
}

# Copy to data/input
Write-Host "`n[1/6] Copying CSV files to data/input..."
Copy-Item "$csvSource\*.csv" -Destination $dataInput -Force
Write-Host ("OK: Copied {0} CSV file(s)." -f $csvFiles.Count)

# Prepare venv + deps
Write-Host "`n[2/6] Preparing Python venv..."
if (!(Test-Path (Join-Path $venvDir "Scripts\python.exe"))) {
  python -m venv $venvDir
}
$venvPython = Join-Path $venvDir "Scripts\python.exe"
$venvPip    = Join-Path $venvDir "Scripts\pip.exe"
& $venvPython -m pip install --upgrade pip | Out-Null
& $venvPip install pyspark pyyaml | Out-Null
Write-Host "OK: deps installed."

# Locate spark-submit
Write-Host "`n[3/6] Locating spark-submit..."
$sparkSubmit = $null
$venvSparkSubmit = Join-Path $venvDir "Scripts\spark-submit.exe"
if (Test-Path $venvSparkSubmit) { $sparkSubmit = $venvSparkSubmit }
if ($null -eq $sparkSubmit) {
  try { $sparkSubmit = (Get-Command spark-submit).Source } catch { $sparkSubmit = $null }
}
if ($null -eq $sparkSubmit) {
  Write-Host "ERROR: spark-submit not found."
  Write-Host "Install Spark OR ensure venv pyspark provides spark-submit."
  exit 1
}
Write-Host "OK: Using $sparkSubmit"

# Set PYTHONPATH override-first
Write-Host "`n[4/6] Setting PYTHONPATH (override-first)..."
$env:PYTHONPATH = "$overrides;$projectRoot"
Write-Host "PYTHONPATH=$env:PYTHONPATH"

Set-Location $projectRoot

# Full job list
Write-Host "`n[5/6] Running FULL jobs..."
$jobs = @(
  "jobs/01_ingest_bronze.py",
  "jobs/02_build_silver_features.py",
  "jobs/05_build_silver_elo.py",
  "jobs/06_build_silver_rolling.py",
  "jobs/08_build_silver_seeds.py",
  "jobs/09_build_silver_massey.py",
  "jobs/10_build_silver_sos.py",
  "jobs/03_build_gold_training_set.py",
  "jobs/07_backtest_and_export_blend.py",
  "jobs/11_hpo_backtest.py",
  "jobs/12_train_ensemble_export.py"
)

foreach ($j in $jobs) {
  Write-Host "`n--- spark-submit $j ---"
  & $sparkSubmit $j
  if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Job failed: $j"
    exit 1
  }
}

Write-Host "`n[6/6] Outputs:"
Write-Host " - artifacts/submission_ensemble.csv"
Write-Host " - artifacts/submission_expert.csv"
Write-Host " - artifacts/backtest_metrics.csv"
Write-Host " - artifacts/hpo_best_params.json"
Write-Host " - artifacts/logs/*.log"
Write-Host "`nLOCAL FULL COMPLETE."
