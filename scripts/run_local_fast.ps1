# ==========================================================
# March Mania - LOCAL FAST Runner (No Docker)
# ----------------------------------------------------------
# Purpose:
#   A FAST local run that validates your setup quickly.
#
# What it runs (minimal but useful):
#   - Bronze ingest (all CSV -> Parquet)
#   - Silver team season stats
#   - Silver ELO
#   - Silver Seeds parsing
#   - Gold training set build
#   - Expert backtest + submission export (single model)
#
# Skips for speed:
#   - Rolling features
#   - Massey
#   - SOS
#   - HPO
#   - Ensemble
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
#   artifacts/submission_expert.csv
#   artifacts/backtest_metrics.csv
#   artifacts/logs/*.log   (detailed run logs)
#
# Usage:
#   Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
#   .\scripts\run_local_fast.ps1
# ==========================================================

$ErrorActionPreference = "Stop"

Write-Host "====================================="
Write-Host "March Mania LOCAL FAST - START"
Write-Host "====================================="

$scriptDir    = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot  = Split-Path -Parent $scriptDir

$csvSource    = Join-Path $scriptDir "csv_source"
$dataInput    = Join-Path $projectRoot "data\input"
$overrides    = Join-Path $projectRoot "local_overrides"
$venvDir      = Join-Path $projectRoot "venv"

# Ensure dirs
foreach ($p in @($csvSource, $dataInput, (Join-Path $projectRoot "artifacts"), (Join-Path $projectRoot "lakehouse"))) {
  if (!(Test-Path $p)) { New-Item -ItemType Directory -Path $p | Out-Null }
}

# Check CSV presence
$csvFiles = Get-ChildItem -Path $csvSource -Filter *.csv -ErrorAction SilentlyContinue
if ($null -eq $csvFiles -or $csvFiles.Count -eq 0) {
  Write-Host "ERROR: No CSV files in $csvSource"
  Write-Host "Put your Kaggle CSV files into scripts\csv_source\ then rerun."
  exit 1
}

# Copy to data/input
Write-Host "`n[1/5] Copying CSV files to data/input..."
Copy-Item "$csvSource\*.csv" -Destination $dataInput -Force
Write-Host ("OK: Copied {0} CSV file(s)." -f $csvFiles.Count)

# Prepare venv + deps
Write-Host "`n[2/5] Preparing Python venv..."
if (!(Test-Path (Join-Path $venvDir "Scripts\python.exe"))) {
  python -m venv $venvDir
}
$venvPython = Join-Path $venvDir "Scripts\python.exe"
$venvPip    = Join-Path $venvDir "Scripts\pip.exe"
& $venvPython -m pip install --upgrade pip | Out-Null
& $venvPip install pyspark pyyaml | Out-Null

# Locate spark-submit
Write-Host "`n[3/5] Locating spark-submit..."
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
Write-Host "`n[4/5] Setting PYTHONPATH (override-first)..."
$env:PYTHONPATH = "$overrides;$projectRoot"
Write-Host "PYTHONPATH=$env:PYTHONPATH"

Set-Location $projectRoot

# FAST job list
Write-Host "`n[5/5] Running FAST jobs..."
$jobs = @(
  "jobs/01_ingest_bronze.py",
  "jobs/02_build_silver_features.py",
  "jobs/05_build_silver_elo.py",
  "jobs/08_build_silver_seeds.py",
  "jobs/03_build_gold_training_set.py",
  "jobs/07_backtest_and_export_blend.py"
)

foreach ($j in $jobs) {
  Write-Host "`n--- spark-submit $j ---"
  & $sparkSubmit $j
  if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Job failed: $j"
    exit 1
  }
}

Write-Host "`nFAST RUN COMPLETE."
Write-Host "Check:"
Write-Host " - artifacts/submission_expert.csv"
Write-Host " - artifacts/backtest_metrics.csv"
Write-Host " - artifacts/logs/*.log"
