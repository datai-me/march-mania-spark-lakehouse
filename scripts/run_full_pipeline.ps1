# ==========================================
# March Mania - Full Expert Pipeline Runner (PowerShell)
# ------------------------------------------
# What it does:
#   1) Checks that Docker Desktop is running
#   2) Copies all Kaggle *.csv from scripts\csv_source\ into data\input\
#   3) Starts docker-compose services (MinIO + Spark)
#   4) Runs the FULL Kaggle-competitive pipeline:
#        Bronze → Silver → ELO → Rolling → Seeds → Massey → SOS → Gold → Expert Backtest+Submission
#   5) Prints output artifacts paths
#
# Usage (from repo root):
#   Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
#   .\scripts\run_full_pipeline.ps1
#
# Notes:
# - Put your Kaggle CSV files into: scripts\csv_source\
# - Output artifacts:
#     artifacts\submission_expert.csv
#     artifacts\backtest_metrics.csv
# ==========================================

Write-Host "====================================="
Write-Host "March Mania Expert Pipeline - START"
Write-Host "====================================="

# Resolve repo root from script location
$scriptDir  = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Split-Path -Parent $scriptDir

# Where you place CSVs (source) and where the pipeline expects them (target)
$dataSource = Join-Path $scriptDir "csv_source"
$dataTarget = Join-Path $projectRoot "data\input"
$artifacts  = Join-Path $projectRoot "artifacts"

# ---- 0) Pre-checks ----
if (!(Test-Path $dataSource)) {
    Write-Host "Creating CSV source folder: $dataSource"
    New-Item -ItemType Directory -Path $dataSource | Out-Null
}

if (!(Test-Path $dataTarget)) {
    Write-Host "Creating pipeline input folder: $dataTarget"
    New-Item -ItemType Directory -Path $dataTarget | Out-Null
}

# ---- 1) Docker running? ----
Write-Host "`n[1/5] Checking Docker Desktop..."
docker info > $null 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Docker is not running. Start Docker Desktop and retry."
    exit 1
}
Write-Host "OK: Docker is running."

# ---- 2) Copy CSVs ----
Write-Host "`n[2/5] Copying Kaggle CSV files..."
$csvFiles = Get-ChildItem -Path $dataSource -Filter *.csv -ErrorAction SilentlyContinue
if ($null -eq $csvFiles -or $csvFiles.Count -eq 0) {
    Write-Host "WARNING: No CSV files found in $dataSource"
    Write-Host "Put your Kaggle CSV files into scripts\csv_source\ and run again."
    exit 1
}
Copy-Item "$dataSource\*.csv" -Destination $dataTarget -Force
Write-Host ("Copied {0} CSV file(s) to {1}" -f $csvFiles.Count, $dataTarget)

# ---- 3) Start platform ----
Write-Host "`n[3/5] Starting platform (docker compose up -d)..."
Set-Location $projectRoot
docker compose up -d
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: docker compose up failed. Check docker-compose.yml and Docker Desktop."
    exit 1
}
Write-Host "OK: Platform started."

# ---- 4) Run pipeline jobs (ordered) ----
Write-Host "`n[4/5] Running pipeline jobs..."
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
  Write-Host "`n--- Running $j ---"
  docker compose run --rm spark-submit python $j
  if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Job failed: $j"
    Write-Host "Tip: run 'docker compose logs -f --tail=200' in another terminal."
    exit 1
  }
}

# ---- 5) Outputs ----
Write-Host "`n[5/5] Checking outputs..."
$submission = Join-Path $artifacts "submission_ensemble.csv"
$metrics    = Join-Path $artifacts "backtest_metrics.csv"
$hpo        = Join-Path $artifacts "hpo_best_params.json"

if (Test-Path $submission) {
    Write-Host "SUCCESS: Submission generated:"
    Write-Host "  $submission"
} else {
    Write-Host "WARNING: submission_ensemble.csv not found. Check logs."
}

if (Test-Path $metrics) {
    Write-Host "Backtest metrics:"
    Write-Host "  $metrics"
}

if (Test-Path $hpo) {
    Write-Host "HPO best params:"
    Write-Host "  $hpo"
}

Write-Host "`nDONE."
