@echo off
REM ==========================================
REM March Mania - Full Expert Pipeline Runner (.bat)
REM ------------------------------------------
REM What it does:
REM   1) Checks that Docker Desktop is running
REM   2) Copies all Kaggle *.csv from scripts\csv_source\ into data\input\
REM   3) Starts docker-compose services (MinIO + Spark)
REM   4) Runs the full pipeline (including HPO + ensemble export)
REM   5) Prints output artifacts paths
REM
REM Usage (from repo root):
REM   scripts\run_full_pipeline.bat
REM
REM Notes:
REM - Put your Kaggle CSV files into: scripts\csv_source\
REM - Output artifacts:
REM     artifacts\submission_ensemble.csv
REM     artifacts\backtest_metrics.csv
REM     artifacts\hpo_best_params.json
REM ==========================================

echo =====================================
echo March Mania Expert Pipeline - START
echo =====================================

set PROJECT_ROOT=%~dp0..
set DATA_SOURCE=%~dp0csv_source
set DATA_TARGET=%PROJECT_ROOT%\data\input
set ARTIFACTS=%PROJECT_ROOT%\artifacts

REM ---- Ensure folders exist ----
if not exist "%DATA_SOURCE%" mkdir "%DATA_SOURCE%"
if not exist "%DATA_TARGET%" mkdir "%DATA_TARGET%"

REM ---- Check Docker ----
echo [1/5] Checking Docker Desktop...
docker info >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Docker is not running. Start Docker Desktop and retry.
    pause
    exit /b 1
)
echo OK: Docker is running.

REM ---- Copy CSVs ----
echo [2/5] Copying CSV files...
dir "%DATA_SOURCE%\*.csv" >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: No CSV files found in %DATA_SOURCE%
    echo Put your Kaggle CSV files into scripts\csv_source\ then rerun.
    pause
    exit /b 1
)
copy "%DATA_SOURCE%\*.csv" "%DATA_TARGET%" /Y >nul
echo OK: CSV files copied to %DATA_TARGET%

REM ---- Start platform ----
echo [3/5] Starting platform...
pushd "%PROJECT_ROOT%"
docker compose up -d
if %errorlevel% neq 0 (
    echo ERROR: docker compose up failed. Check docker-compose.yml and Docker Desktop.
    popd
    pause
    exit /b 1
)
echo OK: Platform started.

REM ---- Run jobs ----
echo [4/5] Running pipeline jobs...
set JOBS=jobs/01_ingest_bronze.py jobs/02_build_silver_features.py jobs/05_build_silver_elo.py jobs/06_build_silver_rolling.py jobs/08_build_silver_seeds.py jobs/09_build_silver_massey.py jobs/10_build_silver_sos.py jobs/03_build_gold_training_set.py jobs/07_backtest_and_export_blend.py jobs/11_hpo_backtest.py jobs/12_train_ensemble_export.py

for %%J in (%JOBS%) do (
  echo.
  echo --- Running %%J ---
  docker compose run --rm spark-submit python %%J
  if !errorlevel! neq 0 (
    echo ERROR: Job failed: %%J
    echo Tip: run "docker compose logs -f --tail=200" in another terminal.
    popd
    pause
    exit /b 1
  )
)

REM ---- Outputs ----
echo [5/5] Checking outputs...
if exist "%ARTIFACTS%\submission_ensemble.csv" (
    echo SUCCESS: Submission generated:
    echo   %ARTIFACTS%\submission_ensemble.csv
) else (
    echo WARNING: submission_ensemble.csv not found. Check logs.
)

if exist "%ARTIFACTS%\backtest_metrics.csv" (
    echo Backtest metrics:
    echo   %ARTIFACTS%\backtest_metrics.csv
)

if exist "%ARTIFACTS%\hpo_best_params.json" (
    echo HPO best params:
    echo   %ARTIFACTS%\hpo_best_params.json
)

popd
echo.
echo DONE.
pause
