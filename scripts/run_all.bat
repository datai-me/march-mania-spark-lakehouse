@echo off
setlocal enabledelayedexpansion

REM run_all.bat
REM Full run for CMD: down -v -> build --no-cache -> up -d -> run pipeline -> list submissions
REM Usage:
REM   run_all.bat
REM   run_all.bat 1 4
REM   run_all.bat 1 12

set STARTJOB=1
set ENDJOB=4

if not "%~1"=="" set STARTJOB=%~1
if not "%~2"=="" set ENDJOB=%~2

echo.
echo [INFO] Using jobs %STARTJOB% -> %ENDJOB%
echo.

REM Preconditions (basic)
if not exist docker-compose.yml (
  echo [ERROR] docker-compose.yml not found in current folder.
  exit /b 1
)
if not exist jobs\run_pipeline.py (
  echo [ERROR] jobs\run_pipeline.py not found.
  exit /b 1
)

echo [INFO] Stopping stack (and removing volumes)...
docker compose down -v
if errorlevel 1 (
  echo [ERROR] docker compose down failed.
  exit /b 1
)

echo [INFO] Building images (no cache)...
docker compose build --no-cache
if errorlevel 1 (
  echo [ERROR] docker compose build failed.
  exit /b 1
)

echo [INFO] Starting services...
docker compose up -d
if errorlevel 1 (
  echo [ERROR] docker compose up failed.
  exit /b 1
)

echo [INFO] Smoke check: Python & PyYAML inside spark-submit...
docker compose run --rm --entrypoint bash spark-submit -lc "python --version && python -c \"import yaml; print('yaml ok')\""
if errorlevel 1 (
  echo [ERROR] Python/YAML smoke test failed.
  exit /b 1
)

echo [INFO] Running pipeline jobs %STARTJOB% -> %ENDJOB% ...
docker compose run --rm spark-submit jobs/run_pipeline.py %STARTJOB% %ENDJOB%
if errorlevel 1 (
  echo [ERROR] Pipeline failed.
  exit /b 1
)

echo.
echo [INFO] Searching submission files in artifacts...
if exist artifacts (
  for /r artifacts %%F in (*) do (
    echo %%~nxF | findstr /i "submission stage blend" >nul
    if not errorlevel 1 (
      echo  - %%~fF
    )
  )
) else (
  echo [WARN] No artifacts folder found (yet).
)

echo.
echo [INFO] DONE ✅
endlocal
exit /b 0