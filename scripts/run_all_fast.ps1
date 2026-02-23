# run_all_fast.ps1
# Fast run: start stack (if not started) -> run pipeline -> list submissions
# Usage:
#   .\run_all_fast.ps1
#   .\run_all_fast.ps1 -StartJob 1 -EndJob 4
#   .\run_all_fast.ps1 -StartJob 1 -EndJob 12

param(
  [int]$StartJob = 1,
  [int]$EndJob = 4
)

$ErrorActionPreference = "Stop"

function Say($msg) {
  $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
  Write-Host "[$ts] $msg"
}

function Require-File($path) {
  if (-not (Test-Path $path)) {
    throw "Missing required file: $path"
  }
}

Say "Checking project files..."
Require-File ".\docker-compose.yml"
Require-File ".\jobs\run_pipeline.py"

Say "Starting services (no rebuild)..."
docker compose up -d | Out-Host

Say "Smoke check: Python & PyYAML inside spark-submit..."
docker compose run --rm --entrypoint bash spark-submit -lc "python --version && python -c `"import yaml; print('yaml ok')`"" | Out-Host

Say "Running pipeline jobs $StartJob -> $EndJob ..."
docker compose run --rm spark-submit "jobs/run_pipeline.py" $StartJob $EndJob | Out-Host

Say "Searching for submission files under .\artifacts ..."
if (Test-Path ".\artifacts") {
  $subs = Get-ChildItem ".\artifacts" -Recurse -File |
    Where-Object { $_.Name -match "submission|stage|blend" }
  if ($subs.Count -eq 0) {
    Say "No submission-like files found yet. Check job export path/logs."
  } else {
    Say "Found submission files:"
    $subs | ForEach-Object { Write-Host " - $($_.FullName)" }
  }
} else {
  Say "No .\artifacts directory found (yet)."
}

Say "DONE ✅"