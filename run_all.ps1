# run_all.ps1
# One-click: rebuild -> start stack -> run pipeline -> list submissions
# Usage (PowerShell):
#   .\run_all.ps1
#   .\run_all.ps1 -StartJob 1 -EndJob 4
#   .\run_all.ps1 -StartJob 1 -EndJob 12

param(
  [int]$StartJob = 1,
  [int]$EndJob = 4,
  [switch]$NoCache = $true
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

# --- Preconditions ---
Say "Checking project files..."
Require-File ".\docker-compose.yml"
Require-File ".\Dockerfile.spark"
Require-File ".\requirements.txt"
Require-File ".\jobs\run_pipeline.py"

# JARs required for S3A/MinIO (offline)
Require-File ".\jars\hadoop-aws-3.4.1.jar"
Require-File ".\jars\bundle-2.41.32.jar"
Require-File ".\jars\url-connection-client-2.41.32.jar"

# --- Clean & build ---
Say "Stopping stack (and removing volumes)..."
docker compose down -v | Out-Host

Say "Building images..."
if ($NoCache) {
  docker compose build --no-cache | Out-Host
} else {
  docker compose build | Out-Host
}

# --- Start services ---
Say "Starting services (minio + spark master/worker)..."
docker compose up -d | Out-Host

# --- Quick health checks ---
Say "Smoke check: Python & PyYAML inside spark-submit..."
docker compose run --rm --entrypoint bash spark-submit -lc "python --version && python -c `"import yaml; print('yaml ok')`"" | Out-Host

Say "Smoke check: MinIO reachable from inside network..."
docker compose run --rm --entrypoint bash spark-submit -lc "curl -sS http://minio:9000/minio/health/ready && echo OK" | Out-Host

# --- Run pipeline ---
Say "Running pipeline jobs $StartJob -> $EndJob ..."
docker compose run --rm spark-submit "jobs/run_pipeline.py" $StartJob $EndJob | Out-Host

# --- List submissions ---
Say "Searching for submission files under .\artifacts ..."
if (Test-Path ".\artifacts") {
  $subs = Get-ChildItem ".\artifacts" -Recurse -File | Where-Object { $_.Name -match "submission|stage|blend" }
  if ($subs.Count -eq 0) {
    Say "No submission-like files found yet. Check logs or your job export path."
  } else {
    Say "Found submission files:"
    $subs | ForEach-Object { Write-Host " - $($_.FullName)" }
  }
} else {
  Say "No .\artifacts directory found (yet)."
}

Say "DONE ✅"