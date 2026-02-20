# March Machine Learning Mania 2026 — Lakehouse local (MinIO) + Spark 4 + PySpark (Docker, Windows)

This repository provides an **enterprise-style local Big Data setup** (100% free) to process Kaggle CSV files (~180MB+) using:

- **Apache Spark 4** (official Docker image)
- **MinIO** (S3-compatible **local data lake**)
- **Bronze / Silver / Gold** lakehouse layout (Parquet)
- **PySpark jobs** with reusable Spark session config + logging
- Simple **Makefile** shortcuts and a clean project structure

It is designed for:
- Kaggle competitions (like March Machine Learning Mania) where you want a **reproducible**, **cloud-like** pipeline
- Portfolio / exam / interview demonstrations of a modern data platform

---

## Architecture

**Raw CSV (local)** → (PySpark ingest) → **MinIO / Bronze (Parquet)**  
→ (features) → **Silver**  
→ (training dataset) → **Gold**  
→ (model training, optional) → `artifacts/`

MinIO is S3-compatible, so this setup mirrors AWS S3 / GCS / ADLS patterns while running fully locally.

---

## Prerequisites (Windows)

1. **Docker Desktop** installed and running (WSL2 enabled).
2. Optional: `make` (or use the raw docker commands below).

---

## Quick start

### 1) Copy env file

```bash
copy .env.example .env
```

### 2) Start the platform (MinIO + Spark cluster)

```bash
docker compose up -d
```

This starts:
- `minio` (S3 API: http://localhost:9000, Console: http://localhost:9001)
- `minio-mc` (creates the bucket on startup)
- `spark-master` (Spark UI: http://localhost:8080)
- `spark-worker-1` (Worker UI: http://localhost:8081)

### 3) Put Kaggle CSV files into the input folder

Put your Kaggle CSVs into:

```
data/input/
```

Example:
- `data/input/MRegularSeasonCompactResults.csv`
- `data/input/MTeams.csv`
- etc.

> Note: files in `data/input/` are **mounted into the Spark container** at `/opt/project/data/input`.

### 4) Run the pipeline (Bronze → Silver → Gold)

Run each job with `docker compose run` (recommended) or `make`.

**Bronze (CSV → Parquet in MinIO):**
```bash
docker compose run --rm spark-submit python jobs/01_ingest_bronze.py
```

**Silver (feature engineering):**
```bash
docker compose run --rm spark-submit python jobs/02_build_silver_features.py
```

**Gold (training dataset for matchups):**
```bash
docker compose run --rm spark-submit python jobs/03_build_gold_training_set.py
```

### 5) Verify data in MinIO

Open MinIO console: http://localhost:9001  
Login with values from `.env` (default `admin/admin123`)

Bucket: `kaggle-lake`  
You should see:
- `bronze/...`
- `silver/...`
- `gold/...`

---

## Where is the “data lake”?

It is MinIO (S3-compatible) running locally.

- S3 endpoint: `http://localhost:9000`
- Bucket: `kaggle-lake`
- Example path: `s3a://kaggle-lake/bronze/march_mania/...`

---

## Project layout

```
.
├─ docker-compose.yml
├─ .env.example
├─ Makefile
├─ jobs/                         # PySpark batch jobs
│  ├─ 01_ingest_bronze.py
│  ├─ 02_build_silver_features.py
│  └─ 03_build_gold_training_set.py
├─ src/
│  ├─ common/
│  │  ├─ spark.py                # SparkSession factory (S3A / MinIO)
│  │  ├─ paths.py                # Centralized lake paths
│  │  └─ logging.py              # Consistent logging
│  └─ features/
│     └─ basketball_features.py  # Reusable feature builders
├─ data/
│  └─ input/                     # Put Kaggle CSV here (not committed)
└─ artifacts/                    # Optional exports (submissions, models)
```

---

## Notes for Spark 4

This stack uses the **official Apache Spark Docker image** (`apache/spark:4.x`) from Docker Hub.  
See Spark docker images documentation and tags:
- Apache Spark docker availability on Apache website (Downloads → Installing with Docker)
- `apache/spark` tags on Docker Hub

---

## Troubleshooting

### MinIO console opens, but bucket not created
Run:
```bash
docker compose logs minio-mc
```
Then rerun:
```bash
docker compose up -d minio-mc
```

### Spark job cannot access `s3a://...`
Ensure:
- `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` in `.env`
- Spark job uses `src/common/spark.py` which configures S3A
- Containers are on the same docker network (compose handles that)

---

## Next steps (optional)

- Add ML model training (Spark MLlib / sklearn) using `gold` dataset.
- Add orchestration (Airflow) and data quality checks (Great Expectations).
- Add experiment tracking (MLflow).

---

## License
MIT


---

## Model training + Kaggle submission export (optional)

After running Bronze → Silver → Gold, you can train a baseline model and export `submission.csv`.

1) Put the Kaggle sample submission into `data/input/` (one of):
   - `MSampleSubmissionStage1.csv`
   - `MSampleSubmissionStage2.csv`
   - `sample_submission.csv`

2) Run:

```bash
docker compose run --rm spark-submit python jobs/04_train_and_export_submission.py
```

Output:
- `artifacts/submission.csv`

### Leakage note (important)
This repo uses a **season-based split** for validation (latest season as validation) to reduce time-series leakage.
For stronger setups, you can:
- use multiple seasons as validation (rolling backtests),
- remove tournament games from feature computation,
- build ELO per season with strict time ordering.


---

## 📂 Project Structure (Important)

Put your Kaggle CSV files inside:

```
data/input/
```

Example:

```
march-mania-spark-lakehouse/
│
├── docker-compose.yml
├── .env
├── Makefile
│
├── conf/
│   └── log4j2.properties
│
├── data/
│   └── input/                # ← PUT YOUR KAGGLE CSV FILES HERE
│       ├── MRegularSeasonCompactResults.csv
│       ├── MTeams.csv
│       ├── MNCAATourneyCompactResults.csv
│       ├── MSampleSubmissionStage1.csv
│
├── artifacts/
│   └── submission.csv        # Generated Kaggle submission file
│
├── jobs/
│   ├── 01_ingest_bronze.py
│   ├── 02_build_silver_features.py
│   ├── 03_build_gold_training_set.py
│   └── 04_train_and_export_submission.py
│
├── src/
│   ├── common/
│   │   ├── spark.py
│   │   ├── paths.py
│   │   └── logging.py
│   │
│   ├── features/
│   │   └── basketball_features.py
│   │
│   └── ml/
│       └── modeling.py
│
└── .github/
    └── workflows/
        └── ci.yml
```

### 🏗 Lakehouse Logical Architecture

Bronze:
```
s3a://kaggle-lake/bronze/march_mania/
```

Silver:
```
s3a://kaggle-lake/silver/march_mania/
```

Gold:
```
s3a://kaggle-lake/gold/march_mania/
```

Data flow:

Raw CSV (data/input/)  
→ Spark ingestion  
→ Bronze (Parquet)  
→ Silver (features)  
→ Gold (ML dataset)  
→ Model training  
→ artifacts/submission.csv  


---

## 🔥 Expert Mode (Kaggle-competitive)

This repo includes an **expert pipeline** designed to improve Kaggle LogLoss scores:

- **Season-scoped ELO** (reset each season) computed from regular season games
- **Causal rolling momentum features** (last N games, excluding the current game)
- **Rolling season backtest** (train <= season-1, validate = season)
- Export:
  - `artifacts/backtest_metrics.csv`
  - `artifacts/submission_blend.csv`

### Run expert feature jobs

```bash
docker compose run --rm spark-submit python jobs/05_build_silver_elo.py
docker compose run --rm spark-submit python jobs/06_build_silver_rolling.py
```

### Run expert backtest + export

```bash
docker compose run --rm spark-submit python jobs/07_backtest_and_export_blend.py
```

### Configure experiments

Edit:

- `conf/pipeline.yml` (ELO K-factor, rolling window size, model parameters, backtest range)

---

## League (Men/Women)

This repo supports both **Men's** and **Women's** datasets.

Set the league in:
- `conf/pipeline.yml` → `competition.league: "M"` or `"W"`

Then run the same pipeline (Bronze → Silver → Gold → Expert).

---

## Recommended full pipeline (Men, Kaggle-competitive)

```bash
make bronze
make silver
make elo
make rolling
make seeds
make massey      # men only (uses MMasseyOrdinals.csv)
make sos
make gold
make expert2     # rolling backtest + artifacts/submission_expert.csv
```

Outputs:
- `artifacts/backtest_metrics.csv`
- `artifacts/submission_expert.csv`

---

## 🪟 Windows automation scripts (.ps1 + .bat)

To run everything end-to-end on Windows (Docker Desktop), use:

- PowerShell (recommended): `scripts/run_full_pipeline.ps1`
- Batch: `scripts/run_full_pipeline.bat`

### How to use
1) Put ALL Kaggle CSV files into:
```
scripts/csv_source/
```

2) Run:
PowerShell:
```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\scripts
un_full_pipeline.ps1
```

Batch:
```bat
scripts
un_full_pipeline.bat
```

Outputs:
- `artifacts/submission_ensemble.csv` (best final submission)
- `artifacts/submission_expert.csv` (single-model expert submission)
- `artifacts/backtest_metrics.csv`
- `artifacts/hpo_best_params.json`

---

## 🔧 Hyperparameter Tuning (HPO)

Run quick HPO (TrainValidationSplit) on a season-based holdout:

```bash
make hpo
```

This writes:
- `artifacts/hpo_best_params.json`

---

## 🧩 Ensemble export (LogReg + GBT)

Train both models (using HPO params if present) and export an ensemble submission:

```bash
make ensemble
```

Output:
- `artifacts/submission_ensemble.csv`

---

## 🧾 Detailed logging (local or docker)

This project writes detailed logs to:
- `artifacts/logs/<mode>_run_<timestamp>.log`

Each line includes the run mode (`local` or `docker`) for easy debugging.

---

## ⚡ Local run (no Docker) — FAST

For a quick local validation run (no MinIO/S3, no Docker), use:

1) Put Kaggle CSV files into:
```
scripts/csv_source/
```

2) Run:
```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\scriptsun_local_fast.ps1
```

Outputs:
- `artifacts/submission_expert.csv`
- `artifacts/backtest_metrics.csv`
- `artifacts/logs/*.log`

Notes:
- This script uses `local_overrides/` via `PYTHONPATH` so you do not have to change any project source file.
