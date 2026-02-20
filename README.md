```{=html}
<p align="center">
```
```{=html}
<h1 align="center">
```
🏀 March Mania AI Platform
```{=html}
</h1>
```
```{=html}
<h3 align="center">
```
Enterprise Lakehouse • Distributed ML • Spark 4 • Production
Architecture
```{=html}
</h3>
```
```{=html}
</p>
```
```{=html}
<p align="center">
```
`<img src="https://img.shields.io/badge/Apache-Spark%204-orange"/>`{=html}
`<img src="https://img.shields.io/badge/Docker-Containerized-blue"/>`{=html}
`<img src="https://img.shields.io/badge/Python-3.11-yellow"/>`{=html}
`<img src="https://img.shields.io/badge/Machine%20Learning-GBT%20%2B%20LR-green"/>`{=html}
`<img src="https://img.shields.io/badge/Kaggle-March%20Mania-blueviolet"/>`{=html}
`<img src="https://img.shields.io/badge/Architecture-AI%20Platform-black"/>`{=html}
```{=html}
</p>
```

------------------------------------------------------------------------

# 🌍 Executive Overview

This repository presents a **production-grade AI Platform Architecture**
built with **Apache Spark 4**.

Designed around Kaggle's March Machine Learning Mania competition, this
project demonstrates:

-   Modern Lakehouse Architecture (Bronze / Silver / Gold)
-   Distributed feature engineering
-   Enterprise ML pipelines
-   Hyperparameter optimization
-   Model ensembling
-   Season-based anti-leakage validation
-   Docker & Local execution modes
-   Structured logging & observability
-   CI/CD-ready architecture

This is not a competition notebook --- it is an **AI Platform
Blueprint**.

------------------------------------------------------------------------

# 🏗 End-to-End Architecture

## 🔹 Data Flow

``` mermaid
flowchart LR
    A[Raw Kaggle CSV] --> B[Bronze Layer]
    B --> C[Silver Feature Engineering]
    C --> D[Gold ML Dataset]
    D --> E[Model Training]
    E --> F[HPO]
    F --> G[Ensemble]
    G --> H[Kaggle Submission]
```

------------------------------------------------------------------------

## 🔹 Detailed ML Pipeline

``` mermaid
flowchart TD
    A[Gold Dataset] --> B[Feature Vector Assembler]
    B --> C1[Logistic Regression]
    B --> C2[Gradient Boosted Trees]
    C1 --> D1[Validation Metrics]
    C2 --> D2[Validation Metrics]
    D1 --> E[Model Blending]
    D2 --> E
    E --> F[Final Submission CSV]
```

------------------------------------------------------------------------

## 🔹 CI/CD & MLOps Conceptual Flow

``` mermaid
flowchart LR
    Dev[Developer Commit] --> CI[GitHub Actions CI]
    CI --> Test[Lint + Syntax Check]
    Test --> Build[Docker Build]
    Build --> Deploy[Run Pipeline]
    Deploy --> Artifacts[Model & Submission Artifacts]
```

------------------------------------------------------------------------

# 🧠 Feature Engineering Strategy

-   Dynamic ELO ratings
-   Rolling performance momentum
-   Tournament seed modeling
-   Massey ranking consensus
-   Strength of Schedule (SOS)

All features are computed using distributed Spark transformations.

------------------------------------------------------------------------

# 🤖 Machine Learning Strategy

## Base Models

-   Logistic Regression
-   Gradient Boosted Trees

## Hyperparameter Optimization

-   TrainValidationSplit
-   Season-based holdout
-   Exported best parameters

Output:

    artifacts/hpo_best_params.json

------------------------------------------------------------------------

## 🧩 Ensemble Modeling

Final prediction:

    Prediction = α × GBT + (1 − α) × Logistic

Output:

    artifacts/submission_ensemble.csv

------------------------------------------------------------------------

# 🧪 Backtesting & Validation

-   Rolling season validation
-   Anti data leakage
-   Metrics: AUC & LogLoss

Stored in:

    artifacts/backtest_metrics.csv

------------------------------------------------------------------------

# ⚙ Execution Modes

## Docker (Enterprise Simulation)

    docker compose up -d
    make full

## Local Fast

    .\scriptsun_local_fast.ps1

## Local Full

    .\scriptsun_local_full.ps1

------------------------------------------------------------------------

# 🧾 Observability

Logs stored in:

    artifacts/logs/<mode>_run_<timestamp>.log

Each log includes runtime mode detection (local/docker).

------------------------------------------------------------------------

# 📊 Business & Engineering Impact

This project demonstrates the ability to:

-   Architect scalable AI platforms
-   Design distributed feature pipelines
-   Implement reproducible ML workflows
-   Apply production-grade ensemble strategies
-   Build CI/CD-ready ML systems
-   Bridge competition ML and enterprise AI engineering

------------------------------------------------------------------------

# 🏆 AI Platform Engineering Highlights

✔ Distributed Spark transformations\
✔ Clean modular architecture\
✔ Hyperparameter tuning\
✔ Model ensembling\
✔ Dual runtime support\
✔ Structured logging\
✔ CI/CD integration ready

------------------------------------------------------------------------

# 👨‍💻 Author

Enterprise Data & AI Architect\
Specialized in Distributed AI Systems & Lakehouse Platforms

------------------------------------------------------------------------

*Last Updated: 2026-02-20*
