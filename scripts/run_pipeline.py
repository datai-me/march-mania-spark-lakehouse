"""
Pipeline Runner — Exécute les jobs Spark en mode Docker OU local (dual-mode).

Détection automatique du runtime :
  - Docker  : SPARK_HOME=/opt/spark (dans le conteneur spark-submit)
  - Local   : SPARK_HOME défini dans l'environnement, ou spark-submit dans le PATH

Usage :
    python jobs/run_pipeline.py             # liste les jobs disponibles
    python jobs/run_pipeline.py 1 10        # jobs 01 à 10
    python jobs/run_pipeline.py 3           # job 03 uniquement
    python jobs/run_pipeline.py 1 3 7 12    # jobs 01, 03, 07, 12 (liste libre)

Variables d'environnement :
    SPARK_MASTER    url du master (défaut : spark://spark-master:7077 en Docker,
                    local[*] en mode local)
    PIPELINE_MODE   forcer "docker" ou "local" (optionnel)
"""

import os
import sys
import time
import subprocess
from pathlib import Path

JOBS_DIR = Path(__file__).parent

# ── Détection automatique du runtime ──────────────────────────────────────────

def detect_mode() -> str:
    """
    Retourne 'docker' si on tourne dans le conteneur spark-submit,
    'local' sinon. Peut être forcé via la variable PIPELINE_MODE.
    """
    forced = os.environ.get("PIPELINE_MODE", "").lower()
    if forced in {"docker", "local"}:
        return forced
    # Dans le conteneur, SPARK_HOME est fixé à /opt/spark
    return "docker" if os.environ.get("SPARK_HOME") == "/opt/spark" else "local"


def get_spark_submit_cmd(mode: str) -> list[str]:
    """
    Retourne la commande spark-submit adaptée au mode.
    - Docker : chemin absolu dans le conteneur
    - Local  : cherche spark-submit dans SPARK_HOME ou dans le PATH
    """
    if mode == "docker":
        return ["/opt/spark/bin/spark-submit", "--master", "spark://spark-master:7077"]

    # Mode local : utilise SPARK_HOME si défini, sinon cherche dans le PATH
    spark_home = os.environ.get("SPARK_HOME")
    spark_submit = str(Path(spark_home) / "bin" / "spark-submit") if spark_home else "spark-submit"
    master = os.environ.get("SPARK_MASTER", "local[*]")
    return [spark_submit, "--master", master]


# ── Découverte des jobs ────────────────────────────────────────────────────────

def discover_jobs() -> dict[int, str]:
    """Retourne {numéro: nom_fichier} trié, en excluant les scripts utilitaires."""
    jobs = {}
    for file in JOBS_DIR.glob("*.py"):
        if file.stem.startswith("run_"):
            continue
        prefix = file.name.split("_", 1)[0]
        if prefix.isdigit():
            jobs[int(prefix)] = file.name
    return dict(sorted(jobs.items()))


# ── Exécution d'un job ────────────────────────────────────────────────────────

def run_job(job_file: str, spark_cmd: list[str]) -> bool:
    """Lance un job et retourne True si succès."""
    print(f"\n{'─' * 60}")
    print(f"  ▶  {job_file}")
    print(f"{'─' * 60}")
    start = time.time()

    result = subprocess.run(
        [*spark_cmd, str(JOBS_DIR / job_file)],
        check=False,
    )

    elapsed = time.time() - start
    if result.returncode == 0:
        print(f"  ✅  {job_file} — {elapsed:.1f}s")
        return True
    else:
        print(f"  ❌  {job_file} — ÉCHEC (code {result.returncode}) après {elapsed:.1f}s")
        return False


# ── Parsing des arguments CLI ──────────────────────────────────────────────────

def parse_args(args: list[str]) -> list[int]:
    """
    Interprète les arguments :
        "3"       → [3]
        "1 10"    → [1, 2, ..., 10]   (plage continue si exactement 2 args)
        "1 3 7"   → [1, 3, 7]         (liste libre si 3+ args)
    """
    if len(args) == 1:
        return [int(args[0])]
    if len(args) == 2:
        return list(range(int(args[0]), int(args[1]) + 1))
    return [int(a) for a in args]


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    mode = detect_mode()
    spark_cmd = get_spark_submit_cmd(mode)
    jobs = discover_jobs()

    if not jobs:
        print(f"Aucun job trouvé dans {JOBS_DIR}")
        sys.exit(1)

    args = sys.argv[1:]
    if not args:
        print(f"\n🔧  Mode détecté : {mode.upper()}")
        print(f"    spark-submit : {' '.join(spark_cmd)}\n")
        print("Jobs disponibles :\n")
        for k, v in jobs.items():
            print(f"  {k:02d}  {v}")
        print("\nUsage :")
        print("  python run_pipeline.py 1 12      # pipeline complet")
        print("  python run_pipeline.py 3         # job unique")
        print("  python run_pipeline.py 1 3 7 12  # liste libre")
        sys.exit(0)

    to_run = parse_args(args)
    print(f"\n🔧  Mode : {mode.upper()} | Jobs : {to_run}")

    total_start = time.time()
    for num in to_run:
        if num not in jobs:
            print(f"⚠️  Job {num:02d} introuvable — ignoré.")
            continue
        if not run_job(jobs[num], spark_cmd):
            print(f"\n⛔  Pipeline interrompu au job {num:02d}.")
            sys.exit(1)

    elapsed = time.time() - total_start
    print(f"\n{'═' * 60}")
    print(f"  🎯  Terminé en {elapsed:.1f}s  ({mode.upper()})")
    print(f"{'═' * 60}\n")


if __name__ == "__main__":
    main()
