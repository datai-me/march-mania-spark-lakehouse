"""
runtime.py — Détection du runtime (Docker vs local)
═════════════════════════════════════════════════════
Module sans dépendances externes — peut être importé partout.

Heuristiques utilisées (ordre de priorité) :
1. Variable d'environnement PIPELINE_MODE=docker|local  → toujours respectée
2. Présence de /.dockerenv                              → Docker certain
3. Variable CONTAINER=1|true|yes                        → CI/CD Docker
4. SPARK_HOME=/opt/spark                               → notre image Spark
5. Fallback : local
"""

from __future__ import annotations

import os
import platform


def is_docker() -> bool:
    """
    Retourne True si le processus tourne dans un conteneur Docker.

    Note : on évite de tester l'existence de /opt/project seul car ce dossier
    peut exister sur certaines machines Linux locales — trop de faux positifs.
    On préfère des signaux plus spécifiques à notre setup.
    """
    # 1. Forçage explicite
    forced = os.environ.get("PIPELINE_MODE", "").lower()
    if forced == "docker":
        return True
    if forced == "local":
        return False

    # 2. Marqueur Docker standard
    if os.path.exists("/.dockerenv"):
        return True

    # 3. Variable CI/CD
    if os.environ.get("CONTAINER", "").lower() in {"1", "true", "yes"}:
        return True

    # 4. Notre image Spark configure SPARK_HOME=/opt/spark
    if os.environ.get("SPARK_HOME") == "/opt/spark":
        return True

    return False


def run_mode() -> str:
    """Retourne 'docker' ou 'local'."""
    return "docker" if is_docker() else "local"


def runtime_summary() -> dict:
    """
    Retourne un dict de diagnostic — utile pour logger le contexte d'exécution
    en début de job.

    Exemple :
        logger.info("Runtime: %s", runtime_summary())
    """
    return {
        "mode":     run_mode(),
        "platform": platform.platform(),
        "python":   platform.python_version(),
        "cwd":      os.getcwd(),
        "spark_home": os.environ.get("SPARK_HOME", "non défini"),
    }
