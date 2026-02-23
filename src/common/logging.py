"""
logging.py — Configuration du logging enterprise (console + fichier rotatif)
═════════════════════════════════════════════════════════════════════════════
Objectifs :
    - Inclure le mode d'exécution (docker/local) dans chaque ligne de log
    - Écrire simultanément dans la console ET dans un fichier horodaté :
          artifacts/logs/<mode>_run_<YYYYMMDD_HHMMSS>.log
    - Éviter les handlers dupliqués lors des imports multiples (guard _CONFIGURED)
    - Résoudre le dossier logs correctement en mode Docker ET local

Usage dans un job ou module :
    from src.common.logging import get_logger
    logger = get_logger(__name__)
    logger.info("Message avec mode inclus automatiquement")
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from src.common.runtime import run_mode, runtime_summary

# Guard contre la double configuration si le module est importé plusieurs fois
_CONFIGURED: bool = False


def _resolve_logs_dir() -> Path:
    """
    Résout le dossier artifacts/logs selon le contexte d'exécution.

    Docker : SPARK_HOME=/opt/spark → base = /opt/project/artifacts/logs
    Local  : base = <cwd>/artifacts/logs

    On utilise la même heuristique que runtime.py pour la cohérence.
    """
    if os.environ.get("SPARK_HOME") == "/opt/spark":
        base = Path("/opt/project") / "artifacts" / "logs"
    else:
        # En local, on cherche artifacts/ depuis le répertoire courant
        base = Path(os.getcwd()) / "artifacts" / "logs"

    base.mkdir(parents=True, exist_ok=True)
    return base


class _ModeFilter(logging.Filter):
    """
    Injecte l'attribut `mode` (docker/local) dans chaque LogRecord.
    Permet d'utiliser %(mode)s dans le format sans le passer manuellement.
    """

    def __init__(self, mode: str) -> None:
        super().__init__()
        self._mode = mode

    def filter(self, record: logging.LogRecord) -> bool:
        record.mode = self._mode  # type: ignore[attr-defined]
        return True


def configure_logging(level: int = logging.INFO) -> None:
    """
    Configure le logging racine une seule fois (idempotent grâce au guard).

    Handlers ajoutés :
        StreamHandler  → console (stdout)
        FileHandler    → artifacts/logs/<mode>_run_<timestamp>.log

    Format de chaque ligne :
        2026-02-21 08:15:30 | INFO | mode=docker | src.features.elo | Message

    Args:
        level : niveau de logging (défaut INFO). Passer logging.DEBUG pour
                le débogage des jobs Spark.
    """
    global _CONFIGURED
    if _CONFIGURED:
        return

    mode = run_mode()
    logs_dir = _resolve_logs_dir()

    # Horodatage UTC pour les noms de fichiers — évite les ambiguïtés de timezone
    ts = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
    logfile = logs_dir / f"{mode}_run_{ts}.log"

    fmt     = "%(asctime)s | %(levelname)-8s | mode=%(mode)s | %(name)s | %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(fmt=fmt, datefmt=datefmt)
    mode_filter = _ModeFilter(mode)

    root = logging.getLogger()
    root.setLevel(level)

    # ── Handler console ───────────────────────────────────────────────────────
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    console_handler.addFilter(mode_filter)
    root.addHandler(console_handler)

    # ── Handler fichier (un fichier par run, jamais écrasé) ───────────────────
    file_handler = logging.FileHandler(logfile, encoding="utf-8")
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    file_handler.addFilter(mode_filter)
    root.addHandler(file_handler)

    # En-tête de session — visible dans la console ET le fichier
    root.info("Logging initialisé | fichier=%s", logfile)
    root.info("Runtime : %s", runtime_summary())

    _CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    """
    Retourne un logger nommé, en s'assurant que la configuration est faite.

    Usage standard dans chaque module :
        logger = get_logger(__name__)

    Args:
        name : typiquement __name__ du module appelant.
    """
    configure_logging()
    return logging.getLogger(name)
