"""Logging helpers (enterprise-style).

Goals:
- Always include run mode (local/docker) in logs.
- Log to BOTH console and a rotating per-run file:
    artifacts/logs/<mode>_run_<timestamp>.log
- Avoid duplicate handler attachment across imports.

This makes debugging much easier, especially for Spark jobs.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from pathlib import Path

from src.common.runtime import run_mode, runtime_summary

_CONFIGURED = False


def _artifacts_logs_dir() -> Path:
    # In Docker, the project is mounted at /opt/project and artifacts is there.
    # Locally, artifacts/ is relative to repo root.
    if os.path.exists("/opt/project"):
        base = Path("/opt/project") / "artifacts" / "logs"
    else:
        base = Path(os.getcwd()) / "artifacts" / "logs"
    base.mkdir(parents=True, exist_ok=True)
    return base


def configure_logging(level: int = logging.INFO) -> None:
    global _CONFIGURED
    if _CONFIGURED:
        return

    mode = run_mode()
    logs_dir = _artifacts_logs_dir()
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    logfile = logs_dir / f"{mode}_run_{ts}.log"

    fmt = "%(asctime)s | %(levelname)s | mode=%(mode)s | %(name)s | %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"

    class ModeFilter(logging.Filter):
        def filter(self, record: logging.LogRecord) -> bool:
            record.mode = mode
            return True

    root = logging.getLogger()
    root.setLevel(level)

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(logging.Formatter(fmt=fmt, datefmt=datefmt))
    ch.addFilter(ModeFilter())
    root.addHandler(ch)

    # File handler (per-run file)
    fh = logging.FileHandler(logfile, encoding="utf-8")
    fh.setLevel(level)
    fh.setFormatter(logging.Formatter(fmt=fmt, datefmt=datefmt))
    fh.addFilter(ModeFilter())
    root.addHandler(fh)

    # Emit runtime header once
    root.info("Logging configured. logfile=%s", str(logfile))
    root.info("Runtime summary: %s", runtime_summary())

    _CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    configure_logging()
    return logging.getLogger(name)
