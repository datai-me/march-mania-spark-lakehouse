"""Runtime environment detection utilities.

We want detailed, reliable logging that clearly indicates whether we are running:
- inside Docker (project path /opt/project or /.dockerenv exists)
- locally on Windows/Linux/Mac (filesystem lakehouse)

This module is intentionally dependency-free.
"""

from __future__ import annotations

import os
import platform


def is_docker() -> bool:
    # Common heuristic: /.dockerenv exists in Docker containers
    if os.path.exists("/.dockerenv"):
        return True
    # Our docker images mount the project into /opt/project
    if os.path.exists("/opt/project"):
        return True
    # Some CI containers may set container env var
    if os.environ.get("CONTAINER", "").lower() in {"1", "true", "yes"}:
        return True
    return False


def run_mode() -> str:
    return "docker" if is_docker() else "local"


def runtime_summary() -> dict:
    return {
        "mode": run_mode(),
        "platform": platform.platform(),
        "python": platform.python_version(),
        "cwd": os.getcwd(),
    }
