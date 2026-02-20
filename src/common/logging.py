"""Project-wide logging helpers.

Why:
- Spark often runs in multiple processes/containers.
- A consistent log format makes debugging and exam/portfolio demos much easier.
"""

import logging
from typing import Optional


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Create or get a configured logger.

    Args:
        name: Logger name, usually __name__.
        level: Logging level (default INFO).

    Returns:
        Configured logger.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            fmt="%(asctime)s %(levelname)s [%(name)s] %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(level)
    logger.propagate = False
    return logger
