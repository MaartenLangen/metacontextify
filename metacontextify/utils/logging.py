"""
Logging utilities for MetaContextify.
"""

from __future__ import annotations

import logging
import os
from typing import Optional

_DEFAULT_LOG_LEVEL = os.getenv("METACONTEXTIFY_LOG_LEVEL", "INFO")


def configure_logging(level: Optional[str] = None) -> logging.Logger:
    """
    Configure package-wide logging.

    Parameters
    ----------
    level : str, optional
        Logging level (e.g., "INFO", "DEBUG"). If None, uses
        METACONTEXTIFY_LOG_LEVEL or defaults to INFO.

    Returns
    -------
    logging.Logger
        The configured base logger for MetaContextify.
    """
    logger = logging.getLogger("metacontextify")

    has_real_handler = any(
        handler for handler in logger.handlers if not isinstance(handler, logging.NullHandler)
    )
    if not has_real_handler:
        logger.handlers = [
            handler
            for handler in logger.handlers
            if not isinstance(handler, logging.NullHandler)
        ]
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(levelname)s: %(message)s (%(asctime)s, %(name)s)",
            datefmt="%d/%m/%Y %H:%M:%S"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    if level is None:
        level = _DEFAULT_LOG_LEVEL

    logger.setLevel(level)
    logger.propagate = False
    return logger


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Get a namespaced logger for MetaContextify modules.

    Parameters
    ----------
    name : str, optional
        Child logger name under the "metacontextify" namespace.

    Returns
    -------
    logging.Logger
        Logger instance.
    """
    base = "metacontextify"
    logger_name = base if not name else f"{base}.{name}"
    return logging.getLogger(logger_name)
