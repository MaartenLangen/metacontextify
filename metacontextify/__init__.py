"""
MetaContextify: A package for retrieving environmental context for marine sequences.
"""

from __future__ import annotations

import logging

from .utils.logging import configure_logging, get_logger

__version__ = "0.1.0"
__author__ = "Your Name"
__email__ = "your.email@example.com"

logging.getLogger("metacontextify").addHandler(logging.NullHandler())

__all__ = [
	"configure_logging",
	"get_logger",
]
