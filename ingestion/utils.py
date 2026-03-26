"""
utils.py - Shared utilities for ETL and ELT pipelines.
Provides database connection and logging setup.
"""

import logging #for log info. It sorts messages according to their importance (Error or information?).
import sys
from pathlib import Path

import psycopg2

sys.path.append(str(Path(__file__).resolve().parent))
from config import DB_CONFIG


def get_connection():
    """Open and return a psycopg2 connection using DB_CONFIG."""
    return psycopg2.connect(**DB_CONFIG)


def get_logger(name: str) -> logging.Logger:
    """Return a logger with a consistent format for all scripts."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger