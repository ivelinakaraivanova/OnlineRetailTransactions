"""Shared logging configuration for all pipeline scripts."""

import logging
import os
import sys

from config import LOG_DIR


def get_logger(name: str) -> logging.Logger:
    """Create a logger that writes to both file and stdout."""
    log_path = os.path.join(LOG_DIR, f"{name}.log")
    os.makedirs(LOG_DIR, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        file_handler = logging.FileHandler(log_path)
        stream_handler = logging.StreamHandler(sys.stdout)

        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
        file_handler.setFormatter(formatter)
        stream_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)

    return logger
