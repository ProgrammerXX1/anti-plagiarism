# app/core/logger.py
import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path
from .config import LOGS_DIR

def setup_logger(
    name: str = "plagio",
    filename: str = "plagio.log",
    level: int = logging.INFO,
) -> logging.Logger:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(level)

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s:%(funcName)s:%(lineno)d | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # file (rotating)
    fh = RotatingFileHandler(
        Path(LOGS_DIR, filename),
        maxBytes=5_000_000,
        backupCount=3,
        encoding="utf-8",
    )
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    # console
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    return logger


logger = setup_logger()
