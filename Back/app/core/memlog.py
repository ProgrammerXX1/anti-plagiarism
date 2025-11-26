# app/core/memlog.py
import os

from .logger import logger


def get_rss_mb() -> float:
    """
    Текущий RSS по /proc/self/statm.
    Возвращает MB, либо -1.0 если не удалось.
    """
    try:
        with open("/proc/self/statm") as f:
            parts = f.read().split()
            if len(parts) >= 2:
                pages = int(parts[1])
                page_size = os.sysconf("SC_PAGE_SIZE")
                return pages * page_size / (1024 * 1024)
    except Exception:
        pass
    return -1.0


def log_mem(prefix: str) -> None:
    rss = get_rss_mb()
    if rss < 0:
        logger.info(f"[mem] {prefix}: rss_mb=unknown")
    else:
        logger.info(f"[mem] {prefix}: rss_mb={rss:.1f} MB")
