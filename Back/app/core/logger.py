import logging
import sys
from pathlib import Path

def setup_logger(name: str = "oysyn", level: int = logging.INFO) -> logging.Logger:
    """Настройка логгера с выводом в консоль и файл."""
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Избегаем дублирования хендлеров
    if logger.handlers:
        return logger
    
    # Формат
    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(name)s:%(funcName)s:%(lineno)d | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Консоль
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Файл
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    file_handler = logging.FileHandler(log_dir / "oysyn.log", encoding="utf-8")
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    return logger

# Глобальный логгер
logger = setup_logger()