"""Logging utilities for the dynamic Spark pipeline."""

import logging
import os
import sys
from pathlib import Path
from logging.handlers import RotatingFileHandler


def setup_logger(name: str, log_dir: str = None) -> logging.Logger:
    """
    Configure logger with file and console handlers.
    
    Args:
        name: Logger name (typically __name__)
        log_dir: Directory for log files (default: from LOG_PATH env or 'logs')
    
    Returns:
        Configured logger instance
    """
    # Determine log directory
    if log_dir is None:
        log_dir = os.getenv("LOG_PATH", "logs")
    
    logger = logging.getLogger(name)
    
    # Guard against duplicate handlers (if this function is called multiple times)
    if logger.handlers:
        return logger
    
    logger.setLevel(logging.DEBUG)
    
    # Ensure log directory exists with fallback strategy
    log_path = Path(log_dir)
    fallback_dir = Path("/tmp/airflow_logs")
    actual_log_dir = log_dir
    
    try:
        log_path.mkdir(parents=True, exist_ok=True)
        # Test write permissions
        test_file = log_path / ".write_test"
        test_file.touch()
        test_file.unlink()
        logger.info(f"Using log directory: {log_dir}")
    except (PermissionError, OSError) as e:
        # Fall back to /tmp directory
        actual_log_dir = str(fallback_dir)
        fallback_dir.mkdir(parents=True, exist_ok=True)
        logger.warning(f"Failed to create log dir {log_dir}: {e}")
        logger.warning(f"Using fallback log directory: {actual_log_dir}")
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    console_handler.setFormatter(console_formatter)
    
    # File handler with rotation
    file_handler = RotatingFileHandler(
        f"{actual_log_dir}/pipeline.log",
        maxBytes=10_000_000,  # 10 MB
        backupCount=5,
    )
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
    )
    file_handler.setFormatter(file_formatter)
    
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger
