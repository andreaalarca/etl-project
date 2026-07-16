"""
Centralized logging utility for the ETL framework.
Provides a singleton logger that writes to daily log files in the format:
    logs/etl_YYYYMMDD.log
"""
import os
import logging
from datetime import datetime
import threading
from pathlib import Path


class ETLLogger:
    """
    Singleton logger for the ETL framework.
    Logs to a daily file named logs/etl_YYYYMMDD.log.
    """
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self):
        # Prevent re-initialization
        if self._initialized:
            return
        self._initialized = True

        # Logger name
        self.logger = logging.getLogger('etl_framework')
        self.logger.setLevel(logging.INFO)
        self.logger.propagate = False  # Don't propagate to root logger

        # Remove any existing handlers to avoid duplication
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)

        # Daily log file management
        self._current_date = None
        self._file_handler = None
        self._log_dir = Path('./logs')
        self._formatter = logging.Formatter(
            fmt='%(asctime)s %(levelname)s [%(job_name)s] [%(stage)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # Ensure logs directory exists
        self._log_dir.mkdir(parents=True, exist_ok=True)

    def _get_today_date_str(self):
        """Returns today's date as YYYYMMDD string."""
        return datetime.now().strftime('%Y%m%d')

    def _ensure_handler_for_today(self):
        """Ensures the logger has a file handler for today's date."""
        today_str = self._get_today_date_str()
        if today_str != self._current_date:
            # Remove old handler if exists
            if self._file_handler is not None:
                self.logger.removeHandler(self._file_handler)
                self._file_handler.close()
                self._file_handler = None

            # Create new handler for today
            log_file = self._log_dir / f'etl_{today_str}.log'
            self._file_handler = logging.FileHandler(log_file)
            self._file_handler.setFormatter(self._formatter)
            self.logger.addHandler(self._file_handler)

            self._current_date = today_str

    def info(self, job_name: str, stage: str, message: str):
        """Logs an INFO message with job name and stage."""
        self._ensure_handler_for_today()
        self.logger.info(message, extra={'job_name': job_name, 'stage': stage})

    def warning(self, job_name: str, stage: str, message: str):
        """Logs a WARNING message with job name and stage."""
        self._ensure_handler_for_today()
        self.logger.warning(message, extra={'job_name': job_name, 'stage': stage})

    def error(self, job_name: str, stage: str, message: str):
        """Logs an ERROR message with job name and stage."""
        self._ensure_handler_for_today()
        self.logger.error(message, extra={'job_name': job_name, 'stage': stage})


# Create a singleton instance for easy import
logger = ETLLogger()