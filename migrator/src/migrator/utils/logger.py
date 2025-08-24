import logging
import logging.handlers
import os
from typing import Optional

class LoggerConfig:
    """Singleton class to manage application-wide logging configuration."""
    _instance = None
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not LoggerConfig._initialized:
            self.logger = logging.getLogger('migrator')
            # Set up a basic console handler by default
            self._setup_default_logging()
            LoggerConfig._initialized = True

    def _setup_default_logging(self):
        """Set up basic console logging as a default configuration."""
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(
                logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
            )
            self.logger.addHandler(handler)

    def setup_logging(
        self,
        log_level: str = "INFO",
        log_file: Optional[str] = None,
        syslog_address: Optional[tuple[str, int]] = None,
        format_string: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    ):
        self.logger.handlers.clear()
        
        level = getattr(logging, log_level.upper())
        self.logger.setLevel(level)

        formatter = logging.Formatter(format_string)

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        self.logger.addHandler(stream_handler)

        if log_file:
            os.makedirs(os.path.dirname(log_file), exist_ok=True)
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)

        if syslog_address:
            syslog_handler = logging.handlers.SysLogHandler(address=syslog_address)
            syslog_handler.setFormatter(formatter)
            self.logger.addHandler(syslog_handler)
        return self.get_logger()

    def get_logger(self) -> logging.Logger:
        """Get the configured logger instance."""
        return self.logger


def get_logger() -> logging.Logger:
    """
    Get the application logger instance.
    If the logger hasn't been explicitly configured yet,
    it will use a default console logger configuration.
    """
    return LoggerConfig().get_logger()
