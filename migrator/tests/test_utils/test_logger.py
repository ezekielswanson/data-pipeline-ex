import logging
import os
from pathlib import Path

import pytest

from migrator.utils.logger import LoggerConfig, get_logger

class TestLoggerConfig:
    def test_singleton_instance(self):
        """Test that LoggerConfig maintains singleton pattern"""
        logger1 = LoggerConfig()
        logger2 = LoggerConfig()
        assert logger1 is logger2

    def test_default_configuration(self):
        """Test default logger configuration"""
        logger_config = LoggerConfig()
        logger = logger_config.get_logger()
        
        assert logger.name == 'migrator'
        assert logger.level == logging.INFO
        assert len(logger.handlers) == 1
        assert isinstance(logger.handlers[0], logging.StreamHandler)

    def test_get_logger_function(self):
        """Test the get_logger utility function"""
        logger = get_logger()
        assert isinstance(logger, logging.Logger)
        assert logger.name == 'migrator'

    def test_custom_log_level(self, tmp_path):
        """Test setting custom log level"""
        logger_config = LoggerConfig()
        logger = logger_config.setup_logging(log_level="DEBUG")
        
        assert logger.level == logging.DEBUG

    def test_file_handler_creation(self, tmp_path):
        """Test adding a file handler"""
        log_file = tmp_path / "test.log"
        
        logger_config = LoggerConfig()
        logger = logger_config.setup_logging(log_file=str(log_file))
        
        # Check if file handler was added
        file_handlers = [h for h in logger.handlers if isinstance(h, logging.FileHandler)]
        assert len(file_handlers) == 1
        assert file_handlers[0].baseFilename == str(log_file)
        
        # Test logging to file
        test_message = "Test log message"
        logger.info(test_message)
        
        with open(log_file) as f:
            log_content = f.read()
            assert test_message in log_content

    def test_multiple_handlers(self, tmp_path):
        """Test configuration with multiple handlers"""
        log_file = tmp_path / "test.log"
        
        logger_config = LoggerConfig()
        logger = logger_config.setup_logging(
            log_level="INFO",
            log_file=str(log_file),
            syslog_address=None  # Not testing syslog in unit tests
        )
        
        # Should have both stream and file handlers
        assert len(logger.handlers) == 2
        assert any(isinstance(h, logging.StreamHandler) for h in logger.handlers)
        assert any(isinstance(h, logging.FileHandler) for h in logger.handlers)

    def test_custom_format(self, tmp_path):
        """Test custom format string"""
        log_file = tmp_path / "test.log"
        custom_format = "%(levelname)s - %(message)s"
        
        logger_config = LoggerConfig()
        logger = logger_config.setup_logging(
            log_file=str(log_file),
            format_string=custom_format
        )
        
        # Test logging with custom format
        logger.info("Test message")
        
        with open(log_file) as f:
            log_content = f.read()
            # Should not contain timestamp
            assert "INFO - Test message" in log_content

    def test_handler_clearing(self, tmp_path):
        """Test that handlers are properly cleared when reconfiguring"""
        logger_config = LoggerConfig()
        
        # First setup
        logger = logger_config.setup_logging()
        initial_handlers = len(logger.handlers)
        
        # Second setup
        logger = logger_config.setup_logging()
        
        # Should still have the same number of handlers
        assert len(logger.handlers) == initial_handlers

    @pytest.mark.integration
    def test_syslog_handler(self):
        """Test syslog handler configuration"""
        logger_config = LoggerConfig()
        logger = logger_config.setup_logging(
            syslog_address=('localhost', 514)
        )
        
        syslog_handlers = [h for h in logger.handlers 
                          if isinstance(h, logging.handlers.SysLogHandler)]
        assert len(syslog_handlers) == 1

    def test_log_directory_creation(self, tmp_path):
        """Test that log directories are created if they don't exist"""
        nested_log_path = tmp_path / "logs" / "nested" / "test.log"
        
        logger_config = LoggerConfig()
        logger = logger_config.setup_logging(log_file=str(nested_log_path))
        
        assert nested_log_path.parent.exists()
        logger.info("Test message")
        assert nested_log_path.exists()
