#!/usr/bin/env python
import sys
from pathlib import Path
from datetime import datetime

# Add proper error handling for imports
try:
    from migrator.cli.commands import cli
    from migrator.utils.logger import LoggerConfig
except ImportError as e:
    print(f"Error importing required modules: {e}")
    print("Please ensure the package is installed correctly (pip install -e .)")
    sys.exit(1)

def setup_environment():
    """Setup basic environment configuration"""
    # Ensure logs directory exists
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    
    # Generate unique log filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = logs_dir / f"migrator_{timestamp}.log"
    
    return log_file

def main():
    """Main entry point with error handling"""
    try:
        # Setup environment and get log file path
        log_file = setup_environment()
        
        # Configure logging
        logger_config = LoggerConfig()
        logger = logger_config.setup_logging(
            log_level="INFO",
            log_file=str(log_file),
            format_string="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        logger.info(f"Starting Migrator - Log file: {log_file}")

        # Run CLI
        cli()

    except Exception as e:
        print(f"Error running migrator: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    main()