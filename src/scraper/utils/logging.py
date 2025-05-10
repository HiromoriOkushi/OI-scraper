# src/scraper/utils/logging.py
import logging
import logging.handlers
import sys
from pathlib import Path
from typing import Dict, Any

def setup_logging(config: Dict[str, Any]) -> logging.Logger:
    """
    Configures logging for the application.
    """
    log_level_str = config.get("level", "INFO").upper()
    log_level = getattr(logging, log_level_str, logging.INFO)

    log_format = config.get("format", "%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    
    # Get the root logger
    root_logger = logging.getLogger() # Gets the root logger
    root_logger.setLevel(log_level) # Set level on root logger
    
    # Remove any existing handlers to avoid duplication if setup_logging is called multiple times
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    formatter = logging.Formatter(log_format)

    # Console Handler
    if config.get("console", True):
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        console_handler.setLevel(log_level) # Console handler respects the global log level
        root_logger.addHandler(console_handler)

    # File Handler
    if "file" in config and config["file"]:
        log_file_path_str = config["file"]
        log_file_path = Path(log_file_path_str)
        
        # Ensure log directory exists
        log_file_path.parent.mkdir(parents=True, exist_ok=True)

        max_bytes = config.get("max_size", 10 * 1024 * 1024)  # Default 10MB
        backup_count = config.get("backup_count", 5)

        file_handler = logging.handlers.RotatingFileHandler(
            log_file_path,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        file_handler.setLevel(log_level) # File handler also respects the global log level
        root_logger.addHandler(file_handler)

    # Silence overly verbose libraries if necessary
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("selenium").setLevel(logging.INFO)


    # Return a specific logger for the application, not the root logger,
    # so that app messages are distinct from library messages if needed.
    # Or, if you want all subsequent loggers to inherit this config,
    # configuring the root logger is fine.
    app_logger = logging.getLogger("scraper") # Main application logger
    app_logger.info(f"Logging setup complete. Level: {log_level_str}, File: {config.get('file')}")
    
    return app_logger # Return the specific application logger