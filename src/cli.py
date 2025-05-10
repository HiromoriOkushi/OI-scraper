# src/cli.py
import argparse
import logging # Already imported in setup_logging, but good for direct use
import sys
import os # For path manipulation if needed
from pathlib import Path
from typing import Dict, Any, List, Optional

# Adjust import path based on how this CLI script is run.
# If run as a module (python -m src.cli), this is fine.
# If run as script (python src/cli.py), Python path needs adjustment.
# Poetry handles this by installing the package.
from scraper.config import load_config, PROJECT_ROOT
from scraper.main import OpenInsiderScraper
from scraper.utils.logging import setup_logging
from scraper.exceptions import ScraperException, ConfigurationError

# Get a logger for the CLI module itself
cli_logger = logging.getLogger("cli")


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="OpenInsider Scraper: Extracts insider trading data from OpenInsider.com",
        formatter_class=argparse.RawTextHelpFormatter # For better help text formatting
    )
    
    # Path to the config directory, relative to project root
    default_config_dir = PROJECT_ROOT / "config"

    parser.add_argument(
        "--config", "-c",
        type=str,
        help=(
            "Path to a specific YAML configuration file. \n"
            "If not provided, 'default.yaml' in the config directory is used, \n"
            "potentially merged with an environment-specific file (e.g., 'development.yaml')."
        )
    )
    
    parser.add_argument(
        "--env", "-e",
        type=str,
        default="development", # Default to 'development' to load dev overrides if present
        help=(
            "Environment configuration to use (e.g., 'development', 'production'). \n"
            "This will look for '<env>.yaml' in the same directory as the base config \n"
            "and merge it. Default: 'development'."
        )
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Command to execute", required=True)
    
    # Full scrape command
    full_parser = subparsers.add_parser("full", help="Perform a full scrape of specified or all enabled sources.")
    full_parser.add_argument(
        "--sources", # Changed from 'sections' to 'sources' for consistency
        type=str,
        nargs="+",
        metavar="SOURCE_NAME",
        help="Specific source names (keys from config's scraper.sources) to scrape (default: all enabled sources)."
    )
    
    # Monitor command
    monitor_parser = subparsers.add_parser("monitor", help="Run continuous monitoring for new trades.")
    monitor_parser.add_argument(
        "--interval",
        type=int,
        metavar="SECONDS",
        help="Override monitoring check interval (seconds). Uses config if not set."
    )
    monitor_parser.add_argument(
        "--full-refresh-interval",
        type=int,
        metavar="SECONDS",
        help="Override full refresh interval during monitoring (seconds). Uses config if not set."
    )
    
    # Check command
    check_parser = subparsers.add_parser("check", help="Perform a one-time check for updates on specified or all sources.")
    check_parser.add_argument(
        "--sources", # Changed from 'sections' to 'sources'
        type=str,
        nargs="+",
        metavar="SOURCE_NAME",
        help="Specific source names to check (default: all enabled sources)."
    )

    # Database health check command
    db_parser = subparsers.add_parser("db-health", help="Check the health and connectivity of the database.")


    return parser.parse_args()


def main_cli() -> int: # Renamed to avoid conflict if this file is imported
    """Main entry point for the CLI."""
    args = parse_args()
    
    # Determine config path and environment
    # `load_config` handles merging default with env-specific or a direct path.
    # args.config (specific file path) takes precedence over args.env if both are somehow used.
    
    effective_config_path: Optional[str] = args.config # If user provided a direct path
    effective_env: Optional[str] = args.env if not args.config else None # Use env only if no specific path given
                                                                        # Or, could always use env to merge on top of specific path.
                                                                        # Current `load_config` logic:
                                                                        # - If config_path is given, it's primary.
                                                                        # - If env is also given, it tries to find <env>.yaml relative to config_path's dir.
                                                                        # - If no config_path, uses default.yaml, then merges <env>.yaml if env is given.

    # Set up logging FIRST, using a minimal config if full config load fails
    temp_log_config = {"level": "INFO", "console": True, "format": "%(asctime)s [%(levelname)s] %(name)s - %(message)s"}
    try:
        # Try to load config to get logging settings
        # The `load_config` function now uses PROJECT_ROOT for relative paths
        config = load_config(config_path=effective_config_path, env=effective_env)
        # Setup logging with loaded configuration
        # `setup_logging` returns the 'scraper' logger, but also configures root logger.
        # We use a generic 'cli' logger here for CLI messages.
        _ = setup_logging(config.get("logging", temp_log_config)) # Main app logger setup
        cli_logger.info(f"Logging configured using: {effective_config_path or 'default config with env ' + (effective_env or 'none')}.")

    except ConfigurationError as e:
        # If config load fails, setup basic console logging for error messages
        _ = setup_logging(temp_log_config)
        cli_logger.error(f"Configuration error: {e}")
        cli_logger.error("Please ensure your configuration files (e.g., config/default.yaml) are correctly formatted and accessible.")
        return 1
    except Exception as e: # Catch-all for other setup errors
        _ = setup_logging(temp_log_config)
        cli_logger.error(f"Critical error during initial setup: {e}", exc_info=True)
        return 1

    cli_logger.info(f"OpenInsider Scraper starting with command: '{args.command}'")
    cli_logger.debug(f"Full arguments: {args}")
    cli_logger.debug(f"Effective configuration loaded: {config}")


    scraper: Optional[OpenInsiderScraper] = None
    try:
        scraper = OpenInsiderScraper(config)
        
        if args.command == "full":
            target_sources = args.sources if args.sources else None # None means all enabled
            cli_logger.info(f"Running full scrape. Target sources: {target_sources or 'all enabled'}.")
            scraper.perform_full_scrape(specific_sources=target_sources)
        
        elif args.command == "monitor":
            # Override config if CLI args are provided
            if args.interval is not None:
                config["monitoring"]["change_detection_interval"] = args.interval
                cli_logger.info(f"Overriding monitoring interval to {args.interval} seconds.")
            if args.full_refresh_interval is not None:
                config["monitoring"]["full_refresh_interval"] = args.full_refresh_interval
                cli_logger.info(f"Overriding full refresh interval to {args.full_refresh_interval} seconds.")
            
            scraper.run_continuous_monitoring() # This loop handles its own logging
        
        elif args.command == "check":
            # Default to all enabled sources if args.sources is None
            sources_to_check = args.sources
            if not sources_to_check: # If --sources not given, check all enabled
                sources_to_check = [
                    name for name, conf in config.get("scraper", {}).get("sources", {}).items() if conf.get("enabled", True)
                ]
            if not sources_to_check:
                cli_logger.warning("No sources specified or enabled to check.")
            else:
                cli_logger.info(f"Checking for updates on sources: {sources_to_check}")
                updates_found_count = 0
                for source_name in sources_to_check:
                    if scraper.stop_event.is_set(): break # Allow interruption
                    if scraper.check_for_updates(source_name):
                        cli_logger.info(f"Updates found for source: {source_name}")
                        updates_found_count += 1
                    else:
                        cli_logger.info(f"No updates found for source: {source_name}")
                cli_logger.info(f"Update check complete. Updates potentially found in {updates_found_count} source(s).")
        
        elif args.command == "db-health":
            cli_logger.info("Performing database health check...")
            if scraper.db.health_check():
                cli_logger.info("Database health check: PASSED. Connection successful and core table found.")
            else:
                cli_logger.error("Database health check: FAILED. Check logs for details.")
                return 1 # Indicate failure

        # No 'else' needed as subparsers are required

        cli_logger.info(f"Command '{args.command}' completed successfully.")
        return 0

    except KeyboardInterrupt:
        cli_logger.info("Keyboard interrupt received. Shutting down gracefully...")
        if scraper and hasattr(scraper, 'stop_event'): # Ensure scraper was initialized
            scraper.stop_event.set()
        # The scraper's own shutdown logic (e.g., in run_continuous_monitoring or close) should handle cleanup.
        return 130 # Standard exit code for Ctrl+C
    except ScraperException as e:
        cli_logger.error(f"A scraper-specific error occurred: {e}", exc_info=True) # exc_info for traceback on DEBUG
        return 2
    except Exception as e:
        cli_logger.error(f"An unexpected error occurred: {e}", exc_info=True)
        return 1
    finally:
        if scraper: # Ensure scraper was initialized before trying to close
            cli_logger.info("CLI attempting to close scraper resources...")
            scraper.close() # Close resources like DB connections, sessions
        cli_logger.info("OpenInsider Scraper CLI finished.")


if __name__ == "__main__":
    # This allows running `python src/cli.py ...`
    # For Poetry, it's better to use the script entry point: `poetry run openinsider-cli ...`
    
    # If run directly, the project root might not be in Python's path for imports like `from scraper.config...`
    # A common hack for direct script execution:
    if not Path("pyproject.toml").exists() and Path(__file__).parent.parent.name == "src":
         # If pyproject.toml is not in current dir, and we are in src/ something
         # This suggests we might be running from project_root/src or similar
         # Add project root to sys.path
         project_root_dir = Path(__file__).resolve().parent.parent.parent
         if (project_root_dir / "pyproject.toml").exists():
             sys.path.insert(0, str(project_root_dir))
             # print(f"DEBUG: Added {project_root_dir} to sys.path for direct script execution.", file=sys.stderr)
         else:
             print(f"Warning: Could not reliably determine project root for sys.path modification when running {__file__} directly.", file=sys.stderr)
             print("It's recommended to run using the Poetry script: `poetry run openinsider-cli`", file=sys.stderr)

    sys.exit(main_cli())