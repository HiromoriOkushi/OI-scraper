# src/scraper/main.py
from pydantic import ValidationError
import threading
import logging
import time
import signal
import gc
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, List, Optional, Tuple

from .types import Config
from .extractors.request_manager import RequestManager
from .parsers.trade_parser import InsiderTradeParser
from .storage.database import InsiderTradeDatabase
from .types import InsiderTrade as InsiderTradePydanticModel, ParsedTrades, RawTradeData
from .exceptions import ScraperException, NetworkError, ParsingError, DatabaseError
from .utils.hash import generate_content_hash
from .storage.models import SourceMetadata # For source metadata handling
from .utils.concurrency import shutdown_executor

from .parsers import data_cleaner

logger = logging.getLogger(__name__)

class OpenInsiderScraper:
    """
    Main scraper class that orchestrates the extraction, parsing, and storage process.
    """

    def __init__(self, config: Config):
        self.config: Config = config
        self.scraper_config = config.get("scraper", {})
        self.monitoring_config = config.get("monitoring", {})
        self.advanced_config = config.get("advanced", {})
        
        self.base_url = self.scraper_config.get("base_url", "http://openinsider.com")
        
        # Initialize components
        try:
            self.request_manager = RequestManager(config)
            self.parser = InsiderTradeParser(base_url=self.base_url)
            self.db = InsiderTradeDatabase(config['database']['path'], config=config)
            self.db._init_db_if_needed() # Ensure DB is ready
        except ScraperException as e:
            logger.error(f"Failed to initialize scraper components: {e}")
            raise
        except Exception as e: # Catch any other init errors
            logger.error(f"Unexpected error during scraper initialization: {e}")
            raise ScraperException(f"Initialization failed: {e}")


        self.stop_event = threading.Event() # For graceful shutdown
        self._register_signal_handlers()

        self.processed_items_since_gc = 0
        self.gc_threshold = self.advanced_config.get("memory", {}).get("gc_threshold_items", 10000)
        
        logger.info("OpenInsiderScraper initialized successfully.")

    def _register_signal_handlers(self):
        signal.signal(signal.SIGINT, self._handle_shutdown_signal)
        signal.signal(signal.SIGTERM, self._handle_shutdown_signal)

    def _handle_shutdown_signal(self, signum, frame):
        logger.info(f"Shutdown signal {signal.Signals(signum).name} received. Stopping scraper...")
        self.stop_event.set()
        # Give threads some time to finish, then exit.
        # This might be handled more gracefully within run_continuous_monitoring loop


    def _fetch_page_content(self, url_path: str, source_name: str) -> Optional[str]:
        """Fetches HTML content for a given URL path."""
        full_url = f"{self.base_url.strip('/')}{url_path.strip()}"
        
        # OpenInsider uses query parameters to show more rows, e.g. `?maxnumrows=1000`
        # Let's make this configurable
        max_rows = self.scraper_config.get("max_rows_per_source", 1000) # Default to 1000, can be small for testing
        params = {"maxrows": str(max_rows)} # OpenInsider parameter name seems to be `maxrows` or `MaxRows`

        try:
            logger.info(f"Fetching content for source '{source_name}' from URL: {full_url} with params {params}")
            # Prefer GET with requests
            response = self.request_manager.get(full_url, params=params)
            return response.text
        except NetworkError as e:
            logger.error(f"Network error fetching {full_url} for source '{source_name}': {e}")
            # Check if Selenium fallback is configured and needed
            if self.advanced_config.get("selenium", {}).get("enabled", False) and "Circuit breaker open" not in str(e): # Don't use selenium if circuit is open
                logger.warning(f"Attempting Selenium fallback for {full_url} due to network error.")
                try:
                    return self.request_manager.get_with_selenium(full_url) # Selenium might not support params easily
                except ScraperException as se:
                    logger.error(f"Selenium fallback also failed for {full_url}: {se}")
                    return None
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching {full_url} for source '{source_name}': {e}")
            return None

    def scrape_source_page(self, source_name: str, source_config: Dict[str, Any]) -> Tuple[str, Optional[ParsedTrades]]:
        """
        Scrapes a single source page (e.g., "latest_filings").
        Returns the source name and the list of parsed trades, or None if failed.
        """
        if self.stop_event.is_set():
            logger.info(f"Scraping stopped for source {source_name} due to shutdown signal.")
            return source_name, None

        url_path = source_config.get("url_path")
        if not url_path:
            logger.error(f"URL path not configured for source: {source_name}. Skipping.")
            return source_name, None

        logger.info(f"Starting scrape for source: {source_name} (URL path: {url_path})")
        
        html_content = self._fetch_page_content(url_path, source_name)
        if not html_content:
            logger.warning(f"No HTML content retrieved for source: {source_name}. Skipping parsing.")
            return source_name, None

        # Content hash for change detection
        current_content_hash = generate_content_hash(html_content)
        source_meta = self.db.get_source_metadata(source_name)
        
        if source_meta and source_meta.last_content_hash == current_content_hash:
            logger.info(f"Content for source '{source_name}' has not changed since last scrape (hash: {current_content_hash[:10]}...). Skipping full parse.")
            # Update last_checked_at timestamp
            self.db.update_source_metadata(SourceMetadata(
                source_name=source_name,
                last_content_hash=source_meta.last_content_hash, # Keep old hash
                last_scraped_newest_trade_hash=source_meta.last_scraped_newest_trade_hash, # Keep old trade hash
                last_successful_scrape_at=source_meta.last_successful_scrape_at, # Keep old scrape time
                last_checked_at=datetime.now()
            ))
            return source_name, [] # Return empty list to indicate no new data from this path


        try:
            parsed_trades: ParsedTrades = self.parser.parse_trade_table(html_content, source_name)
            logger.info(f"Successfully parsed {len(parsed_trades)} trades from source: {source_name}")
            
            # Update source metadata after successful parse
            newest_trade_hash = parsed_trades[0]['hash_id'] if parsed_trades and 'hash_id' in parsed_trades[0] else None
            self.db.update_source_metadata(SourceMetadata(
                source_name=source_name,
                last_content_hash=current_content_hash,
                last_scraped_newest_trade_hash=newest_trade_hash,
                last_successful_scrape_at=datetime.now(),
                last_checked_at=datetime.now()
            ))
            return source_name, parsed_trades
        except ParsingError as e:
            logger.error(f"Parsing error for source {source_name}: {e}")
            return source_name, None
        except Exception as e:
            logger.error(f"Unexpected error during parsing for source {source_name}: {e}")
            return source_name, None


    def process_and_store_trades(self, parsed_trades_list: List[Optional[ParsedTrades]]) -> int:
        """
        Validates, and stores trades from multiple sources.
        Returns total number of newly inserted trades.
        """
        all_trades_to_store: List[RawTradeData] = []
        for trade_list_from_source in parsed_trades_list:
            if trade_list_from_source:
                all_trades_to_store.extend(trade_list_from_source)
        
        if not all_trades_to_store:
            logger.info("No trades to process and store.")
            return 0

        logger.info(f"Preparing to store {len(all_trades_to_store)} trades in database...")
        
        # Pydantic validation and conversion happens inside db.insert_trades if needed,
        # or we can do it here explicitly if trades are still raw dicts
        # For now, let's assume parser returns List[RawTradeData]
        
        # Convert RawTradeData to Pydantic models before sending to DB for strong typing
        validated_pydantic_trades: List[InsiderTradePydanticModel] = []
        for raw_trade in all_trades_to_store:
            try:
                # Ensure dates are in correct type (date object) if parser returned strings
                if isinstance(raw_trade.get('filing_date'), str):
                    raw_trade['filing_date'] = data_cleaner.parse_date_flexible(raw_trade['filing_date'])
                if isinstance(raw_trade.get('trade_date'), str):
                    raw_trade['trade_date'] = data_cleaner.parse_date_flexible(raw_trade['trade_date'])

                # Ensure required fields for Pydantic model are present
                # Example: 'hash_id' which is generated by parser.
                if 'hash_id' not in raw_trade or not raw_trade['hash_id']:
                    logger.warning(f"Skipping trade due to missing hash_id before Pydantic validation: {raw_trade.get('ticker')}")
                    continue

                validated_model = InsiderTradePydanticModel(**raw_trade)
                validated_pydantic_trades.append(validated_model)
            except ValidationError as e:
                logger.warning(f"Skipping trade due to Pydantic validation error before DB: {e.errors()}. Data: {raw_trade}")
            except Exception as e: # Catch any other error during this pre-validation
                logger.error(f"Unexpected error preparing trade for DB {raw_trade.get('ticker')}: {e}")


        if not validated_pydantic_trades:
            logger.info("No valid trades to store after Pydantic validation.")
            return 0

        try:
            newly_inserted_count = self.db.insert_trades(validated_pydantic_trades)
            logger.info(f"Successfully inserted {newly_inserted_count} new trades into the database.")
            
            self.processed_items_since_gc += len(validated_pydantic_trades)
            if self.processed_items_since_gc >= self.gc_threshold:
                logger.info(f"Processed {self.processed_items_since_gc} items. Triggering garbage collection.")
                gc.collect()
                self.processed_items_since_gc = 0
                
            return newly_inserted_count
        except DatabaseError as e:
            logger.error(f"Database error storing trades: {e}")
            return 0
        except Exception as e:
            logger.error(f"Unexpected error storing trades: {e}")
            return 0


    def perform_full_scrape(self, specific_sources: Optional[List[str]] = None) -> None:
        """
        Performs a complete scrape of all configured and enabled (or specified) sources.
        Uses a ThreadPoolExecutor for parallel scraping of sources.
        """
        logger.info(f"Starting full scrape... Target sources: {specific_sources or 'all enabled'}")
        start_time = time.monotonic()
        
        # ---- START TEMPORARY DEBUGGING CODE ----
        if specific_sources == ['latest_filings'] or (specific_sources is None and 'latest_filings' in self.scraper_config.get("sources", {})):
            logger.warning("TEMPORARY DEBUG: Forcing deletion of 'latest_filings' metadata before scrape.")
            try:
                conn = self.db._get_connection() # Get a connection
                with conn:
                    cursor = conn.cursor()
                    cursor.execute("DELETE FROM source_metadata WHERE source_name = ?", ('latest_filings',))
                    # Verify deletion immediately
                    cursor.execute("SELECT COUNT(*) FROM source_metadata WHERE source_name = ?", ('latest_filings',))
                    count_after_delete = cursor.fetchone()[0]
                    if count_after_delete == 0:
                        logger.info("TEMPORARY DEBUG: Successfully deleted 'latest_filings' metadata from DB.")
                    else:
                        logger.error("TEMPORARY DEBUG: FAILED to delete 'latest_filings' metadata from DB. Row still exists.")
                self.db.close_connection() # Close this specific connection if it's not thread-managed robustly for this one-off
                                           # Or rely on thread_local to be separate.
                                           # For safety, let's ensure it's a fresh context for the main scrape.
            except Exception as db_debug_e:
                logger.error(f"TEMPORARY DEBUG: Error during metadata deletion: {db_debug_e}")
        # ---- END TEMPORARY DEBUGGING CODE ----

        sources_to_scrape: Dict[str, Dict[str, Any]] = {}
        configured_sources = self.scraper_config.get("sources", {})
        
        if specific_sources:
            for source_name in specific_sources:
                if source_name in configured_sources and configured_sources[source_name].get("enabled", False):
                    sources_to_scrape[source_name] = configured_sources[source_name]
                elif source_name in configured_sources:
                    logger.warning(f"Source '{source_name}' is configured but not enabled. Skipping.")
                else:
                    logger.warning(f"Source '{source_name}' not found in configuration. Skipping.")
        else: # Scrape all enabled sources
            for source_name, source_config in configured_sources.items():
                if source_config.get("enabled", True): # Default to enabled if key missing
                    sources_to_scrape[source_name] = source_config
                else:
                    logger.info(f"Source '{source_name}' is disabled in config. Skipping.")
        
        if not sources_to_scrape:
            logger.info("No sources configured or enabled for scraping.")
            return

        all_parsed_trades_from_sources: List[Optional[ParsedTrades]] = []
        max_threads = self.scraper_config.get("max_threads", 4)
        
        with ThreadPoolExecutor(max_workers=max_threads, thread_name_prefix="ScraperThread") as executor:
            future_to_source = {
                executor.submit(self.scrape_source_page, name, conf): name
                for name, conf in sources_to_scrape.items()
            }
            
            for future in as_completed(future_to_source):
                if self.stop_event.is_set():
                    logger.info("Shutdown initiated, cancelling pending scrape tasks...")
                    # Attempt to cancel remaining futures
                    for f in future_to_source.keys(): # Iterate over original keys
                        if not f.done():
                            f.cancel()
                    break # Exit loop

                source_name_completed = future_to_source[future]
                try:
                    _, parsed_trades = future.result() # result() is (source_name, Optional[ParsedTrades])
                    if parsed_trades is not None: # Could be empty list if no change, or None if error
                        all_parsed_trades_from_sources.append(parsed_trades)
                        logger.info(f"Completed scraping source: {source_name_completed}, found {len(parsed_trades)} trades.")
                    else:
                        logger.warning(f"Scraping source {source_name_completed} resulted in no data or an error.")
                except Exception as exc:
                    logger.error(f"Source {source_name_completed} generated an exception: {exc}")
                    all_parsed_trades_from_sources.append(None) # Indicate failure for this source
        
        if self.stop_event.is_set():
            logger.info("Full scrape interrupted by shutdown signal.")
            # Cleanup already handled by shutdown_executor called in close() or main loop
            return

        total_newly_inserted = self.process_and_store_trades(all_parsed_trades_from_sources)
        
        end_time = time.monotonic()
        duration = end_time - start_time
        logger.info(f"Full scrape completed in {duration:.2f} seconds. Total new trades inserted: {total_newly_inserted}.")


    def check_for_updates(self, source_name: str) -> bool:
        """
        Checks a specific source for new data by comparing the hash of the newest trade.
        This is a lightweight check.
        Returns True if new data is suspected (triggering a full scrape of that source), False otherwise.
        """
        logger.info(f"Checking for updates in source: {source_name}...")
        source_config = self.scraper_config.get("sources", {}).get(source_name)
        if not source_config or not source_config.get("enabled", True):
            logger.warning(f"Source {source_name} not configured or disabled. Cannot check for updates.")
            return False

        # 1. Fetch only a small part of the page or headers if possible (not easy with OpenInsider's full table load)
        #    For OpenInsider, we likely need to fetch the page to see the top trades.
        #    We can limit rows fetched for this check.
        url_path = source_config["url_path"]
        temp_max_rows = self.scraper_config.get("max_rows_for_update_check", 20) # Fetch few rows for check
        
        html_content = None
        full_url = f"{self.base_url.strip('/')}{url_path.strip()}"
        try:
            response = self.request_manager.get(full_url, params={"maxrows": str(temp_max_rows)})
            html_content = response.text
        except NetworkError as e:
            logger.error(f"Network error during update check for {source_name}: {e}")
            return False # Assume no update or treat as error

        if not html_content:
            logger.warning(f"No content for update check on {source_name}.")
            return False
        
        # 2. Parse just the first few trades to get their hashes
        try:
            # Parse with the regular parser, but it will only see `temp_max_rows` trades
            parsed_trades: ParsedTrades = self.parser.parse_trade_table(html_content, source_name)
            if not parsed_trades:
                logger.info(f"No trades found during update check for {source_name}. Might be an empty page or parse issue.")
                # Update last_checked_at even if no trades found
                db_source_meta = self.db.get_source_metadata(source_name)
                if db_source_meta:
                    db_source_meta.last_checked_at = datetime.now()
                    self.db.update_source_metadata(db_source_meta)
                else: # First check, create metadata
                    self.db.update_source_metadata(SourceMetadata(source_name=source_name, last_checked_at=datetime.now()))
                return False # No trades means nothing new compared to existing DB state
            
            current_newest_trade_hash = parsed_trades[0].get('hash_id')
            if not current_newest_trade_hash:
                 logger.warning(f"Could not determine newest trade hash for {source_name} during update check.")
                 return False # Cannot determine, assume no update for safety

            # 3. Compare with the stored newest trade hash for this source
            source_meta = self.db.get_source_metadata(source_name)
            if source_meta:
                # Update last_checked_at timestamp
                source_meta.last_checked_at = datetime.now() 
                self.db.update_source_metadata(source_meta) # Save the check time

                if source_meta.last_scraped_newest_trade_hash == current_newest_trade_hash:
                    logger.info(f"No new trades detected for source '{source_name}' based on newest trade hash ({current_newest_trade_hash[:10]}...).")
                    return False
                else:
                    logger.info(f"Potential new trades detected for source '{source_name}'. "
                                f"DB newest: {source_meta.last_scraped_newest_trade_hash[:10] if source_meta.last_scraped_newest_trade_hash else 'None'}, "
                                f"Site newest: {current_newest_trade_hash[:10]}.")
                    return True # Hashes differ, potential update
            else:
                # No previous metadata, means it's the first time or metadata was lost.
                # Definitely needs a full scrape.
                logger.info(f"No existing metadata for source '{source_name}'. Update required.")
                # Create initial metadata with just the check time
                self.db.update_source_metadata(SourceMetadata(source_name=source_name, last_checked_at=datetime.now()))
                return True

        except ParsingError as e:
            logger.error(f"Parsing error during update check for {source_name}: {e}")
            return False # Treat as no update or error
        except Exception as e:
            logger.error(f"Unexpected error during update check for {source_name}: {e}")
            return False

    def run_continuous_monitoring(self) -> None:
        """
        Runs the scraper in a continuous monitoring loop.
        Periodically checks for updates and performs full scrapes.
        """
        if not self.monitoring_config.get("enabled", True):
            logger.info("Continuous monitoring is disabled in configuration.")
            return

        change_interval = self.monitoring_config.get("change_detection_interval", 600) # Default 10 mins
        full_refresh_interval = self.monitoring_config.get("full_refresh_interval", 3600) # Default 1 hour
        
        logger.info(
            f"Starting continuous monitoring. Update check interval: {change_interval}s, "
            f"Full refresh interval: {full_refresh_interval}s."
        )

        last_full_refresh_time = time.monotonic() - full_refresh_interval # Force first refresh

        try:
            while not self.stop_event.is_set():
                loop_start_time = time.monotonic()
                
                # Determine if it's time for a full refresh
                if (loop_start_time - last_full_refresh_time) >= full_refresh_interval:
                    logger.info("Scheduled full refresh triggered.")
                    self.perform_full_scrape()
                    last_full_refresh_time = time.monotonic()
                else:
                    logger.info("Performing periodic update checks...")
                    updated_sources = []
                    sources_to_check = [
                        name for name, conf in self.scraper_config.get("sources", {}).items() if conf.get("enabled", True)
                    ]
                    if not sources_to_check:
                        logger.warning("No sources enabled for monitoring.")
                    else:
                        # Could run checks in parallel too if many sources
                        for source_name in sources_to_check:
                            if self.stop_event.is_set(): break
                            if self.check_for_updates(source_name):
                                updated_sources.append(source_name)
                        
                        if self.stop_event.is_set(): break

                        if updated_sources:
                            logger.info(f"Updates detected for sources: {updated_sources}. Performing targeted scrape.")
                            self.perform_full_scrape(specific_sources=updated_sources)
                        else:
                            logger.info("No updates detected in any source during periodic check.")
                
                if self.stop_event.is_set(): break # Check again after operations

                # Wait for the next cycle
                elapsed_time = time.monotonic() - loop_start_time
                wait_time = max(0, change_interval - elapsed_time)
                
                if wait_time > 0:
                    logger.info(f"Monitoring loop completed. Waiting {wait_time:.2f} seconds for next cycle.")
                    # Use stop_event.wait for interruptible sleep
                    self.stop_event.wait(timeout=wait_time) 
                
                if self.stop_event.is_set(): break # Final check before looping

        except KeyboardInterrupt: # Should be caught by signal handler, but as fallback
            logger.info("KeyboardInterrupt received in monitoring loop. Shutting down.")
            self.stop_event.set()
        except Exception as e:
            logger.error(f"Unhandled exception in monitoring loop: {e}", exc_info=True)
            # Consider if loop should continue or terminate on unhandled error
            self.stop_event.set() # Stop on critical errors
        finally:
            logger.info("Continuous monitoring loop finished.")
            self.close() # Ensure resources are cleaned up

    def close(self):
        """Cleans up resources like database connections and HTTP sessions."""
        logger.info("Closing OpenInsiderScraper resources...")
        if hasattr(self, 'request_manager') and self.request_manager:
            self.request_manager.close()
        if hasattr(self, 'db') and self.db:
            self.db.close_connection() # Close thread-local connection for main thread
        
        # If any global executors were used (not typical for this design, but if added)
        # shutdown_executor(...)
        
        logger.info("Scraper resources closed.")