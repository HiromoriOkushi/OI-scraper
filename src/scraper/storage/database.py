# src/scraper/storage/database.py
import sqlite3
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional, Union
from datetime import date, datetime
import threading

from pydantic import ValidationError

from .schema import SCHEMA_DEFINITIONS, INDICES, PRAGMAS, TRIGGERS
from .query_builder import SQLQueryBuilder
from ..types import InsiderTrade as InsiderTradePydanticModel, ValidatedTrades, RawTradeData
from ..exceptions import DatabaseError, DataValidationError
from ..utils.validation import validate_trade_data
from .models import SourceMetadata

logger = logging.getLogger(__name__)

class InsiderTradeDatabase:
    """
    Manages storage of insider trading data in SQLite.
    Optimized for write performance with batch operations and data integrity.
    """
    _thread_local = threading.local() # For thread-safe connections

    def __init__(self, db_path: str, config: Optional[Dict[str, Any]] = None):
        self.db_path = Path(db_path)
        self.db_config = config.get("database", {}) if config else {}
        self._ensure_db_directory()
        # Connection is managed per thread or on demand, not stored as instance var directly for multithreading
        # self._init_db_if_needed() # Initial call to setup schema on startup

    def _ensure_db_directory(self):
        """Ensure the directory for the SQLite database file exists."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def _get_connection(self) -> sqlite3.Connection:
        """Gets a thread-local database connection."""
        if not hasattr(self._thread_local, 'connection') or self._thread_local.connection is None:
            try:
                logger.debug(f"Attempting to connect to database: {self.db_path}")
                # `check_same_thread=False` is generally discouraged but might be used if you are very careful
                # with how connections are shared or if you use a connection pool.
                # For thread-local, it's usually not needed as each thread has its own connection.
                # The `timeout` parameter for connect is for waiting on locks.
                conn_timeout = self.db_config.get("timeout", 5.0) # seconds
                conn = sqlite3.connect(str(self.db_path), timeout=conn_timeout, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
                conn.row_factory = sqlite3.Row # Access columns by name
                self._thread_local.connection = conn
                logger.debug(f"New SQLite connection established for thread {threading.get_ident()}.")
                self._apply_pragmas(conn)
                self._create_schema_if_needed(conn) # Ensure schema on new connection too
            except sqlite3.Error as e:
                logger.error(f"Failed to connect to database {self.db_path}: {e}")
                raise DatabaseError(f"Database connection failed: {e}") from e
        return self._thread_local.connection

    def close_connection(self):
        """Closes the thread-local database connection if it exists."""
        if hasattr(self._thread_local, 'connection') and self._thread_local.connection:
            logger.debug(f"Closing SQLite connection for thread {threading.get_ident()}.")
            self._thread_local.connection.close()
            self._thread_local.connection = None

    def _apply_pragmas(self, conn: sqlite3.Connection):
        if self.db_config.get("optimize", True):
            logger.debug("Applying PRAGMA optimizations...")
            for pragma in PRAGMAS:
                try:
                    conn.execute(pragma)
                    logger.debug(f"Executed PRAGMA: {pragma}")
                except sqlite3.Error as e:
                    logger.warning(f"Failed to execute PRAGMA {pragma}: {e}")
            conn.commit()


    def _create_schema_if_needed(self, conn: sqlite3.Connection):
        """Creates database schema and indices if they don't exist."""
        try:
            with conn: # Automatic commit/rollback for DDL
                cursor = conn.cursor()
                for table_name, ddl_statement in SCHEMA_DEFINITIONS.items():
                    logger.debug(f"Ensuring table '{table_name}' exists...")
                    cursor.execute(ddl_statement)
                
                for index_statement in INDICES:
                    logger.debug(f"Ensuring index exists: {index_statement.split('ON')[0].strip()}...")
                    cursor.execute(index_statement)

                for trigger_name, trigger_ddl in TRIGGERS.items():
                    logger.debug(f"Ensuring trigger '{trigger_name}' exists...")
                    cursor.execute(trigger_ddl)

                logger.info("Database schema and indices verified/created successfully.")
        except sqlite3.Error as e:
            logger.error(f"Error during schema creation: {e}")
            raise DatabaseError(f"Schema creation failed: {e}") from e

    def _init_db_if_needed(self):
        """Initializes the database by getting a connection (which creates schema)."""
        conn = self._get_connection()
        # The schema creation is handled by _get_connection -> _create_schema_if_needed
        # self.close_connection() # Close immediately if only for init. Or keep open if app is starting.


    def _validate_and_prepare_trades(self, trades: List[RawTradeData]) -> ValidatedTrades:
        """Validates raw trade data and converts to Pydantic models."""
        validated_trades: ValidatedTrades = []
        for raw_trade in trades:
            try:
                # Convert date objects to ISO format strings for SQLite
                if isinstance(raw_trade.get('filing_date'), date):
                    raw_trade['filing_date'] = raw_trade['filing_date'].isoformat()
                if isinstance(raw_trade.get('trade_date'), date):
                    raw_trade['trade_date'] = raw_trade['trade_date'].isoformat()
                
                # Pydantic validation
                # Using validate_trade_data utility (which uses InsiderTradePydanticModel)
                # This step might be redundant if trades are already Pydantic models
                if isinstance(raw_trade, InsiderTradePydanticModel):
                    validated_trade_model = raw_trade
                else:
                    validated_trade_model = validate_trade_data(raw_trade) # This raises ValidationError
                
                validated_trades.append(validated_trade_model)
            except ValidationError as e: # Pydantic's ValidationError
                logger.warning(f"Skipping trade due to validation error: {e.errors()}. Data: {raw_trade}")
            except DataValidationError as e: # Custom validation error
                logger.warning(f"Skipping trade due to validation error: {e}. Data: {raw_trade}")
            except Exception as e:
                logger.error(f"Unexpected error validating trade {raw_trade}: {e}")
        return validated_trades

    def insert_trades(self, trades: List[Union[RawTradeData, InsiderTradePydanticModel]]) -> int:
        """
        Inserts trades with bulk operations and deduplication (ON CONFLICT DO NOTHING).
        Returns the count of newly inserted (or affected) records.
        """
        if not trades:
            return 0

        # If trades are already Pydantic models, convert them to dicts for DB insertion
        # Otherwise, they are RawTradeData (dicts) and will be validated
        
        prepared_data_for_db: List[Dict[str, Any]] = []
        for trade_input in trades:
            if isinstance(trade_input, InsiderTradePydanticModel):
                # Convert Pydantic model to dict, ensuring dates are strings
                trade_dict = trade_input.model_dump(mode='json') # mode='json' helps with dates/enums
                # Pydantic v1: trade_dict = trade_input.dict()
                # Ensure date objects are converted to ISO strings if not handled by model_dump
                if isinstance(trade_dict.get('filing_date'), date):
                     trade_dict['filing_date'] = trade_dict['filing_date'].isoformat()
                if isinstance(trade_dict.get('trade_date'), date):
                     trade_dict['trade_date'] = trade_dict['trade_date'].isoformat()
                prepared_data_for_db.append(trade_dict)
            elif isinstance(trade_input, dict):
                # Validate and prepare raw dicts (this path might be less common if pipeline produces Pydantic models)
                try:
                    # This will convert dates to strings if they are date objects in the dict
                    validated_model = self._validate_and_prepare_trades([trade_input])
                    if validated_model:
                        prepared_data_for_db.append(validated_model[0].model_dump(mode='json'))
                except (DataValidationError, ValidationError) as e:
                     logger.warning(f"Skipping invalid trade data during insert prep: {trade_input}, Error: {e}")
            else:
                logger.error(f"Unsupported trade data type for insertion: {type(trade_input)}")
                continue
        
        if not prepared_data_for_db:
            logger.info("No valid trades to insert after preparation.")
            return 0

        conn = self._get_connection()
        inserted_count = 0
        try:
            with conn: # Transaction handling
                cursor = conn.cursor()
                
                # We need to build the query based on the keys of the first item,
                # assuming all items have the same structure (Pydantic models ensure this).
                if not prepared_data_for_db: return 0
                
                first_trade_dict = prepared_data_for_db[0]
                query, _ = SQLQueryBuilder.build_insert_on_conflict_do_nothing(
                    "insider_trades",
                    first_trade_dict # Used to determine column names
                )
                # The values part of the query needs to match the order of columns in the first_trade_dict
                # when preparing `data_tuples`.
                
                # Prepare list of tuples for executemany
                # Ensure all dicts have the same keys in the same order as `first_trade_dict.keys()`
                ordered_keys = list(first_trade_dict.keys())
                data_tuples = [
                    tuple(item.get(key) for key in ordered_keys) for item in prepared_data_for_db
                ]

                batch_size = self.db_config.get("batch_size", 100)
                for i in range(0, len(data_tuples), batch_size):
                    batch = data_tuples[i:i + batch_size]
                    cursor.executemany(query, batch)
                    # For "ON CONFLICT DO NOTHING", cursor.rowcount reflects rows that were actually inserted or changed.
                    # If a row was ignored due to conflict, it's not counted.
                    # SQLite's `sqlite3_changes()` C function returns the number of rows modified, inserted or deleted by the most recently completed INSERT, UPDATE or DELETE statement.
                    # cursor.rowcount might be -1 or the number of rows in the batch depending on Python's DB-API driver version for SQLite.
                    # To get an accurate count of *newly* inserted rows:
                    # One way is to query count before and after, or use `SELECT changes()` but that needs separate exec.
                    # A simpler approach for `ON CONFLICT DO NOTHING` is to assume `cursor.rowcount` (if > -1) is the number of successful operations.
                    # Let's query for changes explicitly to be sure.
                    
                    # This gets changes from the LAST statement (the executemany).
                    # It's more reliable to sum this up.
                    # Note: For `executemany`, `rowcount` is often -1.
                    # To get the true number of inserted rows with ON CONFLICT DO NOTHING, it's tricky without another query.
                    # A common pattern is to select count before and after, or use a different upsert strategy
                    # that allows counting. With "DO NOTHING", it's hard to get count of *new* rows from rowcount.
                    # `conn.total_changes` accumulates changes over the connection's lifetime.
                    # Let's assume for now that we want to report the number of *attempted* inserts in batch that didn't fail.
                    # Or, we can use a workaround:
                    # For "INSERT OR IGNORE" (synonym for ON CONFLICT DO NOTHING in some contexts), cursor.rowcount gives number of rows *inserted*.
                    # For "INSERT ... ON CONFLICT ... DO NOTHING", cursor.rowcount behavior is less consistent across drivers/versions.
                    
                    # A more robust way if you need the exact count of *newly inserted* rows:
                    # Iterate and insert one by one, checking rowcount, but this loses batch performance.
                    # Or, if hash_ids are available, select existing hash_ids first, then insert only new ones.
                    
                    # For this implementation, we'll rely on total changes within the transaction if possible,
                    # or just report that the batch was processed.
                    # cursor.execute("SELECT changes()")
                    # inserted_in_batch = cursor.fetchone()[0]
                    # inserted_count += inserted_in_batch
                    # logger.debug(f"Batch insert: {inserted_in_batch} new rows added in this batch.")

                # A simpler way: if the insert affects rows, it means they were new.
                # After all batches, conn.commit() happens due to `with conn:`.
                # The `total_changes` before and after the entire operation can give a clue.
                # For now, let's count based on `cursor.rowcount` if it's helpful, or just confirm execution.
                # `cursor.rowcount` for `executemany` with `ON CONFLICT DO NOTHING` is often not reliable for *new* rows.
                # We will return the number of trades *attempted* to insert that were valid.
                # To get actual inserted, would need to select counts or use INSERT OR IGNORE.
                
                # Let's try to get the actual number of changes from the connection.
                # This will be the total for all `executemany` calls in this transaction.
                # This is a bit of a hack as it depends on when SQLite updates this value.
                # A robust way is to use `SELECT changes()` after each `executemany`.
                # For simplicity, we'll count based on how many items were in the `prepared_data_for_db`
                # and log the success. The "new records" count is tricky here without more queries.
                
                # The prompt asked for "Return count of new records".
                # With ON CONFLICT DO NOTHING, the most straightforward is to assume all successful operations
                # in the batch were new, unless the DB itself reports otherwise effectively.
                # The `sqlite3` module's `cursor.rowcount` for `executemany` of `INSERT ... ON CONFLICT ... DO NOTHING`
                # is typically -1 or the number of parameter sets. It does NOT directly give new rows.
                # To get this, one might select hash_ids before inserting.
                # For now, we'll report that the operation was submitted.
                # A better insert query for counting new rows is "INSERT OR IGNORE".
                # Let's switch to that for `insider_trades` as it's simpler for counting.

                # Rebuild query for INSERT OR IGNORE:
                columns_list = list(prepared_data_for_db[0].keys())
                cols_str = ", ".join(columns_list)
                vals_placeholder_str = ", ".join(["?"] * len(columns_list))
                insert_or_ignore_query = f"INSERT OR IGNORE INTO insider_trades ({cols_str}) VALUES ({vals_placeholder_str})"
                
                current_total_changes = conn.total_changes
                
                for i in range(0, len(data_tuples), batch_size):
                    batch = data_tuples[i:i + batch_size]
                    cursor.executemany(insert_or_ignore_query, batch)
                    # `cursor.rowcount` with `INSERT OR IGNORE` should give the number of rows actually inserted in the last operation.
                    # However, for `executemany`, it's often still -1 or total statements.
                    # The `conn.total_changes` is more reliable for the cumulative effect within a transaction.
                
                inserted_count = conn.total_changes - current_total_changes

            logger.info(f"Successfully processed {len(prepared_data_for_db)} trades for DB insertion. Newly inserted: {inserted_count}.")
            return inserted_count
        except sqlite3.Error as e:
            logger.error(f"Database error during batch insert: {e}")
            raise DatabaseError(f"Batch insert failed: {e}") from e
        except Exception as e:
            logger.error(f"Unexpected error during insert_trades: {e}")
            raise DatabaseError(f"Unexpected error in insert_trades: {e}") from e


    def get_latest_trade_hash_for_source(self, source_name: str) -> Optional[str]:
        """
        Gets the hash_id of the most recent trade (by filing_date, then by id) for a given source.
        This is used to check if new trades have appeared at the top of the list for that source.
        """
        conn = self._get_connection()
        query = """
            SELECT hash_id
            FROM insider_trades
            WHERE source = ?
            ORDER BY filing_date DESC, id DESC 
            LIMIT 1
        """
        try:
            cursor = conn.cursor()
            cursor.execute(query, (source_name,))
            result = cursor.fetchone()
            return result['hash_id'] if result else None
        except sqlite3.Error as e:
            logger.error(f"Database error getting latest trade hash for source {source_name}: {e}")
            # raise DatabaseError(f"Failed to get latest trade hash: {e}") from e # Or return None
            return None


    # --- Methods for source_metadata table ---
    def update_source_metadata(self, metadata: SourceMetadata) -> None:
        """Upserts source metadata."""
        conn = self._get_connection()
        
        # Convert datetime to ISO string for SQLite
        data_dict = metadata.model_dump(mode='json') # Pydantic v2
        # data_dict = metadata.dict() # Pydantic v1
        if data_dict.get('last_successful_scrape_at') and isinstance(data_dict['last_successful_scrape_at'], datetime):
            data_dict['last_successful_scrape_at'] = data_dict['last_successful_scrape_at'].isoformat()
        if data_dict.get('last_checked_at') and isinstance(data_dict['last_checked_at'], datetime):
            data_dict['last_checked_at'] = data_dict['last_checked_at'].isoformat()

        query, values = SQLQueryBuilder.build_upsert_query(
            "source_metadata",
            data_dict,
            conflict_target_column="source_name"
        )
        try:
            with conn:
                cursor = conn.cursor()
                cursor.execute(query, values)
            logger.debug(f"Source metadata for '{metadata.source_name}' updated/inserted.")
        except sqlite3.Error as e:
            logger.error(f"Database error updating source metadata for {metadata.source_name}: {e}")
            raise DatabaseError(f"Failed to update source metadata: {e}") from e

    def get_source_metadata(self, source_name: str) -> Optional[SourceMetadata]:
        """Retrieves metadata for a given source."""
        conn = self._get_connection()
        query, values = SQLQueryBuilder.build_select_query(
            "source_metadata",
            conditions={"source_name": source_name},
            limit=1
        )
        try:
            cursor = conn.cursor()
            cursor.execute(query, values)
            row = cursor.fetchone()
            if row:
                # Convert ISO string dates back to datetime objects if needed by Pydantic model
                row_dict = dict(row)
                if row_dict.get('last_successful_scrape_at'):
                    row_dict['last_successful_scrape_at'] = datetime.fromisoformat(row_dict['last_successful_scrape_at'])
                if row_dict.get('last_checked_at'):
                    row_dict['last_checked_at'] = datetime.fromisoformat(row_dict['last_checked_at'])
                return SourceMetadata(**row_dict)
            return None
        except sqlite3.Error as e:
            logger.error(f"Database error getting source metadata for {source_name}: {e}")
            return None # Or raise
        except ValidationError as e:
            logger.error(f"Data validation error constructing SourceMetadata for {source_name} from DB: {e}")
            return None


    def get_all_source_names_from_db(self) -> List[str]:
        """Retrieves all unique source names stored in the insider_trades table."""
        conn = self._get_connection()
        query = "SELECT DISTINCT source FROM insider_trades"
        try:
            cursor = conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            return [row['source'] for row in results]
        except sqlite3.Error as e:
            logger.error(f"Database error getting all source names: {e}")
            return []

    def health_check(self) -> bool:
        """Performs a simple query to check database connectivity and schema presence."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            # Check if a key table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='insider_trades';")
            if cursor.fetchone():
                logger.info("Database health check: 'insider_trades' table exists.")
                return True
            else:
                logger.warning("Database health check: 'insider_trades' table NOT found.")
                return False
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False