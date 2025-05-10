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
        if not trades:
            return 0

        prepared_data_for_db: List[Dict[str, Any]] = []
        for trade_input in trades:
            if isinstance(trade_input, InsiderTradePydanticModel):
                trade_dict = trade_input.model_dump(mode='json')
                if isinstance(trade_dict.get('filing_date'), date):
                     trade_dict['filing_date'] = trade_dict['filing_date'].isoformat()
                if isinstance(trade_dict.get('trade_date'), date):
                     trade_dict['trade_date'] = trade_dict['trade_date'].isoformat()
                prepared_data_for_db.append(trade_dict)
            elif isinstance(trade_input, dict):
                try:
                    validated_model_list = self._validate_and_prepare_trades([trade_input])
                    if validated_model_list:
                        prepared_data_for_db.append(validated_model_list[0].model_dump(mode='json'))
                except (DataValidationError, ValidationError) as e:
                     logger.warning(f"Skipping invalid trade data during insert prep: {trade_input}, Error: {e}")
            else:
                logger.error(f"Unsupported trade data type for insertion: {type(trade_input)}")
                continue
        
        if not prepared_data_for_db:
            logger.info("No valid trades to insert after preparation.")
            return 0

        conn = self._get_connection()
        
        try:
            with conn: # Transaction ensures atomicity for all batches
                cursor = conn.cursor()
                
                first_trade_dict = prepared_data_for_db[0]
                columns_list = list(first_trade_dict.keys())
                cols_str = ", ".join(columns_list)
                vals_placeholder_str = ", ".join(["?"] * len(columns_list))
                
                # Using INSERT OR IGNORE for simpler "new rows" counting via total_changes
                sql_query_to_execute = f"INSERT OR IGNORE INTO insider_trades ({cols_str}) VALUES ({vals_placeholder_str})"
                logger.debug(f"Executing batch insert with query: {sql_query_to_execute}")

                data_tuples = [
                    tuple(item.get(key) for key in columns_list) for item in prepared_data_for_db
                ]

                batch_size = self.db_config.get("batch_size", 100)
                
                # Get total changes on the connection BEFORE this batch operation begins
                # Note: total_changes is for the lifetime of the connection. 
                # For changes within this specific transaction, this is the best we can do with Python's sqlite3 API
                # without querying "SELECT changes()" after each batch.
                initial_total_changes = conn.total_changes 
                
                for i in range(0, len(data_tuples), batch_size):
                    batch = data_tuples[i:i + batch_size]
                    cursor.executemany(sql_query_to_execute, batch)
                
                # Calculate newly inserted rows by the difference in total_changes for the connection
                # This works because the 'with conn:' block ensures all executemany calls are part of one transaction.
                newly_inserted_this_call = conn.total_changes - initial_total_changes

            logger.info(
                f"Successfully processed {len(prepared_data_for_db)} trades for DB insertion. "
                f"Newly inserted this call: {newly_inserted_this_call}."
            )
            return newly_inserted_this_call
            
        except sqlite3.Error as e:
            logger.error(f"Database error during batch insert: {e}")
            raise DatabaseError(f"Batch insert failed: {e}") from e
        except Exception as e:
            logger.error(f"Unexpected error during insert_trades: {e}", exc_info=True)
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