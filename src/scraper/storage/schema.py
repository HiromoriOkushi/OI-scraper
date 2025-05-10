# src/scraper/storage/schema.py

# Define the SQLite schema with optimization for insider trading data
SCHEMA_DEFINITIONS = {
    'insider_trades': '''
        CREATE TABLE IF NOT EXISTS insider_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hash_id TEXT UNIQUE NOT NULL,          -- Unique identifier hash for the trade event
            filing_date TEXT NOT NULL,             -- Filing date in YYYY-MM-DD format
            trade_date TEXT NOT NULL,              -- Trade date in YYYY-MM-DD format
            ticker TEXT NOT NULL,                  -- Stock ticker symbol
            company_name TEXT,                     -- Company name (can be null if not found)
            insider_name TEXT NOT NULL,            -- Insider name
            title TEXT,                            -- Insider's title
            trade_type TEXT NOT NULL,              -- P (Purchase), S (Sale), S+ (Sale+Exercise) etc.
            price REAL,                            -- Share price (can be null)
            quantity INTEGER,                      -- Number of shares (can be null)
            owned INTEGER,                         -- Shares owned after transaction (can be null)
            delta_own REAL,                        -- Percentage change in ownership (can be null, stored as decimal e.g. 0.05 for 5%)
            value REAL,                            -- Total transaction value (can be null)
            form_url TEXT,                         -- Link to SEC Form 4 (can be null)
            source TEXT NOT NULL,                  -- Source section on OpenInsider (e.g., latest_filings)
            scraped_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, -- Timestamp when record was scraped/inserted
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Timestamp when record was last updated (e.g., if re-scraped)
        );
    ''',
    
    'source_metadata': '''
        CREATE TABLE IF NOT EXISTS source_metadata (
            source_name TEXT PRIMARY KEY,          -- Source section name (e.g., latest_filings)
            last_content_hash TEXT,                -- Hash of the raw HTML content of the last successful scrape for this source
            last_scraped_newest_trade_hash TEXT,   -- Hash of the newest trade found during the last scrape of this source
            last_successful_scrape_at TEXT,        -- Timestamp of the last successful scrape for this source
            last_checked_at TEXT                   -- Timestamp when this source was last checked for updates
        );
    '''
}

# Define indices for efficient queries
INDICES = [
    # For insider_trades table
    'CREATE INDEX IF NOT EXISTS idx_trades_filing_date ON insider_trades(filing_date DESC)', # Recent first
    'CREATE INDEX IF NOT EXISTS idx_trades_trade_date ON insider_trades(trade_date DESC)',   # Recent first
    'CREATE INDEX IF NOT EXISTS idx_trades_ticker ON insider_trades(ticker)',
    'CREATE INDEX IF NOT EXISTS idx_trades_insider_name ON insider_trades(insider_name)',
    'CREATE INDEX IF NOT EXISTS idx_trades_trade_type ON insider_trades(trade_type)',
    'CREATE INDEX IF NOT EXISTS idx_trades_source ON insider_trades(source)',
    'CREATE INDEX IF NOT EXISTS idx_trades_hash_id ON insider_trades(hash_id)', # Already unique, but explicit index can help query planner
    'CREATE INDEX IF NOT EXISTS idx_trades_scraped_at ON insider_trades(scraped_at DESC)',

    # For source_metadata table (PRIMARY KEY source_name is already indexed)
    # No other indices typically needed for source_metadata as it's small and queried by PK.
]

# Define pragmas for performance optimization and integrity
PRAGMAS = [
    'PRAGMA journal_mode=WAL;',        # Write-Ahead Logging for concurrent access and performance
    'PRAGMA synchronous=NORMAL;',      # Balance between safety and speed. NORMAL is good with WAL.
    'PRAGMA cache_size=-20000;',       # Cache size in KiB (e.g., -20000 for 20MB). Negative value is KiB.
    'PRAGMA temp_store=MEMORY;',       # Store temporary tables and indices in memory
    'PRAGMA foreign_keys=ON;',         # Enforce foreign key constraints (if any are added later)
    'PRAGMA busy_timeout=5000;'        # Wait 5 seconds if DB is locked before failing
]

# It's good practice to add a trigger to update `updated_at` on row modification.
TRIGGERS = {
    'update_insider_trades_updated_at': '''
        CREATE TRIGGER IF NOT EXISTS update_insider_trades_updated_at
        AFTER UPDATE ON insider_trades
        FOR EACH ROW
        BEGIN
            UPDATE insider_trades SET updated_at = CURRENT_TIMESTAMP
            WHERE id = OLD.id;
        END;
    '''
}