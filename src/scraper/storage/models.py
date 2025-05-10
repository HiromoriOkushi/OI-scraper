# src/scraper/storage/models.py
# This file would typically contain ORM models if using SQLAlchemy or similar.
# Since we are using raw SQLite with Pydantic for data structure,
# our primary "models" are the Pydantic classes in `src/scraper/types.py`.

# For direct DB interaction without a full ORM, we might define helper functions
# or classes here that map Pydantic models to SQL, but much of that logic
# will be in `database.py` and `query_builder.py`.

# Example: Placeholder if we wanted to represent table structures here
# (though schema.py already defines them for creation)

from typing import Optional
from pydantic import BaseModel, Field
from datetime import date, datetime

# Re-using the Pydantic model from types.py for data representation
from ..types import InsiderTrade as InsiderTradeData

class SourceMetadata(BaseModel):
    """Pydantic model for data in the source_metadata table."""
    source_name: str = Field(..., description="Source section name (e.g., latest_filings)")
    last_content_hash: Optional[str] = Field(None, description="Hash of the raw HTML content")
    last_scraped_newest_trade_hash: Optional[str] = Field(None, description="Hash of the newest trade from last scrape")
    last_successful_scrape_at: Optional[datetime] = Field(None, description="Timestamp of last successful scrape")
    last_checked_at: Optional[datetime] = Field(None, description="Timestamp when source was last checked")

    class Config:
        #orm_mode = True # For compatibility if ever used with an ORM-like tool
        # Pydantic v2:
        from_attributes = True