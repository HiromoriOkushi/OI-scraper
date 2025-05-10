# src/scraper/types.py
from typing import Dict, Any, List, TypedDict, Optional
from pydantic import BaseModel, Field, validator
from datetime import date

# Using Pydantic for structured data and validation
class InsiderTrade(BaseModel):
    hash_id: str = Field(..., description="Unique identifier hash for the trade")
    filing_date: date = Field(..., description="Filing date")
    trade_date: date = Field(..., description="Trade date")
    ticker: str = Field(..., description="Stock ticker symbol")
    company_name: str = Field(..., description="Company name")
    insider_name: str = Field(..., description="Insider name")
    title: Optional[str] = Field(None, description="Insider's title")
    trade_type: str = Field(..., description="P (Purchase), S (Sale), etc.") # Could be an Enum
    price: Optional[float] = Field(None, description="Share price")
    quantity: Optional[int] = Field(None, description="Number of shares traded")
    owned: Optional[int] = Field(None, description="Shares owned after transaction")
    delta_own: Optional[float] = Field(None, description="Percentage change in ownership")
    value: Optional[float] = Field(None, description="Total transaction value") # Changed to float, can be large
    form_url: Optional[str] = Field(None, description="Link to SEC Form 4")
    source: str = Field(..., description="Source section on OpenInsider (e.g., latest_filings)")
    # created_at is added by the database automatically

    # Pydantic allows custom validators if needed, e.g., for date formats or positive numbers
    # @validator('ticker')
    # def ticker_must_be_uppercase(cls, v):
    #     if not v.isupper():
    #         raise ValueError('Ticker must be uppercase')
    #     return v

# Raw parsed data structure before Pydantic conversion
RawTradeData = Dict[str, Any]
ParsedTrades = List[RawTradeData] # List of dicts after initial parsing
ValidatedTrades = List[InsiderTrade] # List of Pydantic models

# Configuration dictionary structure (simplified)
Config = Dict[str, Any]