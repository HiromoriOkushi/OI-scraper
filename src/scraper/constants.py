# src/scraper/constants.py
from enum import Enum

# Default User Agent if not specified or rotation list is empty
DEFAULT_USER_AGENT = "OpenInsiderScraper/0.1 (Python Requests; compatible; respectful)"

# HTTP Headers
COMMON_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate", # requests handles this automatically
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

# OpenInsider specific table ID
INSIDER_TABLE_ID = "insidertrades"

# Expected columns (order might vary, used for mapping)
# These are based on typical OpenInsider table structure.
# The parser will try to dynamically map headers.
EXPECTED_COLUMN_HEADERS = {
    "X": "delete_marker", # Checkbox column, usually ignored
    "Filing Date": "filing_date",
    "Trade Date": "trade_date",
    "Ticker": "ticker",
    "Company Name": "company_name", # Often part of Ticker cell, needs splitting
    "Insider Name": "insider_name",
    "Title": "title",
    "Trade Type": "trade_type_raw", # e.g., "P - Purchase"
    "Price": "price",
    "Qty": "quantity",
    "Owned": "owned",
    "ΔOwn": "delta_own",
    "Value": "value",
    # Columns for specific views like "Cluster Buys"
    "Insider Cnt": "insider_count",
    "Trade Cnt": "trade_count",
    "View": "view_link", # Link to detailed trades
}


class TradeType(Enum):
    PURCHASE = "P"
    SALE = "S"
    SALE_EXERCISE = "S+" # Sale following option exercise
    GIFT = "G"
    AWARD = "A"
    OTHER = "O"
    UNKNOWN = "UNK"

    @staticmethod
    def from_string(s: str) -> 'TradeType':
        s_upper = s.upper()
        if "P - PURCHASE" in s_upper or s_upper == "P":
            return TradeType.PURCHASE
        if "S - SALE" in s_upper and "+" not in s_upper or s_upper == "S":
            return TradeType.SALE
        if ("S - SALE (SEC RULE 10B5-1)" in s_upper and "+" not in s_upper): # Example of specific sale type
            return TradeType.SALE
        if "S + " in s_upper or "SALE + OPTION EXERCISE" in s_upper or s_upper == "S+": # May need refinement based on actual site data
            return TradeType.SALE_EXERCISE
        if "G - GIFT" in s_upper or s_upper == "G":
            return TradeType.GIFT
        if "A - AWARD" in s_upper or s_upper == "A":
            return TradeType.AWARD
        # Add more mappings as discovered
        return TradeType.UNKNOWN