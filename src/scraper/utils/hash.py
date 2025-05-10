# src/scraper/utils/hash.py
import hashlib
from typing import Dict, Any, List, Union

def generate_trade_hash(trade_data: Dict[str, Any], fields_to_hash: List[str]) -> str:
    """
    Generates a unique SHA256 hash for a trade record based on specified fields.
    """
    # Normalize and concatenate field values
    # Ensure consistent order by sorting field names if not using a predefined list
    # or by using the predefined list's order
    hash_input_parts = []
    for field in sorted(fields_to_hash): # Sort to ensure consistent order if list changes
        value = trade_data.get(field)
        if value is None:
            part = "none"
        elif isinstance(value, (int, float)):
            part = str(value)
        else: # strings, dates converted to strings
            part = str(value).strip().lower()
        hash_input_parts.append(part)
    
    hash_input_string = "|".join(hash_input_parts)
    
    return hashlib.sha256(hash_input_string.encode('utf-8')).hexdigest()

def generate_content_hash(content: Union[str, bytes]) -> str:
    """
    Generates a SHA256 hash for a block of content (e.g., HTML).
    """
    if isinstance(content, str):
        content_bytes = content.encode('utf-8')
    else:
        content_bytes = content
    return hashlib.sha256(content_bytes).hexdigest()

# Fields to use for generating a unique hash for each trade.
# These should uniquely identify a trade and be relatively immutable.
# `company_name` is often part of `ticker` cell on OpenInsider, so it might not be a stable separate field.
# `form_url` can also be very unique.
# Consider the actual data stability.
TRADE_HASH_FIELDS = [
    "filing_date",
    "trade_date",
    "ticker",
    "insider_name",
    "title", # Title can change, but for a specific filing it should be fixed
    "trade_type", # Normalized trade type code
    "price",
    "quantity",
    "value", # Value can be None if price or quantity is None
    # "form_url" # SEC Form URL can be a very good unique identifier too
]