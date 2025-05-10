# src/scraper/parsers/data_cleaner.py
import re
from datetime import datetime, date
from typing import Optional, Any, Union, Tuple
from dateutil import parser as date_parser
import logging

logger = logging.getLogger(__name__)

def clean_text(text: Optional[str]) -> Optional[str]:
    """Removes leading/trailing whitespace and multiple spaces. Returns None if input is None or empty after strip."""
    if text is None:
        return None
    text = str(text).strip() # Ensure it's a string before stripping
    if not text: # If stripping results in an empty string
        return None
    text = re.sub(r'\s+', ' ', text) # Replace multiple spaces/newlines with a single space
    return text if text else None


def parse_int(value: Optional[Any]) -> Optional[int]:
    """Converts a value to an integer, removing commas and handling None."""
    if value is None:
        return None
    if isinstance(value, (int, float)): # If already a number, convert to int
        return int(value)
    
    text_value = clean_text(str(value))
    if text_value is None:
        return None
    
    text_value = text_value.replace(',', '').split('.')[0] # Remove commas and decimal part
    if not text_value: # if empty after cleaning
        return None
    try:
        return int(text_value)
    except ValueError:
        logger.debug(f"Could not parse '{value}' as int.")
        return None

def parse_float(value: Optional[Any]) -> Optional[float]:
    """Converts a value to a float, removing commas and currency symbols, handling None."""
    if value is None:
        return None
    if isinstance(value, (float, int)): # If already a number, convert to float
        return float(value)

    text_value = clean_text(str(value))
    if text_value is None:
        return None
    
    # Remove common currency symbols and commas
    text_value = text_value.replace(',', '').replace('$', '').replace('£', '').replace('€', '')
    text_value = text_value.strip()

    if not text_value or text_value.lower() == 'nan' or text_value == '-': # Handle empty or 'nan'
        return None
    
    # Handle percentage values like "10.5%"
    if text_value.endswith('%'):
        try:
            return float(text_value[:-1]) / 100.0
        except ValueError:
            logger.debug(f"Could not parse percentage '{value}' as float.")
            return None
    
    try:
        return float(text_value)
    except ValueError:
        logger.debug(f"Could not parse '{value}' as float.")
        return None

def parse_date_flexible(date_string: Optional[str], default_to_none: bool = True) -> Optional[date]:
    """
    Parses a date string using dateutil.parser for flexibility.
    Returns a datetime.date object or None.
    """
    if date_string is None:
        return None
    
    cleaned_date_string = clean_text(date_string)
    if not cleaned_date_string:
        return None
        
    try:
        # Examples: "2023-12-25", "12/25/2023", "Dec 25, 2023", "2023-12-25 10:00 AM"
        # dateutil.parser is quite powerful
        dt_obj = date_parser.parse(cleaned_date_string)
        return dt_obj.date()
    except (ValueError, TypeError, OverflowError) as e:
        logger.debug(f"Could not parse date string '{date_string}': {e}")
        if default_to_none:
            return None
        raise # Re-raise if strict parsing is needed

def normalize_trade_type(trade_type_str: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Normalizes a trade type string like "P - Purchase" or "S + Sale following option exercise"
    into a code (e.g., "P", "S+") and a description.
    Uses the TradeType enum from constants for mapping codes.
    """
    from ..constants import TradeType # Local import to avoid circular dependency if constants imports this

    cleaned_str = clean_text(trade_type_str)
    if not cleaned_str:
        return None, None

    # Try to extract code and description
    # Common patterns: "CODE - Description", "CODE"
    match = re.match(r"([A-Z][\+\-]?)\s*-\s*(.*)", cleaned_str, re.IGNORECASE)
    if match:
        code = match.group(1).upper()
        description = clean_text(match.group(2))
        
        # Validate against TradeType enum, if code is found directly use it
        try:
            enum_member = TradeType(code) # Check if code itself is a valid enum value
            return enum_member.value, description
        except ValueError:
            # If code is not directly an enum value, try to find matching enum from description
            pass # Fall through to parsing the full string with TradeType.from_string

    # If no clear split or direct code match, try to parse using the enum's logic
    parsed_enum = TradeType.from_string(cleaned_str)
    if parsed_enum != TradeType.UNKNOWN:
        return parsed_enum.value, cleaned_str # Return the original string as description if parsed from full
    
    # Fallback for simple codes if no description found
    if len(cleaned_str) <= 3 and cleaned_str.isalnum() and cleaned_str.isalpha(): # e.g. "P", "S", "S+"
        try:
            enum_member = TradeType(cleaned_str.upper())
            return enum_member.value, None
        except ValueError:
            pass

    logger.warning(f"Could not normalize trade type: '{trade_type_str}'. Returning UNKNOWN.")
    return TradeType.UNKNOWN.value, cleaned_str # return original string as desc for unknown

def split_ticker_company(ticker_company_str: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Splits a string like "AAPL Apple Inc" into ticker ("AAPL") and company name ("Apple Inc").
    Handles cases where only ticker might be present.
    """
    cleaned_str = clean_text(ticker_company_str)
    if not cleaned_str:
        return None, None

    parts = cleaned_str.split(maxsplit=1)
    ticker = None
    company_name = None

    if parts:
        # Assume the first part is the ticker if it's all uppercase and relatively short
        potential_ticker = parts[0]
        if potential_ticker.isupper() and len(potential_ticker) <= 6 and re.match(r"^[A-Z\.]+$", potential_ticker):
            ticker = potential_ticker
            if len(parts) > 1:
                company_name = clean_text(parts[1])
            else: # Only ticker was found
                company_name = None # Or potentially use an external lookup for company name based on ticker
        else:
            # Could not identify a clear ticker, assume the whole string is company name if long enough,
            # or it's a malformed entry.
            # This logic might need refinement based on observed data patterns.
            company_name = cleaned_str 
            logger.debug(f"Could not reliably split ticker from '{cleaned_str}'. Treating as company name.")
    
    return ticker, company_name


def extract_form_url(cell_html_or_element: Any, base_url: str) -> Optional[str]:
    """
    Extracts the SEC form URL from an HTML cell or BeautifulSoup element.
    Assumes the URL is within an <a> tag.
    """
    from bs4 import BeautifulSoup, Tag

    if cell_html_or_element is None:
        return None

    # If it's already a BeautifulSoup Tag
    if isinstance(cell_html_or_element, Tag):
        a_tag = cell_html_or_element.find('a')
    # If it's an HTML string
    elif isinstance(cell_html_or_element, str):
        soup = BeautifulSoup(cell_html_or_element, 'lxml')
        a_tag = soup.find('a')
    else:
        logger.warning(f"Cannot extract form URL from type: {type(cell_html_or_element)}")
        return None

    if a_tag and a_tag.has_attr('href'):
        href = a_tag['href']
        # Make URL absolute if it's relative
        if href.startswith('/'):
            return base_url.strip('/') + href
        elif href.startswith("http"):
            return href
        else:
            logger.debug(f"Found href '{href}' that is not absolute or relative, cannot form full URL.")
            return href # Return as is, might be an issue
    return None