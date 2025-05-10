# src/scraper/parsers/trade_parser.py
import logging
from typing import List, Dict, Any, Optional
from bs4 import BeautifulSoup, Tag
import hashlib # For hash_id generation

from .table_parser import GenericTableParser
from . import data_cleaner # Import the module
from ..constants import INSIDER_TABLE_ID, EXPECTED_COLUMN_HEADERS, TradeType
from ..exceptions import ParsingError, DataValidationError
from ..types import RawTradeData, ParsedTrades
from ..utils.hash import generate_trade_hash, TRADE_HASH_FIELDS

logger = logging.getLogger(__name__)

class InsiderTradeParser(GenericTableParser):
    """
    Parses HTML content from OpenInsider.com into structured trade data.
    Inherits from GenericTableParser and customizes row processing.
    """

    def __init__(self, base_url: str):
        # Use the default table identifier for OpenInsider
        super().__init__(
            table_identifier={"id": INSIDER_TABLE_ID},
            column_map=EXPECTED_COLUMN_HEADERS, # Use the predefined column map
            row_processor=self.extract_trade_details # Custom row processor
        )
        self.base_url = base_url.strip('/')

    def parse_trade_table(self, html: str, source_name: str) -> ParsedTrades:
        """
        Parse the main insider trading table from HTML.
        This method now primarily calls the parent's parse method.
        The actual row-level data extraction is handled by `extract_trade_details`.

        :param html: HTML content string.
        :param source_name: Name of the source page (e.g., "latest_filings").
        :return: List of dictionaries, each representing a trade.
        """
        if not html:
            logger.warning(f"Cannot parse empty HTML for source: {source_name}")
            return []
        
        logger.info(f"Starting to parse trade table for source: {source_name}")
        
        # The parent's parse method will use self.extract_trade_details via the row_processor argument
        parsed_rows = super().parse(html_content=html, source_url=f"{self.base_url}/{source_name}")
        
        # Post-process each row: add source and generate hash_id
        final_trades: ParsedTrades = []
        for raw_trade in parsed_rows:
            if not raw_trade: # Skip if row processor returned None
                continue

            # Add source information
            raw_trade['source'] = source_name
            
            # Generate unique hash_id
            try:
                # Ensure all necessary fields for hash are present, even if None
                hash_payload = {field: raw_trade.get(field) for field in TRADE_HASH_FIELDS}
                raw_trade['hash_id'] = generate_trade_hash(hash_payload, TRADE_HASH_FIELDS)
            except Exception as e:
                logger.error(f"Failed to generate hash_id for trade: {raw_trade}. Error: {e}")
                raw_trade['hash_id'] = None # Or skip this trade

            # Basic validation: check if essential fields are present
            # More thorough validation will be done by Pydantic models later
            if not raw_trade.get('ticker') or not raw_trade.get('filing_date') or not raw_trade.get('trade_date'):
                logger.warning(f"Skipping trade due to missing essential fields (ticker/dates): {raw_trade}")
                continue
            
            if raw_trade['hash_id']: # Only add if hash could be generated
                final_trades.append(raw_trade)
            else:
                logger.warning(f"Skipping trade due to missing hash_id: {raw_trade}")

        logger.info(f"Successfully parsed and processed {len(final_trades)} trades from source: {source_name}")
        return final_trades


    def extract_trade_details(self, row_element: Tag, header_keys: List[str], mapped_fields: Dict[str, str]) -> Optional[RawTradeData]:
        """
        Extracts data from a single table row (<tr> element).
        This is the custom row_processor.

        :param row_element: BeautifulSoup Tag object for the <tr>.
        :param header_keys: List of original header texts from the table.
        :param mapped_fields: Dictionary mapping original header text to standardized field names.
        :return: A dictionary containing structured data for the trade, or None if row is invalid.
        """
        cells = row_element.find_all("td", recursive=False)
        if not cells or len(cells) != len(header_keys):
            # This check is also in GenericTableParser, but good to have defensively
            logger.debug(f"Row skipped: cell count mismatch or no cells. Expected {len(header_keys)}, got {len(cells)}")
            return None

        raw_data: RawTradeData = {}
        # Iterate using original header keys to map to cells correctly
        for i, original_header in enumerate(header_keys):
            cell = cells[i]
            field_name = mapped_fields.get(original_header)

            if not field_name:
                logger.warning(f"No mapping found for header '{original_header}'. Skipping cell.")
                continue

            cell_text_content = data_cleaner.clean_text(cell.get_text(separator=" "))
            
            # --- Field-specific parsing and cleaning ---
            if field_name == "delete_marker": # 'X' column
                continue # Usually not needed

            elif field_name in ["filing_date", "trade_date"]:
                raw_data[field_name] = data_cleaner.parse_date_flexible(cell_text_content)
            
            elif field_name == "ticker": # This cell often contains "TICKER Company Name"
                # Form URL is also often in this cell's <a> tag's title attribute or href
                a_tag = cell.find('a')
                if a_tag:
                    ticker_company_str = data_cleaner.clean_text(a_tag.get_text())
                    if a_tag.has_attr('href') and 'secform4url' in a_tag['href']:
                        # This is a common pattern for the SEC form link on OpenInsider
                        # e.g., /screener_secform4?form4url=http://www.sec.gov/...
                        form_url_param = a_tag['href'].split('form4url=')[-1]
                        if form_url_param.startswith("http"):
                             raw_data["form_url"] = form_url_param
                        else:
                            logger.debug(f"Unrecognized form_url structure in ticker cell: {a_tag['href']}")
                    elif a_tag.has_attr('href') and a_tag['href'].startswith('/'): # Relative link to company page
                        # raw_data["company_profile_url"] = self.base_url + a_tag['href']
                        pass # Could store this if needed
                else:
                    ticker_company_str = cell_text_content
                
                ticker, company_name = data_cleaner.split_ticker_company(ticker_company_str)
                raw_data["ticker"] = ticker
                raw_data["company_name"] = company_name
            
            elif field_name == "company_name": # If there's a dedicated company name column
                if not raw_data.get("company_name"): # Only if not already parsed from ticker cell
                    raw_data[field_name] = cell_text_content

            elif field_name == "insider_name":
                raw_data[field_name] = cell_text_content
            
            elif field_name == "title":
                raw_data[field_name] = cell_text_content
            
            elif field_name == "trade_type_raw": # e.g., "P - Purchase"
                trade_code, trade_desc = data_cleaner.normalize_trade_type(cell_text_content)
                raw_data["trade_type"] = trade_code
                # raw_data["trade_description"] = trade_desc # Optionally store full description

            elif field_name == "price":
                raw_data[field_name] = data_cleaner.parse_float(cell_text_content)

            elif field_name in ["quantity", "owned", "insider_count", "trade_count"]:
                raw_data[field_name] = data_cleaner.parse_int(cell_text_content)
            
            elif field_name == "delta_own": # Percentage, e.g., "+10%" or "10.00%"
                # data_cleaner.parse_float handles '%'
                parsed_float = data_cleaner.parse_float(cell_text_content)
                raw_data[field_name] = parsed_float # Will be a decimal, e.g., 0.10 for 10%

            elif field_name == "value": # Transaction value, e.g., "$1,234,567"
                raw_data[field_name] = data_cleaner.parse_float(cell_text_content)

            elif field_name == "view_link": # For "Cluster Buys" etc., links to detail page
                link_tag = cell.find('a')
                if link_tag and link_tag.has_attr('href'):
                    href = link_tag['href']
                    if href.startswith('/'):
                        raw_data['detail_page_url'] = self.base_url + href
                    else:
                        raw_data['detail_page_url'] = href
            
            else: # For any other mapped fields not explicitly handled
                raw_data[field_name] = cell_text_content
        
        # Post-processing and derived fields
        # Ensure essential fields are present, even if None after parsing
        for key in ["price", "quantity", "owned", "delta_own", "value", "title", "form_url"]:
            if key not in raw_data:
                raw_data[key] = None
        
        # Calculate 'value' if not directly available but price and quantity are
        if raw_data.get("value") is None and raw_data.get("price") is not None and raw_data.get("quantity") is not None:
            try:
                raw_data["value"] = float(raw_data["price"]) * int(raw_data["quantity"])
            except (TypeError, ValueError):
                logger.debug(f"Could not calculate 'value' for trade: price={raw_data.get('price')}, qty={raw_data.get('quantity')}")


        # Simple validation: Check for at least a ticker and one date
        if not raw_data.get("ticker") or not (raw_data.get("filing_date") or raw_data.get("trade_date")):
            logger.warning(f"Row missing essential data (ticker/date). Row content: {row_element.get_text(strip=True, separator=' | ')}")
            return None
            
        return raw_data