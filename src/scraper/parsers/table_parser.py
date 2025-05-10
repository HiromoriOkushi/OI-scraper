# src/scraper/parsers/table_parser.py
import logging
from typing import List, Dict, Any, Optional, Callable
from bs4 import BeautifulSoup, Tag

from .base_parser import BaseParser
from ..exceptions import ParsingError
from ..constants import INSIDER_TABLE_ID, EXPECTED_COLUMN_HEADERS
from . import data_cleaner # Import the module

logger = logging.getLogger(__name__)

class GenericTableParser(BaseParser):
    """
    A generic parser for HTML tables.
    It can be configured with specific table identifiers and column processing logic.
    """

    def __init__(self,
                 table_identifier: Dict[str, str] = {"id": INSIDER_TABLE_ID},
                 row_selector: str = "tr",
                 header_selector: str = "th",
                 cell_selector: str = "td",
                 column_map: Optional[Dict[str, str]] = None,
                 row_processor: Optional[Callable[[Tag, List[str], Dict[str, str]], Optional[Dict[str, Any]]]] = None):
        """
        Initialize the generic table parser.

        :param table_identifier: A dictionary specifying how to find the table (e.g., {'id': 'myTableId'}).
        :param row_selector: CSS selector for table rows.
        :param header_selector: CSS selector for header cells within a row.
        :param cell_selector: CSS selector for data cells within a row.
        :param column_map: Optional map of header text to standardized field names.
                           If None, uses header text as keys (cleaned).
        :param row_processor: Optional custom function to process a single row.
                              Takes (bs4_row_element, header_keys, column_map_used) and returns a dict or None.
        """
        self.table_identifier = table_identifier
        self.row_selector = row_selector
        self.header_selector = header_selector
        self.cell_selector = cell_selector
        self.column_map = column_map if column_map else EXPECTED_COLUMN_HEADERS
        self.row_processor = row_processor if row_processor else self._default_row_processor

    def _find_table(selfself, soup: BeautifulSoup) -> Optional[Tag]:
        """Finds the table element in the parsed HTML."""
        table = soup.find("table", self.table_identifier)
        if not table:
            logger.warning(f"Table with identifier {self.table_identifier} not found.")
            # Fallback: try to find the first large table if specific ID fails
            all_tables = soup.find_all("table")
            if not all_tables:
                logger.error("No tables found in HTML content.")
                return None
            
            # Heuristic: pick the table with the most rows or a significant number of rows
            # This is a guess and might not always be correct.
            # For OpenInsider, the main table usually has id="insidertrades"
            logger.info("Attempting to find table by common ID 'insidertrades' as a fallback.")
            table = soup.find("table", id="insidertrades")
            if table:
                logger.info("Found table by fallback ID 'insidertrades'.")
                return table

            logger.warning("Fallback table selection: Picking the first table found. This may be incorrect.")
            return all_tables[0] # Risky fallback
        return table

    def _extract_headers(self, table_element: Tag) -> List[str]:
        """Extracts header texts from the table."""
        headers = []
        # Look for headers in <thead>, or first row if no <thead>
        thead = table_element.find("thead")
        header_row = None
        if thead:
            header_row = thead.find(self.row_selector) # Usually 'tr'
        
        if not header_row: # If no <thead> or no <tr> in <thead>, try the first <tr> of <tbody> or table
            first_body_row = table_element.find("tbody").find(self.row_selector) if table_element.find("tbody") else None
            if first_body_row and first_body_row.find_all(self.header_selector): # Check if first body row has <th>
                header_row = first_body_row
            elif table_element.find(self.row_selector): # Fallback to first row of the table itself
                 header_row = table_element.find(self.row_selector)


        if header_row:
            header_cells = header_row.find_all([self.header_selector, self.cell_selector]) # Some tables use <td> in headers
            for cell in header_cells:
                # Try to get text, clean it, and handle complex headers (e.g. with <br>)
                text_parts = [str(s).strip() for s in cell.stripped_strings]
                header_text = " ".join(text_parts) if text_parts else data_cleaner.clean_text(cell.get_text())
                headers.append(header_text if header_text else f"unknown_header_{len(headers)}")
        
        if not headers:
            logger.warning("Could not extract headers from table. Parsing might be unreliable.")
            # As a last resort, if we have rows, we can infer header count from first data row
            first_data_row_cells = table_element.find("tbody").find(self.row_selector).find_all(self.cell_selector) if table_element.find("tbody") and table_element.find("tbody").find(self.row_selector) else []
            if first_data_row_cells:
                headers = [f"column_{i}" for i in range(len(first_data_row_cells))]
                logger.info(f"Generated generic headers based on first data row: {headers}")


        logger.debug(f"Extracted headers: {headers}")
        return headers
    
    def _map_headers(self, extracted_headers: List[str]) -> Dict[str,str]:
        """Maps extracted headers to standardized field names using self.column_map."""
        mapped_headers = {}
        normalized_column_map = {k.lower().strip(): v for k, v in self.column_map.items()}

        for i, header_text in enumerate(extracted_headers):
            cleaned_header = header_text.lower().strip()
            # Try exact match first (case-insensitive)
            if cleaned_header in normalized_column_map:
                mapped_headers[header_text] = normalized_column_map[cleaned_header]
            else:
                # Try partial match (e.g., "Filing Date Time" should map to "filing_date")
                best_match = None
                for map_key, field_name in normalized_column_map.items():
                    if map_key in cleaned_header: # "filing date" in "filing date time"
                        best_match = field_name
                        break # Take first partial match
                if best_match:
                    mapped_headers[header_text] = best_match
                    logger.debug(f"Partially mapped header '{header_text}' to '{best_match}'")
                else:
                    # If no map, use cleaned header or a generic name
                    generic_name = f"column_{i}_{data_cleaner.clean_text(header_text).replace(' ', '_').lower()}" if data_cleaner.clean_text(header_text) else f"column_{i}"
                    mapped_headers[header_text] = generic_name
                    logger.warning(f"Unmapped header: '{header_text}'. Using generic name: '{generic_name}'")
        
        logger.debug(f"Final header mapping: {mapped_headers}")
        return mapped_headers


    def _default_row_processor(self, row_element: Tag, header_keys: List[str], mapped_fields: Dict[str, str]) -> Optional[Dict[str, Any]]:
        """
        Default row processor. Extracts text from cells.
        :param row_element: bs4 Tag for the row.
        :param header_keys: List of original header texts (keys for mapped_fields).
        :param mapped_fields: Dictionary mapping original header text to standardized field names.
        :return: A dictionary representing the parsed row.
        """
        cells = row_element.find_all(self.cell_selector, recursive=False) # recursive=False for direct children
        if not cells:
            # This might be a header row if not properly skipped, or an empty/malformed row
            if not row_element.find_all(self.header_selector): # If it's not a header row
                 logger.debug(f"Skipping row with no cells: {row_element.prettify()[:100]}")
            return None

        if len(cells) != len(header_keys):
            logger.warning(
                f"Row has {len(cells)} cells, but {len(header_keys)} headers were expected. "
                f"Row content (partial): {row_element.get_text(strip=True, separator='|')[:100]}. Skipping."
            )
            return None # Skip rows that don't match header count

        row_data = {}
        for i, cell in enumerate(cells):
            original_header = header_keys[i]
            field_name = mapped_fields.get(original_header)
            
            if not field_name: # Should not happen if _map_headers worked correctly
                logger.error(f"Logic error: No mapped field name for original header '{original_header}'")
                field_name = f"unknown_field_{i}"

            # Basic text extraction, can be overridden by a custom row_processor
            cell_text = data_cleaner.clean_text(cell.get_text(separator=" ")) # Use space as separator for multi-line cells
            row_data[field_name] = cell_text
            
            # Store raw HTML of cell if needed for complex parsing (e.g., links)
            # row_data[f"{field_name}_html"] = str(cell)
        
        return row_data


    def parse(self, html_content: str, source_url: str) -> List[Dict[str, Any]]:
        """
        Parses the HTML content to extract table data.
        """
        if not html_content:
            logger.warning(f"Empty HTML content received for parsing from {source_url}.")
            return []

        try:
            soup = BeautifulSoup(html_content, "lxml")
        except Exception as e:
            logger.error(f"Failed to parse HTML with lxml from {source_url}: {e}")
            raise ParsingError(f"BeautifulSoup parsing failed for {source_url}") from e

        table_element = self._find_table(soup)
        if not table_element:
            logger.error(f"No suitable table found in HTML from {source_url}.")
            return [] # Or raise ParsingError if a table is strictly expected

        original_headers = self._extract_headers(table_element)
        if not original_headers:
            logger.error(f"Could not extract headers from table at {source_url}.")
            return [] # Or raise ParsingError

        mapped_header_fields = self._map_headers(original_headers)
        
        parsed_items = []
        # Find all data rows (usually in <tbody>, or directly in <table>)
        tbody = table_element.find("tbody")
        rows_container = tbody if tbody else table_element
        
        data_rows = rows_container.find_all(self.row_selector, recursive=False) # Get direct children <tr>
        
        # Skip header row(s) if they are part of the main rows list
        # This can be tricky. A common pattern is first row is headers.
        # _extract_headers tries to find headers in thead first.
        # If headers were found in first row of rows_container, we need to skip it.
        # A simple check: if the first row contains <th> elements.
        
        start_row_index = 0
        if data_rows:
            # If the first row found by row_selector contains header_selector elements, it's likely a header.
            # This check helps if headers are not in a distinct <thead> and were picked up by self.row_selector
            first_row_is_header_like = False
            first_row_header_cells = data_rows[0].find_all(self.header_selector)
            if first_row_header_cells and len(first_row_header_cells) > 0:
                 # Compare with extracted headers to be more certain
                 first_row_texts = [data_cleaner.clean_text(cell.get_text()) for cell in first_row_header_cells]
                 if all(text in original_headers for text in first_row_texts if text):
                     first_row_is_header_like = True
            
            # If headers were extracted from a <thead>, and the first row in `data_rows` is identical to that header row text-wise
            # it's a duplicate and should be skipped.
            # This is more complex to check reliably without comparing actual elements.

            if first_row_is_header_like and not table_element.find("thead"):
                # This logic assumes if no <thead>, the first <tr> is the header.
                # If _extract_headers already processed this row, we must skip it here.
                # This might need adjustment based on how _extract_headers handles tables without <thead>
                logger.debug("Skipping first row in rows_container as it appears to be a header row already processed.")
                start_row_index = 1


        for i, row_element in enumerate(data_rows[start_row_index:]):
            # Skip rows that are clearly headers if missed (e.g., multiple header rows)
            if row_element.find(self.header_selector): # If a row still contains <th>, it's likely a header
                logger.debug(f"Skipping row {i+start_row_index} as it appears to be a header: {row_element.get_text(strip=True, separator='|')[:100]}")
                continue
            
            # Skip empty rows or rows with only   or similar
            if not row_element.get_text(strip=True):
                logger.debug(f"Skipping empty row {i+start_row_index}")
                continue

            try:
                processed_row_data = self.row_processor(row_element, original_headers, mapped_header_fields)
                if processed_row_data:
                    parsed_items.append(processed_row_data)
            except Exception as e:
                logger.error(f"Error processing row {i+start_row_index} from {source_url}: {e}. Row: {row_element.prettify()[:200]}")
                # Optionally, re-raise or collect errors
        
        logger.info(f"Successfully parsed {len(parsed_items)} items from table at {source_url}.")
        return parsed_items