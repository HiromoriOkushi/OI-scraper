# src/scraper/parsers/base_parser.py
from abc import ABC, abstractmethod
from typing import List, Dict, Any

class BaseParser(ABC):
    """
    Abstract base class for HTML parsers.
    """
    @abstractmethod
    def parse(self, html_content: str, source_url: str) -> List[Dict[str, Any]]:
        """
        Parses HTML content into a list of structured data items.

        :param html_content: The HTML string to parse.
        :param source_url: The URL from which the HTML was fetched (for context/logging).
        :return: A list of dictionaries, each representing a parsed item.
        """
        pass