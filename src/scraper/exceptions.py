# src/scraper/exceptions.py

class ScraperException(Exception):
    """Base exception for the scraper application."""
    pass

class ConfigurationError(ScraperException):
    """Error related to configuration loading or validation."""
    pass

class NetworkError(ScraperException):
    """Error related to network operations (e.g., connection, timeout)."""
    pass

class HTTPError(NetworkError):
    """Error specific to HTTP responses (e.g., 4xx, 5xx status codes)."""
    def __init__(self, message, status_code=None, url=None):
        super().__init__(message)
        self.status_code = status_code
        self.url = url

    def __str__(self):
        return f"{super().__str__()} (Status: {self.status_code}, URL: {self.url})"


class ParsingError(ScraperException):
    """Error encountered during HTML parsing or data extraction."""
    pass

class DataValidationError(ScraperException):
    """Error related to data validation after parsing."""
    pass

class DatabaseError(ScraperException):
    """Error related to database operations."""
    pass

class RateLimitError(NetworkError):
    """Error indicating that a rate limit has been hit."""
    pass

class SeleniumError(ScraperException):
    """Error related to Selenium operations."""
    pass