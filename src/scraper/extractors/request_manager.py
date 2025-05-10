# src/scraper/extractors/request_manager.py
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry # Corrected import path
import time
import random
import logging
from typing import Dict, Any, Optional, List

from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from pybreaker import CircuitBreaker, CircuitBreakerError
import requests_cache # Optional caching

from ..config import Config
from ..exceptions import NetworkError, HTTPError, RateLimitError, SeleniumError
from ..constants import COMMON_HEADERS, DEFAULT_USER_AGENT
from .selenium_client import SeleniumClient # Assuming selenium_client.py defines this

logger = logging.getLogger(__name__)

# Define common retryable HTTP status codes
RETRYABLE_STATUS_CODES = {500, 502, 503, 504, 429} # 429 Too Many Requests

class RequestManager:
    """
    Manages HTTP requests with advanced features:
    - Connection pooling (via requests.Session)
    - Rate limiting (simple delay, can be enhanced with `ratelimit` library)
    - Retry logic with exponential backoff (via `tenacity`)
    - Caching (optional, via `requests-cache`)
    - Response validation
    - User-Agent rotation
    - Proxy support (basic)
    - Circuit breaker for persistent failures
    """

    def __init__(self, config: Config):
        self.config = config
        self.scraper_config = config.get("scraper", {})
        self.http_config = config.get("advanced", {}).get("http_client", {})
        self.caching_config = config.get("advanced", {}).get("caching", {})

        self.user_agents: List[str] = self.http_config.get("user_agents", [DEFAULT_USER_AGENT])
        if not self.user_agents: # Ensure there's at least one UA
            self.user_agents = [DEFAULT_USER_AGENT]

        self.proxies_list: List[str] = self.http_config.get("proxies", [])
        self.current_proxy_index = 0

        self.session = self._create_session()
        
        self.request_delay = self.scraper_config.get("request_delay", 1.0)
        self.last_request_time = 0

        # Circuit Breaker: trips after 3 failures, resets after 60 seconds
        self.circuit_breaker = CircuitBreaker(fail_max=3, reset_timeout=60)

        # Selenium client (optional, initialized on demand)
        self.selenium_config = config.get("advanced", {}).get("selenium", {})
        self.selenium_client: Optional[SeleniumClient] = None
        if self.selenium_config.get("enabled", False):
            # Defer initialization of SeleniumClient until first use or make it lazy
            # self.selenium_client = SeleniumClient(self.selenium_config)
            logger.info("Selenium fallback is configured but will be initialized on first use.")


    def _get_random_user_agent(self) -> str:
        return random.choice(self.user_agents)

    def _get_next_proxy(self) -> Optional[Dict[str, str]]:
        if not self.proxies_list:
            return None
        
        proxy_url = self.proxies_list[self.current_proxy_index]
        self.current_proxy_index = (self.current_proxy_index + 1) % len(self.proxies_list)
        
        # Assuming proxies are in format "http://user:pass@host:port" or "http://host:port"
        return {"http": proxy_url, "https": proxy_url}


    def _create_session(self) -> requests.Session:
        if self.caching_config.get("enabled", False):
            cache_name = self.caching_config.get("cache_name", "data/cache/http_cache")
            Path(cache_name).parent.mkdir(parents=True, exist_ok=True) # Ensure dir for sqlite cache
            
            requests_cache.install_cache(
                cache_name=cache_name,
                backend=self.caching_config.get("backend", "sqlite"),
                expire_after=self.caching_config.get("expire_after", 3600), # seconds
                allowable_codes=[200], # Cache only successful responses
            )
            logger.info(f"HTTP Caching enabled. Backend: {self.caching_config.get('backend', 'sqlite')}, Name: {cache_name}")
            session = requests_cache.CachedSession()
        else:
            session = requests.Session()
            logger.info("HTTP Caching disabled.")

        # Standard retry logic using requests' built-in mechanism (can be less flexible than tenacity)
        # max_retries = self.scraper_config.get("max_retries", 3)
        # retry_strategy = Retry(
        #     total=max_retries,
        #     status_forcelist=RETRYABLE_STATUS_CODES,
        #     backoff_factor=self.scraper_config.get("retry_delay_base", 1) # backoff_factor * (2 ** ({number of total retries} - 1))
        # )
        # adapter = HTTPAdapter(max_retries=retry_strategy)
        # session.mount("http://", adapter)
        # session.mount("https://", adapter)
        
        session.headers.update(COMMON_HEADERS)
        # Initial User-Agent, will be overridden per request if rotation is active
        session.headers["User-Agent"] = self._get_random_user_agent()
        
        return session

    @retry(
        stop=stop_after_attempt(5), # Default max attempts from config
        wait=wait_exponential(multiplier=1, min=2, max=30), # Exponential backoff: 1s, 2s, 4s, 8s, etc.
        retry=retry_if_exception_type((requests.exceptions.RequestException, RateLimitError, HTTPError)),
        before_sleep=lambda retry_state: logger.warning(
            f"Retrying request {retry_state.args[1] if len(retry_state.args) > 1 else ''} "
            f"due to {retry_state.outcome.exception()}, attempt {retry_state.attempt_number}..."
        )
    )
    @circuit_breaker # Apply circuit breaker pattern
    def get(self, url: str, params: Optional[Dict[str, str]] = None, headers: Optional[Dict[str, str]] = None) -> requests.Response:
        """
        Perform GET request with automatic retry, throttling, circuit breaking, and UA rotation.
        """
        # Rate limiting (simple delay)
        current_time = time.monotonic()
        time_since_last_request = current_time - self.last_request_time
        if time_since_last_request < self.request_delay:
            sleep_time = self.request_delay - time_since_last_request
            logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f}s before request to {url}")
            time.sleep(sleep_time)
        
        self.last_request_time = time.monotonic()

        request_headers = self.session.headers.copy()
        request_headers["User-Agent"] = self._get_random_user_agent()
        if headers:
            request_headers.update(headers)
        
        current_proxies = self._get_next_proxy()

        logger.debug(f"Making GET request to {url} with params {params}, UA: {request_headers['User-Agent']}, Proxies: {current_proxies is not None}")
        
        try:
            response = self.session.get(
                url,
                params=params,
                headers=request_headers,
                timeout=self.scraper_config.get("request_timeout", 30),
                proxies=current_proxies,
                verify=True # Standard SSL verification
            )
            
            # Response validation
            if response.status_code == 429: # Too Many Requests
                logger.warning(f"Rate limit hit (429) for {url}. Consider increasing request_delay.")
                # Extract Retry-After header if present and wait
                retry_after = response.headers.get("Retry-After")
                if retry_after:
                    try:
                        wait_seconds = int(retry_after)
                        logger.info(f"Honoring Retry-After: sleeping for {wait_seconds} seconds.")
                        time.sleep(wait_seconds)
                    except ValueError:
                        logger.warning(f"Invalid Retry-After header value: {retry_after}")
                raise RateLimitError(f"Rate limit hit (429) for {url}", status_code=429, url=url)

            if response.status_code in RETRYABLE_STATUS_CODES:
                 raise HTTPError(f"Server error {response.status_code} for {url}", status_code=response.status_code, url=url)
            
            response.raise_for_status() # Raises HTTPError for 4xx/5xx client/server errors not in RETRYABLE_STATUS_CODES

            logger.info(f"Successfully fetched {url}. Status: {response.status_code}. Cached: {getattr(response, 'from_cache', False)}")
            return response

        except requests.exceptions.Timeout as e:
            logger.error(f"Timeout while requesting {url}: {e}")
            raise NetworkError(f"Timeout for {url}: {e}", url=url) from e
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error while requesting {url}: {e}")
            raise NetworkError(f"Connection error for {url}: {e}", url=url) from e
        except requests.exceptions.HTTPError as e: # Raised by raise_for_status()
            logger.error(f"HTTP error {e.response.status_code} for {url}: {e.response.text[:200]}")
            raise HTTPError(f"HTTP error for {url}: {e}", status_code=e.response.status_code, url=url) from e
        except requests.exceptions.RequestException as e: # Catch-all for other requests issues
            logger.error(f"Request exception for {url}: {e}")
            raise NetworkError(f"Request failed for {url}: {e}", url=url) from e
        except CircuitBreakerError as e:
            logger.error(f"Circuit breaker open for {url}. Request not attempted. {e}")
            # Re-raise as NetworkError or a specific CircuitBreakerTrippedError
            raise NetworkError(f"Circuit breaker open, preventing request to {url}", url=url) from e


    def get_with_selenium(self, url: str) -> str:
        """
        Fallback method using Selenium for JavaScript-heavy pages.
        Uses a BrowserPool for managing WebDriver instances.
        """
        if not self.selenium_config.get("enabled", False):
            logger.warning("Selenium is not enabled in config. Cannot fetch with Selenium.")
            raise SeleniumError("Selenium is not enabled.")

        if self.selenium_client is None:
            logger.info("Initializing SeleniumClient...")
            from .browser_pool import BrowserPool # Lazy import
            browser_pool = BrowserPool(self.selenium_config)
            self.selenium_client = SeleniumClient(self.selenium_config, browser_pool)

        try:
            logger.info(f"Fetching {url} using Selenium...")
            # Basic rate limiting for Selenium as well
            current_time = time.monotonic()
            time_since_last_request = current_time - self.last_request_time
            if time_since_last_request < self.request_delay:
                sleep_time = self.request_delay - time_since_last_request
                logger.debug(f"Rate limiting (Selenium): sleeping for {sleep_time:.2f}s before request to {url}")
                time.sleep(sleep_time)
            self.last_request_time = time.monotonic()

            page_source = self.selenium_client.get_page_source(url)
            logger.info(f"Successfully fetched {url} with Selenium.")
            return page_source
        except Exception as e:
            logger.error(f"Selenium failed to fetch {url}: {e}")
            raise SeleniumError(f"Selenium error fetching {url}: {e}") from e

    def close(self):
        """Clean up resources."""
        logger.info("Closing RequestManager session.")
        if hasattr(self.session, 'close'):
            self.session.close()
        
        if self.caching_config.get("enabled", False) and requests_cache.is_installed():
            requests_cache.uninstall_cache()
            logger.info("HTTP Caching uninstalled.")
        
        if self.selenium_client:
            logger.info("Closing SeleniumClient (which should close its browser pool).")
            self.selenium_client.close()