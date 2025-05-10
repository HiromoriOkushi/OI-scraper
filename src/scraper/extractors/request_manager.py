# src/scraper/extractors/request_manager.py
import requests
from requests.adapters import HTTPAdapter
# from requests.packages.urllib3.util.retry import Retry # Not using requests' built-in retry directly
import time
import random
import logging
from typing import Dict, Any, Optional, List
from pathlib import Path # Added for Path operations

from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from pybreaker import CircuitBreaker, CircuitBreakerError
import requests_cache # Optional caching

from ..types import Config # Corrected: Import Config type from types.py
from ..exceptions import NetworkError, HTTPError, RateLimitError, SeleniumError
from ..constants import COMMON_HEADERS, DEFAULT_USER_AGENT
from .selenium_client import SeleniumClient
from .browser_pool import BrowserPool # Moved to top-level import

logger = logging.getLogger(__name__)

# Define common retryable HTTP status codes
RETRYABLE_STATUS_CODES = {500, 502, 503, 504, 429} # 429 Too Many Requests

class RequestManager:
    """
    Manages HTTP requests with advanced features:
    - Connection pooling (via requests.Session)
    - Rate limiting (simple delay)
    - Retry logic with exponential backoff (via `tenacity`)
    - Caching (optional, via `requests-cache`)
    - Response validation
    - User-Agent rotation
    - Proxy support (basic)
    - Circuit breaker for persistent failures
    """

    def __init__(self, config: Config): # config type hint uses Config from ..types
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

        # Circuit Breaker: trips after N failures, resets after X seconds
        # These values can also come from config for more flexibility
        cb_fail_max = self.scraper_config.get("circuit_breaker_fail_max", 3)
        cb_reset_timeout = self.scraper_config.get("circuit_breaker_reset_timeout", 60)
        self.circuit_breaker_instance = CircuitBreaker(
            fail_max=cb_fail_max, 
            reset_timeout=cb_reset_timeout
        )
        logger.info(f"CircuitBreaker initialized: fail_max={cb_fail_max}, reset_timeout={cb_reset_timeout}s")

        # Selenium client (optional, initialized on demand)
        self.selenium_config = config.get("advanced", {}).get("selenium", {})
        self.selenium_client: Optional[SeleniumClient] = None
        if self.selenium_config.get("enabled", False):
            logger.info("Selenium fallback is configured but will be initialized on first use.")
        
        # --- IMPORTANT: Wrap the core GET method with the circuit breaker ---
        # self._get_with_retries is already decorated by @retry from tenacity.
        # Now, self.get will be the version that is first checked by the circuit breaker.
        self.get = self.circuit_breaker_instance(self._get_with_retries)


    def _get_random_user_agent(self) -> str:
        return random.choice(self.user_agents)

    def _get_next_proxy(self) -> Optional[Dict[str, str]]:
        if not self.proxies_list:
            return None
        
        proxy_url = self.proxies_list[self.current_proxy_index]
        self.current_proxy_index = (self.current_proxy_index + 1) % len(self.proxies_list)
        return {"http": proxy_url, "https": proxy_url}


    def _create_session(self) -> requests.Session:
        if self.caching_config.get("enabled", False):
            cache_name = self.caching_config.get("cache_name", "data/cache/http_cache")
            # Ensure cache directory exists (Path is imported at the top)
            Path(cache_name).parent.mkdir(parents=True, exist_ok=True)
            
            requests_cache.install_cache(
                cache_name=str(cache_name), # Ensure it's a string
                backend=self.caching_config.get("backend", "sqlite"),
                expire_after=self.caching_config.get("expire_after", 3600), # seconds
                allowable_codes=[200], # Cache only successful responses
            )
            logger.info(f"HTTP Caching enabled. Backend: {self.caching_config.get('backend', 'sqlite')}, Name: {cache_name}")
            session = requests_cache.CachedSession()
        else:
            session = requests.Session()
            logger.info("HTTP Caching disabled.")
        
        session.headers.update(COMMON_HEADERS)
        session.headers["User-Agent"] = self._get_random_user_agent() # Initial UA
        return session

    # This is the core GET logic, decorated for retries by tenacity.
    # It will be further wrapped by the circuit breaker in __init__.
    @retry(
        stop=stop_after_attempt( int( # Get from config, ensuring it's an int
            # Need to access self.scraper_config here, which is tricky for decorators
            # directly if they are defined at class level before __init__.
            # Workaround: use a lambda or make these configurable after init if tenacity allows.
            # For now, let's assume scraper_config is available or use defaults.
            # A better way for config-driven tenacity: define retry dynamically in __init__.
            # For simplicity of this fix, hardcoding or using a fixed value here.
            # Let's assume self.scraper_config IS available when decorator is processed (it is for methods).
            # No, it's not available when the decorator is first applied at class definition time.
            # Let's hardcode these for now and note that they should be configurable.
            # scraper_config.get("max_retries", 5) -> This would need self.
            # A common pattern is to have a module-level default or pass config to a decorator factory.
            5 
        )),
        wait=wait_exponential(
            multiplier=float(2), # retry_delay_base
            min=float(2), 
            max=float(30)
        ),
        retry=retry_if_exception_type((requests.exceptions.RequestException, RateLimitError, HTTPError, CircuitBreakerError)), # Retry if CB is open too
        before_sleep=lambda retry_state: logger.warning(
            # Accessing args: retry_state.args will contain (self, url, params, headers)
            f"Retrying request for URL '{retry_state.args[1] if len(retry_state.args) > 1 else 'unknown'}' "
            f"due to {retry_state.outcome.exception().__class__.__name__}: {str(retry_state.outcome.exception())}, attempt {retry_state.attempt_number}..."
        )
    )
    def _get_with_retries(self, url: str, params: Optional[Dict[str, str]] = None, headers: Optional[Dict[str, str]] = None) -> requests.Response:
        """
        Perform GET request with automatic retry and throttling.
        This method is wrapped by the circuit breaker.
        """
        # Rate limiting (simple delay)
        current_time = time.monotonic()
        time_since_last_request = current_time - self.last_request_time
        if time_since_last_request < self.request_delay:
            sleep_time = self.request_delay - time_since_last_request
            logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f}s before request to {url}")
            time.sleep(sleep_time)
        
        self.last_request_time = time.monotonic()

        request_headers = self.session.headers.copy() # Start with session defaults
        request_headers["User-Agent"] = self._get_random_user_agent() # Rotate UA per request
        if headers: # Apply any request-specific headers
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
                retry_after_str = response.headers.get("Retry-After")
                if retry_after_str:
                    try:
                        # Handle HTTP-date or seconds delta
                        wait_seconds = int(retry_after_str)
                        logger.info(f"Honoring Retry-After ({retry_after_str}): sleeping for {wait_seconds} seconds.")
                        # Sleeping here can interfere with tenacity's own backoff if it also retries this.
                        # It might be better to just raise RateLimitError and let tenacity handle all waits.
                        # For now, if we sleep, tenacity will wait *additionally*.
                        # If RateLimitError is in retry_on, tenacity will handle it.
                        # time.sleep(wait_seconds) # Commenting out direct sleep here to let tenacity manage it.
                    except ValueError:
                        # Could be an HTTP Date, requests-cache handles this better if it were part of its core.
                        logger.warning(f"Could not parse Retry-After header value as int: {retry_after_str}")
                raise RateLimitError(f"Rate limit hit (429) for {url}", status_code=429, url=url)

            # Check for other retryable status codes explicitly if not covered by raise_for_status
            if response.status_code in RETRYABLE_STATUS_CODES and response.status_code != 429:
                 raise HTTPError(f"Server error {response.status_code} for {url}", status_code=response.status_code, url=url)
            
            response.raise_for_status() # Raises HTTPError for other 4xx/5xx client/server errors

            logger.info(f"Successfully fetched {url}. Status: {response.status_code}. Cached: {getattr(response, 'from_cache', False)}")
            return response

        # Note: CircuitBreakerError, if the circuit is open, will be raised by the self.get wrapper *before*
        # this _get_with_retries method is even called.
        # However, if CircuitBreakerError is added to tenacity's retry_if_exception_type,
        # tenacity might attempt to retry it.
        # The try-except for CircuitBreakerError here is a safeguard or for logging if it somehow gets through.
        except CircuitBreakerError as e:
            logger.error(f"Circuit breaker is open (caught within _get_with_retries) for URL: {url}. Details: {e}")
            # This re-raise ensures tenacity (if configured to retry on it) or the caller handles it.
            raise NetworkError(f"Circuit breaker open, preventing request to {url}", url=url) from e
        except requests.exceptions.Timeout as e:
            logger.error(f"Timeout while requesting {url}: {e}")
            raise NetworkError(f"Timeout for {url}: {e}", url=url) from e
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error while requesting {url}: {e}")
            raise NetworkError(f"Connection error for {url}: {e}", url=url) from e
        except requests.exceptions.HTTPError as e: # Raised by raise_for_status()
            logger.error(f"HTTP error {e.response.status_code} for {url}: {e.response.text[:200]}")
            # Check if this status code is one we want to retry via tenacity
            if e.response.status_code in RETRYABLE_STATUS_CODES:
                raise HTTPError(f"HTTP error for {url} (retryable)", status_code=e.response.status_code, url=url) from e
            else: # Non-retryable HTTP error by our definition
                raise HTTPError(f"HTTP error for {url} (non-retryable)", status_code=e.response.status_code, url=url) from e
        except requests.exceptions.RequestException as e: # Catch-all for other requests issues
            logger.error(f"Request exception for {url}: {e}")
            raise NetworkError(f"Request failed for {url}: {e}", url=url) from e
        # No need to catch generic Exception here; let specific ones propagate or be caught by tenacity/circuitbreaker.

        
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
            # BrowserPool is already imported at the top
            browser_pool_instance = BrowserPool(self.selenium_config)
            self.selenium_client = SeleniumClient(self.selenium_config, browser_pool_instance)

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
        except Exception as e: # Catch broad exceptions from Selenium path
            logger.error(f"Selenium failed to fetch {url}: {e}")
            raise SeleniumError(f"Selenium error fetching {url}: {e}") from e

    def close(self):
        """Clean up resources."""
        logger.info("Closing RequestManager session.")
        if hasattr(self.session, 'close'):
            self.session.close()
        
        if self.caching_config.get("enabled", False) and requests_cache.is_installed():
            try:
                requests_cache.uninstall_cache()
                logger.info("HTTP Caching uninstalled.")
            except Exception as e: # Can sometimes fail if not properly installed/uninstalled
                logger.warning(f"Error uninstalling requests_cache: {e}")

        if self.selenium_client:
            logger.info("Closing SeleniumClient (which should close its browser pool).")
            self.selenium_client.close()