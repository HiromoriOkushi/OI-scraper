# src/scraper/extractors/selenium_client.py
import logging
from typing import Dict, Any, Optional

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, WebDriverException

from .browser_pool import BrowserPool
from ..exceptions import SeleniumError

logger = logging.getLogger(__name__)

class SeleniumClient:
    """
    A client to interact with web pages using Selenium, utilizing a BrowserPool.
    """
    def __init__(self, selenium_config: Dict[str, Any], browser_pool: BrowserPool):
        self.config = selenium_config
        self.pool = browser_pool
        self.page_load_timeout = self.config.get("page_load_timeout", 60)
        self.element_wait_timeout = self.config.get("element_wait_timeout", 10) # Time to wait for elements

    def get_page_source(self, url: str, wait_for_element_xpath: Optional[str] = None) -> str:
        """
        Fetches the page source of a URL using a WebDriver instance from the pool.
        Optionally waits for a specific element to be present before returning source.
        """
        driver = None
        try:
            driver = self.pool.get_driver()
            driver.get(url)

            if wait_for_element_xpath:
                logger.debug(f"Waiting for element {wait_for_element_xpath} on {url}...")
                WebDriverWait(driver, self.element_wait_timeout).until(
                    EC.presence_of_element_located((By.XPATH, wait_for_element_xpath))
                )
                logger.debug(f"Element {wait_for_element_xpath} found.")
            
            # Potentially add more sophisticated wait conditions, e.g., for JS to finish
            # time.sleep(self.config.get("js_render_wait", 2)) # Simple fixed wait if needed

            page_source = driver.page_source
            return page_source

        except TimeoutException as e:
            logger.error(f"Timeout waiting for element or page load for {url}: {e}")
            raise SeleniumError(f"Selenium timeout for {url}: {e}") from e
        except WebDriverException as e: # Catch broader Selenium exceptions
            logger.error(f"WebDriverException for {url}: {e.msg}") # e.msg often has useful info
            # If a WebDriverException occurs, the driver might be corrupted.
            # It's safer to close it rather than return it to the pool.
            if driver:
                self.pool.close_driver(driver, force_decrement=True) 
                driver = None # Ensure it's not released back
            raise SeleniumError(f"Selenium WebDriver error for {url}: {e.msg}") from e
        except Exception as e:
            logger.error(f"Unexpected error during Selenium operation for {url}: {e}")
            if driver: # Also close driver on unknown errors
                self.pool.close_driver(driver, force_decrement=True)
                driver = None
            raise SeleniumError(f"Unexpected Selenium error for {url}: {e}") from e
        finally:
            if driver:
                self.pool.release_driver(driver)
    
    def close(self):
        """Closes the underlying browser pool."""
        logger.info("SeleniumClient closing its browser pool.")
        if self.pool:
            self.pool.close_all()