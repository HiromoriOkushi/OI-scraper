# src/scraper/extractors/browser_pool.py
import queue
import threading
import logging
from typing import Optional, Dict, Any
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.firefox.service import Service as FirefoxService
# from webdriver_manager.chrome import ChromeDriverManager # Option for auto-driver management
# from webdriver_manager.firefox import GeckoDriverManager

from ..exceptions import SeleniumError

logger = logging.getLogger(__name__)

class BrowserPool:
    """
    Manages a pool of Selenium WebDriver instances for concurrent use.
    """
    def __init__(self, selenium_config: Dict[str, Any]):
        self.config = selenium_config
        self.max_instances = self.config.get("max_instances", 2)
        self.headless = self.config.get("headless", True)
        self.driver_path = self.config.get("driver_path") # e.g., path to chromedriver
        self.browser_type = self.config.get("browser_type", "chrome").lower() # chrome or firefox

        self._pool = queue.Queue(maxsize=self.max_instances)
        self._lock = threading.Lock()
        self._created_instances = 0
        
        # Pre-fill pool or create on demand
        # For simplicity, creating on demand up to max_instances.
        # self._initialize_pool()

    # def _initialize_pool(self):
    #     # Optional: pre-fill the pool with some instances
    #     for _ in range(min(1, self.max_instances)): # Start with 1 or a few
    #         try:
    #             driver = self._create_driver()
    #             self._pool.put(driver)
    #         except Exception as e:
    #             logger.error(f"Failed to pre-initialize a browser instance: {e}")

    def _create_driver(self) -> webdriver.Remote:
        logger.info(f"Creating new Selenium WebDriver instance (Type: {self.browser_type}, Headless: {self.headless})...")
        driver: Optional[webdriver.Remote] = None
        try:
            if self.browser_type == "chrome":
                options = webdriver.ChromeOptions()
                if self.headless:
                    options.add_argument("--headless")
                options.add_argument("--no-sandbox")
                options.add_argument("--disable-dev-shm-usage")
                options.add_argument("--disable-gpu") # Often recommended for headless
                # options.add_argument("user-agent=...") # Can set UA here too
                
                if self.driver_path:
                    service = ChromeService(executable_path=self.driver_path)
                    driver = webdriver.Chrome(service=service, options=options)
                else:
                    # Attempt to use webdriver_manager if installed and configured, or rely on PATH
                    # driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
                    logger.warning("Chrome driver_path not specified in config. Relying on ChromeDriver being in PATH.")
                    driver = webdriver.Chrome(options=options)

            elif self.browser_type == "firefox":
                options = webdriver.FirefoxOptions()
                if self.headless:
                    options.add_argument("--headless")
                
                if self.driver_path: # Path to geckodriver
                    service = FirefoxService(executable_path=self.driver_path)
                    driver = webdriver.Firefox(service=service, options=options)
                else:
                    # driver = webdriver.Firefox(service=FirefoxService(GeckoDriverManager().install()), options=options)
                    logger.warning("Firefox driver_path (geckodriver) not specified. Relying on geckodriver being in PATH.")
                    driver = webdriver.Firefox(options=options)
            else:
                raise SeleniumError(f"Unsupported browser type: {self.browser_type}")

            if driver:
                driver.set_page_load_timeout(self.config.get("page_load_timeout", 60))
                logger.info(f"WebDriver instance created successfully. (PID: {driver.service.process.pid if hasattr(driver.service, 'process') else 'N/A'})")
                return driver
            else:
                raise SeleniumError("Driver could not be initialized.")

        except Exception as e:
            logger.error(f"Failed to create Selenium WebDriver (Type: {self.browser_type}): {e}")
            if driver: # Ensure cleanup if partial creation failed
                try:
                    driver.quit()
                except: pass
            raise SeleniumError(f"Failed to create WebDriver: {e}") from e


    def get_driver(self, timeout: int = 30) -> webdriver.Remote:
        """
        Acquires a WebDriver instance from the pool.
        Blocks if pool is empty and max_instances not reached, or waits for an instance.
        """
        with self._lock:
            if not self._pool.empty():
                driver = self._pool.get_nowait()
                logger.debug("Reusing WebDriver instance from pool.")
                return driver
            
            if self._created_instances < self.max_instances:
                driver = self._create_driver()
                self._created_instances += 1
                logger.debug(f"Created new WebDriver. Total instances: {self._created_instances}/{self.max_instances}")
                return driver
            # If max instances are created and pool is empty, wait for one to be returned
        
        # Wait for an instance to become available if maxed out
        logger.debug(f"Max WebDriver instances ({self.max_instances}) reached. Waiting for available driver...")
        try:
            driver = self._pool.get(block=True, timeout=timeout)
            logger.debug("Acquired WebDriver instance from pool after waiting.")
            return driver
        except queue.Empty:
            logger.error(f"Timeout waiting for WebDriver instance from pool after {timeout}s.")
            raise SeleniumError("Timeout waiting for available WebDriver instance.")


    def release_driver(self, driver: webdriver.Remote):
        """
        Returns a WebDriver instance to the pool.
        Resets driver state if necessary (e.g., clear cookies, navigate to blank page).
        """
        if driver:
            try:
                # Basic reset: navigate to blank page to clear current state
                driver.get("about:blank") 
                # driver.delete_all_cookies() # Optional, depending on use case
                self._pool.put(driver)
                logger.debug("WebDriver instance returned to pool.")
            except Exception as e:
                # If driver is broken (e.g., browser crashed), discard it
                logger.warning(f"Error returning WebDriver to pool, discarding instance: {e}")
                self.close_driver(driver, force_decrement=True)

    def close_driver(self, driver: webdriver.Remote, force_decrement: bool = False):
        """Closes a specific driver instance and decrements count if it was managed by pool."""
        if driver:
            logger.info(f"Closing WebDriver instance (PID: {driver.service.process.pid if hasattr(driver.service, 'process') else 'N/A'})...")
            try:
                driver.quit()
            except Exception as e:
                logger.error(f"Error quitting WebDriver: {e}")
            finally:
                if force_decrement: # Or if this driver was definitely one created by the pool
                    with self._lock:
                        if self._created_instances > 0 : # ensure not to go negative
                             self._created_instances -= 1
                             logger.debug(f"Decremented active WebDriver instance count to {self._created_instances}.")


    def close_all(self):
        """Closes all WebDriver instances in the pool and shuts down."""
        logger.info("Closing all WebDriver instances in the pool...")
        with self._lock: # Ensure no new drivers are created or acquired during shutdown
            while not self._pool.empty():
                try:
                    driver = self._pool.get_nowait()
                    self.close_driver(driver)
                except queue.Empty:
                    break # Pool is empty
                except Exception as e:
                    logger.error(f"Error closing a pooled driver: {e}")
            self._created_instances = 0 # Reset count
        logger.info("Browser pool closed.")