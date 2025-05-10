# src/scraper/utils/concurrency.py
import concurrent.futures
import logging
import threading
from typing import List, Any, Callable

logger = logging.getLogger(__name__)

class StoppableThread(threading.Thread):
    """Thread class with a stop() event."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._stop_event = threading.Event()

    def stop(self):
        self._stop_event.set()

    def stopped(self):
        return self._stop_event.is_set()

def shutdown_executor(executor: concurrent.futures.Executor, wait: bool = True):
    """Gracefully shuts down a concurrent.futures.Executor."""
    if isinstance(executor, concurrent.futures.ThreadPoolExecutor):
        logger.info(f"Shutting down ThreadPoolExecutor (PID: {threading.get_ident()})...")
        # Standard shutdown. For ThreadPoolExecutor, cancel_futures=True is Python 3.9+
        # Python 3.8 and below: executor.shutdown(wait=wait)
        # Python 3.9+:
        try:
            # Attempt to cancel futures if supported (Python 3.9+)
            # This is not available on all executor types or Python versions for ThreadPoolExecutor
            if hasattr(executor, '_threads'): # Check if it's a ThreadPoolExecutor with internal _threads
                 # This is a bit of a hack to try and cancel pending tasks.
                 # Proper cancellation requires tasks to check for a cancellation flag.
                 pass # Actual cancellation of running threads is complex.
        except Exception as e:
            logger.warning(f"Could not actively cancel futures during shutdown: {e}")
        
        executor.shutdown(wait=wait)
        logger.info("ThreadPoolExecutor shut down.")
    elif isinstance(executor, concurrent.futures.ProcessPoolExecutor):
        logger.info("Shutting down ProcessPoolExecutor...")
        executor.shutdown(wait=wait) # cancel_futures=True available in 3.9+
        logger.info("ProcessPoolExecutor shut down.")
    else:
        logger.warning(f"Unsupported executor type for shutdown: {type(executor)}")

# Example usage (not directly used by core scraper but could be for other tasks)
def run_parallel_tasks(tasks_with_args: List[tuple[Callable, tuple]], max_workers: int) -> List[Any]:
    """
    Runs a list of tasks in parallel using ThreadPoolExecutor.
    Each item in tasks_with_args should be a tuple: (function, (arg1, arg2, ...)).
    """
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_task = {executor.submit(func, *args): (func, args) for func, args in tasks_with_args}
        for future in concurrent.futures.as_completed(future_to_task):
            task_func, task_args = future_to_task[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as exc:
                logger.error(f"Task {task_func.__name__} with args {task_args} generated an exception: {exc}")
                results.append(None) # Or re-raise, or append an error object
    return results