import logging
import threading
from collections import deque
from time import time, sleep

logger = logging.getLogger(__name__)

MAX_REQUESTS_PER_MINUTE = 400
RATE_LIMIT_WINDOW = 60
RATE_LIMIT_COOLDOWN = 5


class RateLimiter:
    def __init__(self, max_requests: int = MAX_REQUESTS_PER_MINUTE,
                 window: int = RATE_LIMIT_WINDOW,
                 cooldown: int = RATE_LIMIT_COOLDOWN) -> None:
        self._max_requests = max_requests
        self._window = window
        self._cooldown = cooldown
        self._times: deque[float] = deque()
        self._lock = threading.Lock()

    def wait(self) -> None:
        # Sleeping inside the lock is intentional: all threads share the same
        # cooldown window, preventing bursts when the limit is hit.
        with self._lock:
            while True:
                now = time()
                while self._times and self._times[0] < now - self._window:
                    self._times.popleft()
                if len(self._times) < self._max_requests:
                    break
                logger.warning(
                    f"Rate limit reached ({self._max_requests} requests "
                    f"in {self._window}s), sleeping {self._cooldown}s"
                )
                sleep(self._cooldown)
            self._times.append(time())
