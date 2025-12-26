import random
import time
import logging
import requests
from urllib.parse import urljoin, urlparse

class HttpClient:
    def __init__(self, base_url: str, user_agent: str, min_delay_s: float, max_delay_s: float,
                 timeout_s: float, max_retries: int, logger=None):
        self.base_url = base_url.rstrip("/")
        self.min_delay_s = min_delay_s
        self.max_delay_s = max_delay_s
        self.timeout_s = timeout_s
        self.max_retries = max_retries
        self.log = logger or logging.getLogger("itviec_jd")

        self.sess = requests.Session()
        self.sess.headers.update({"User-Agent": user_agent})
        self._last = 0.0

    def _sleep(self):
        target = random.uniform(self.min_delay_s, self.max_delay_s)
        elapsed = time.time() - self._last
        if elapsed < target:
            time.sleep(target - elapsed)
        self._last = time.time()

    def get_text(self, url_or_path: str) -> str:
        url = url_or_path
        if not urlparse(url_or_path).scheme:
            url = urljoin(self.base_url + "/", url_or_path.lstrip("/"))

        last_err = None
        for attempt in range(self.max_retries + 1):
            try:
                self._sleep()
                r = self.sess.get(url, timeout=self.timeout_s)
                if r.status_code in (429, 500, 502, 503, 504):
                    self.log.warning("Transient %s: %s", r.status_code, url)
                    time.sleep(min(2 ** attempt, 30))
                    continue
                r.raise_for_status()
                return r.text
            except requests.exceptions.HTTPError as e:
                if r.status_code == 410:
                    self.log.warning("HTTP 410: Resource gone for URL: %s", url)
                else:
                    self.log.warning("HTTP error (%s/%s): %s", attempt + 1, self.max_retries + 1, url)
                break
            except Exception as e:
                last_err = e
                self.log.warning("GET fail (%s/%s): %s", attempt + 1, self.max_retries + 1, url)
                time.sleep(min(2 ** attempt, 30))

        if last_err:
            raise RuntimeError(f"GET failed: {url}") from last_err
