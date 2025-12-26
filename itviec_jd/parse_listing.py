import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

JOB_URL_ATTR = "data-search--job-selection-job-url-value"
BASE = "https://itviec.com"

def _normalize_job_url(raw: str, base_url: str = BASE) -> str | None:
    if not raw:
        return None
    raw = raw.strip()

    if not urlparse(raw).scheme:
        raw = urljoin(base_url.rstrip("/") + "/", raw.lstrip("/"))

    p = urlparse(raw)
    path = p.path

    if path.endswith("/content"):
        path = path[:-len("/content")]

    if not re.search(r"/it-jobs/.+-\d{4,}$", path):
        return None

    return p._replace(path=path, query="", fragment="").geturl()

def extract_job_urls_from_listing_html(html: str, base_url: str = BASE) -> list[str]:
    soup = BeautifulSoup(html, "lxml")
    urls = []
    for el in soup.select(f"[{JOB_URL_ATTR}]"):
        u = _normalize_job_url(el.get(JOB_URL_ATTR), base_url=base_url)
        if u:
            urls.append(u)

    if not urls:
        for a in soup.select('a[href^="/it-jobs/"], a[href^="https://itviec.com/it-jobs/"]'):
            u = _normalize_job_url(a.get("href"), base_url=base_url)
            if u:
                urls.append(u)

    return list(dict.fromkeys(urls))
