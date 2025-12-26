import re
from urllib.parse import urlparse

JOB_ID_RE = re.compile(r"/it-jobs/.+-(\d+)$", re.I)

def normalize_url(url: str) -> str:
    p = urlparse(url.strip())
    return p._replace(query="", fragment="").geturl()

def extract_job_key(url: str, min_digits: int = 4) -> str | None:
    p = urlparse(url)
    m = JOB_ID_RE.search(p.path)
    if not m:
        return None
    jid = m.group(1)
    if len(jid) < min_digits:
        return None
    return jid

def is_listing_like_title(title: str) -> bool:
    t = (title or "").lower()
    return (" jobs in vietnam" in t) or (" jobs in viet nam" in t)
