import json, re
from bs4 import BeautifulSoup
from urllib.parse import urljoin

def _t(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def _guess_working_model(text: str):
    t = (text or "").lower()
    if "remote" in t: return "Remote"
    if "hybrid" in t: return "Hybrid"
    if "at office" in t or "onsite" in t: return "At office"
    return None

def _extract_section_by_heading(root: BeautifulSoup, keywords: list[str]) -> str | None:
    ks = [k.lower() for k in keywords]
    for h in root.find_all(["h2", "h3", "h4"]):
        ht = _t(h.get_text()).lower()
        if any(k in ht for k in ks):
            chunks = []
            for sib in h.find_all_next():
                if sib.name in ("h2", "h3", "h4"):
                    break
                txt = _t(sib.get_text(" ", strip=True))
                if txt:
                    chunks.append(txt)
            out = _t(" ".join(chunks))
            return out or None
    return None

def parse_job_detail(html: str, base_url: str) -> dict:
    soup = BeautifulSoup(html, "lxml")
    main = soup.find("main") or soup

    title = None
    h1 = main.find("h1")
    if h1:
        title = _t(h1.get_text()) or None

    company = ""
    company_url = None
    a_company = main.select_one('a[href^="/companies/"]')
    if a_company:
        company = _t(a_company.get_text()) or ""
        company_url = urljoin(base_url.rstrip("/") + "/", (a_company.get("href") or "").lstrip("/"))

    blob = _t(main.get_text(" ", strip=True))
    working_model = _guess_working_model(blob)

    posted_text = None
    m = re.search(r"Posted\s+\d+\s+days?\s+ago|Posted\s+today|Posted\s+yesterday", blob, flags=re.I)
    if m:
        posted_text = _t(m.group(0))

    skills = []
    for a in main.select('a[href^="/it-jobs/"]'):
        txt = _t(a.get_text())
        if txt and len(txt) <= 80:
            skills.append(txt)
    skills = list(dict.fromkeys(skills))
    skills_json = json.dumps(skills, ensure_ascii=False)

    description = _extract_section_by_heading(main, ["Job description", "Mô tả công việc", "Description"])
    requirements = _extract_section_by_heading(main, ["Requirements", "Yêu cầu", "Job requirements", "Your skills and experience"])
    benefits = _extract_section_by_heading(main, ["Benefits", "Phúc lợi", "Why you'll love working here"])

    return {
        "title": title,
        "company": company,
        "company_url": company_url,
        "locations": None,
        "working_model": working_model,
        "level": None,
        "posted_text": posted_text,
        "skills_json": skills_json,
        "description": description,
        "requirements": requirements,
        "benefits": benefits,
    }
