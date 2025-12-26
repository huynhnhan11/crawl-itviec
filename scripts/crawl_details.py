import logging
import json
from pathlib import Path
import yaml
from itviec_jd.http_client import HttpClient
from itviec_jd.parse_listing import extract_job_urls_from_listing_html
from itviec_jd.parse_detail import parse_job_detail
from itviec_jd.normalize import normalize_url, extract_job_key
from tqdm import tqdm

def extract_urls_online(settings, http: HttpClient, max_urls: int = 20) -> list[str]:
    all_urls = []
    seen = set()

    search_url = f"{settings['base_url']}/it-jobs"
    
    page = 1
    while True:
        url_with_page = f"{search_url}?page={page}"
        html = http.get_text(url_with_page)
        
        if not html:
            break

        urls = extract_job_urls_from_listing_html(html)
        for u in urls:
            u = normalize_url(u)
            if u not in seen:
                seen.add(u)
                all_urls.append(u)
                if len(all_urls) >= max_urls:  
                    return all_urls

        page += 1  

    settings["seeds_job_urls"].write_text("\n".join(all_urls) + ("\n" if all_urls else ""), encoding="utf-8")
    return all_urls

def load_crawled_jobs(state_file: Path) -> set[str]:
    """Load các job đã crawl từ file (tránh crawl lại các job đã xử lý)"""
    if state_file.exists():
        return set(json.loads(state_file.read_text(encoding="utf-8")))
    return set()

def save_crawled_jobs(state_file: Path, crawled_jobs: set[str]):
    """Lưu lại danh sách job đã crawl"""
    state_file.parent.mkdir(parents=True, exist_ok=True)
    state_file.write_text(json.dumps(list(crawled_jobs), ensure_ascii=False, indent=2), encoding="utf-8")

def crawl_jobs_from_web(settings, http: HttpClient, crawled_jobs: set[str]) -> list[dict]:
    """Crawl job details từ web"""
    logging.info("Crawling job URLs from itviec.com...")
    
    # Lấy các URL từ itviec.com (tối đa 20 URL)
    urls = extract_urls_online(settings, http, max_urls=20)
    
    if not urls:
        logging.warning("Không có URL công việc nào được crawl!")
        return []

    logging.info(f"Đang crawl {len(urls)} công việc...")

    crawled = []
    settings["raw_jobs_dir"].mkdir(parents=True, exist_ok=True)

    # Dùng tqdm để theo dõi tiến độ crawl job details
    for url in tqdm(urls, desc="Crawling job details", unit="job"):
        job_key = extract_job_key(url, settings["min_job_id_digits"])

        if job_key and job_key not in crawled_jobs:
            try:
                html = http.get_text(url)  # Gửi GET request để lấy HTML
                if not html:
                    raise ValueError(f"Empty response for URL: {url}")

                raw_path = None
                # Kiểm tra xem có cần lưu raw HTML không
                if settings["save_raw_job_html"]:
                    raw_path = str((settings["raw_jobs_dir"] / f"job_{job_key}.html").resolve())
                    Path(raw_path).write_text(html, encoding="utf-8")  # Lưu raw HTML vào file

                job = parse_job_detail(html, settings["base_url"])  # Trích xuất chi tiết job
                job["raw_html_path"] = raw_path
                crawled.append(job)
                crawled_jobs.add(job_key)

            except Exception as e:
                logging.warning(f"Error crawling job {job_key}: {e}")
                continue

    # Tạo thư mục 'out' nếu chưa có trước khi lưu dữ liệu
    out_dir = settings["out_job_details"].parent
    out_dir.mkdir(parents=True, exist_ok=True)

    # Lưu lại kết quả crawl vào file JSON
    settings["out_job_details"].write_text(json.dumps({"job_details": crawled}, ensure_ascii=False, indent=2), encoding="utf-8")
    return crawled

def main():
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")

    try:
        with open("config.yaml", "r", encoding="utf-8") as file:
            cfg = yaml.safe_load(file) 
    except Exception as e:
        logging.error(f"Error reading config.yaml: {e}")
        return

    http = HttpClient(cfg["base_url"], cfg["user_agent"], **cfg["rate_limit"])

    settings = {
        "base_url": cfg["base_url"],
        "raw_jobs_dir": Path(cfg["paths"]["raw_jobs_dir"]).resolve(),
        "seeds_job_urls": Path(cfg["paths"]["seeds_job_urls"]).resolve(),
        "out_job_details": Path(cfg["paths"]["out_job_details"]).resolve(),
        "min_job_id_digits": int(cfg["extract"]["min_job_id_digits"]),
        "save_raw_job_html": bool(cfg["crawl"]["save_raw_job_html"]),
    }

    crawled_jobs_file = Path("data/state/crawled_jobs.json").resolve()
    crawled_jobs = load_crawled_jobs(crawled_jobs_file)

    crawl_jobs_from_web(settings, http, crawled_jobs)

    save_crawled_jobs(crawled_jobs_file, crawled_jobs)

if __name__ == "__main__":
    main()
