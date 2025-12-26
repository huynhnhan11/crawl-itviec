from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import logging
import json
import requests
from pathlib import Path
from itviec_jd.http_client import HttpClient
from itviec_jd.parse_listing import extract_job_urls_from_listing_html
from itviec_jd.parse_detail import parse_job_detail
from itviec_jd.normalize import normalize_url, extract_job_key
from tqdm import tqdm

# Cấu hình DAG
default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 12, 26),  # Ngày bắt đầu
}

dag = DAG(
    'itviec_crawl',
    default_args=default_args,
    description='A simple DAG to crawl jobs from itviec.com',
    schedule_interval=timedelta(days=1),  # Chạy mỗi ngày
)

def extract_urls_online(settings, http: HttpClient, max_urls: int = 20) -> list[str]:
    """Crawl các URL công việc trực tiếp từ itviec.com thay vì từ các tệp HTML cũ"""
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

def crawl_jobs():
    """Hàm chính để crawl công việc từ itviec.com"""
    logging.info("Crawling job URLs from itviec.com...")
    
    settings = {
        "base_url": "https://itviec.com",
        "raw_jobs_dir": Path("data/raw/jobs").resolve(),
        "seeds_job_urls": Path("data/seeds/job_urls.txt").resolve(),
        "out_job_details": Path("data/out/job_details.json").resolve(),
        "min_job_id_digits": 4,
        "save_raw_job_html": False,
    }
    
    http = HttpClient(settings["base_url"], "uni-jobchatbot/0.1 (academic project)", min_delay_s=1.0, max_delay_s=2.0, timeout_s=25, max_retries=6)
    
    urls = extract_urls_online(settings, http, max_urls=20)
    
    if not urls:
        logging.warning("Không có URL công việc nào được crawl!")
        return

    logging.info(f"Đang crawl {len(urls)} công việc...")

    crawled = []
    settings["raw_jobs_dir"].mkdir(parents=True, exist_ok=True)

    for url in tqdm(urls, desc="Crawling job details", unit="job"):
        job_key = extract_job_key(url, settings["min_job_id_digits"])

        if job_key:
            try:
                html = http.get_text(url)
                if not html:
                    raise ValueError(f"Empty response for URL: {url}")

                raw_path = None
                if settings["save_raw_job_html"]:
                    raw_path = str((settings["raw_jobs_dir"] / f"job_{job_key}.html").resolve())
                    Path(raw_path).write_text(html, encoding="utf-8")

                job = parse_job_detail(html, settings["base_url"])
                job["raw_html_path"] = raw_path
                crawled.append(job)
            except Exception as e:
                logging.warning(f"Error crawling job {job_key}: {e}")
                continue

    # Lưu kết quả vào tệp JSON
    settings["out_job_details"].parent.mkdir(parents=True, exist_ok=True)
    settings["out_job_details"].write_text(json.dumps({"job_details": crawled}, ensure_ascii=False, indent=2), encoding="utf-8")

    logging.info("Crawl completed successfully!")

crawl_task = PythonOperator(
    task_id='crawl_jobs',
    python_callable=crawl_jobs,
    dag=dag,
)

