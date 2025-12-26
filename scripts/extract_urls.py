import logging
from pathlib import Path
import json

from itviec_jd.pipeline import extract_urls_offline

def main():
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")
    
    cfg = json.loads(Path("config.json").read_text())
    settings = {
        "raw_listings_dir": Path(cfg["paths"]["raw_listings_dir"]),
        "seeds_job_urls": Path(cfg["paths"]["seeds_job_urls"]),
    }
    extract_urls_offline(settings)

if __name__ == "__main__":
    main()
