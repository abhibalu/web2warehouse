import os
import time
import json
import requests
import io
import datetime
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode, urljoin
import re
import argparse
import tempfile
from typing import List, Dict
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from dotenv import load_dotenv

from pipelines.minio_upload import create_minio_client, ensure_bucket_exists
from pipelines.helper_functions import transform_to_json_url, flatten_json

load_dotenv()


BASE_URL = os.getenv("BASE_URL", "")
BUCKET_NAME = "staging"
OBJECT_NAME_TEMPLATE = "raw/scraped_data_{date}/scraped_data_{date}.ndjson"


endpoint_url = os.getenv("MINIO_ENDPOINT_URL")
access_key_id = os.getenv("MINIO_ACCESS_KEY_ID")
secret_access_key = os.getenv("MINIO_SECRET_ACCESS_KEY")


# Ensure output directory exists
# os.makedirs(SCRAPED_DATA_DIR, exist_ok=True)


date = datetime.date.today()
# date = datetime.date.today()+datetime.timedelta(days=1)


def _build_page_url(base_url: str, page: int) -> str:
    """Return a page-specific URL.
    Supports either a `{page}` placeholder in BASE_URL or injects/overrides
    a `page` query parameter if no placeholder is present.
    """
    if "{page}" in base_url:
        return base_url.format(page=page)
    # fallback: add/override ?page=
    parsed = urlparse(base_url)
    qs = parse_qs(parsed.query)
    qs["page"] = [str(page)]
    new_query = urlencode(qs, doseq=True)
    return urlunparse(parsed._replace(query=new_query))


def _accept_cookies(driver) -> None:
    """Best-effort click on common cookie banners (e.g. OneTrust)."""
    try:
        # OneTrust default id
        btn = WebDriverWait(driver, 3).until(
            EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
        )
        btn.click()
        time.sleep(0.5)
        return
    except Exception:
        pass
    # Try some common text buttons
    for text in [
        "Accept All",
        "Accept all",
        "Accept All Cookies",
        "I Accept",
        "Accept",
    ]:
        try:
            el = WebDriverWait(driver, 2).until(
                EC.element_to_be_clickable((By.XPATH, f"//button[normalize-space()='{text}']"))
            )
            el.click()
            time.sleep(0.5)
            return
        except Exception:
            continue


def _scroll_to_bottom(driver, max_steps: int = 8, pause: float = 0.6):
    last_height = driver.execute_script("return document.body.scrollHeight")
    for _ in range(max_steps):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(pause)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height


def scrape_props_links_across_pages(start_page: int = 1, end_page: int = 3, debug: bool = False) -> List[str]:
    options = Options()
    options.add_argument("--headless=new")  # Uncomment to run headless
    # More stable scraping in headless mode
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")
    driver = webdriver.Chrome(options=options)

    seen: set[str] = set()
    cookies_handled = False

    try:
        for page in range(start_page, end_page + 1):
            page_url = _build_page_url(BASE_URL, page)
            print(f"Scraping page {page} → {page_url}")
            driver.get(page_url)

            if debug:
                print(f"Loaded URL: {driver.current_url}")

            # Try to accept cookies only once
            if not cookies_handled:
                _accept_cookies(driver)
                cookies_handled = True

            # Ensure we actually landed on the expected page (not redirected)
            try:
                WebDriverWait(driver, 5).until(lambda d: f"page-{page}/" in d.current_url)
            except Exception:
                cur = driver.current_url
                if debug:
                    print(f"WARN: Requested page-{page} but landed on {cur}")

            # Wait for listing anchors to exist
            WebDriverWait(driver, 15).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".property-card .property-name a"))
            )

            # small delay for lazy content
            time.sleep(2)
            _scroll_to_bottom(driver)

            # Dump page HTML for diffing when debugging
            if debug:
                dump_path = os.path.join(tempfile.gettempdir(), f"dng_page_{page}.html")
                with open(dump_path, "w", encoding="utf-8") as f:
                    f.write(driver.page_source)
                print(f"Saved HTML → {dump_path}")

            # Collect candidate links from explicit card title anchors
            anchors = driver.find_elements(By.CSS_SELECTOR, ".property-card .property-name a")
            if debug:
                print(f"Found {len(anchors)} card anchors")
            page_links_raw: list[str] = []
            for a in anchors:
                href = a.get_attribute("href")
                if not href:
                    continue
                # Resolve relative links to absolute
                abs_href = href if href.startswith("http") else urljoin(page_url, href)
                base_href = abs_href.split("?")[0]
                if "/property-for-sale/" in base_href:
                    page_links_raw.append(base_href)

            before = len(seen)
            for l in page_links_raw:
                seen.add(l)
            new_on_page = len(seen) - before
            dup_on_page = len(page_links_raw) - new_on_page

            if debug:
                print(f"Page {page} summary: {len(page_links_raw)} links → {new_on_page} new, {dup_on_page} duplicates")
                for l in sorted(set(page_links_raw)):
                    print(f"  - {l}")

            print(f"Collected {len(seen)} unique links so far")

            if page != end_page:
                time.sleep(2)

    except Exception as e:
        print(f"Error scraping pages: {e}")
    finally:
        driver.quit()

    return sorted(seen)


def scrape_and_upload_ndjson(links: List[str], minio_client):
    # , start_page: int, end_page: int
    buffer = io.StringIO()

    ensure_bucket_exists(minio_client, BUCKET_NAME)

    for i, link in enumerate(links):
        try:
            json_url = transform_to_json_url(link)
            # print(f'json_url - {json_url}')
            response = requests.get(json_url, timeout=10)

            if response.status_code == 200:
                raw_data = response.json()
                cleaned_data = flatten_json(raw_data)

                if cleaned_data:
                    buffer.write(json.dumps(cleaned_data) + "\n")
                    # print(f"{i+1}/{len(links)}: Written to buffer")

            else:
                print(
                    f"Failed to fetch {json_url} – Status Code: {response.status_code}"
                )

        except Exception as e:
            print(f"Error processing link {link}: {e}")

    # Convert buffer to BytesIO for MinIO upload
    byte_data = io.BytesIO(buffer.getvalue().encode("utf-8"))
    object_name = OBJECT_NAME_TEMPLATE.format(date=date)
    # print(f'access_key_id - {access_key_id}')
    
    # Upload to MinIO
    minio_client.put_object(
        Bucket=BUCKET_NAME,
        Key=object_name,
        Body=byte_data,
        ContentLength=byte_data.getbuffer().nbytes,
        ContentType="application/x-ndjson",
    )

    print(f"🚀 NDJSON file uploaded to MinIO bucket '{BUCKET_NAME}' as '{object_name}'")


if __name__ == "__main__":
    start_page = 1
    # int(os.getenv("START_PAGE", 20))
    end_page = 3
    # int(os.getenv("END_PAGE", 20))
    debug_links = os.getenv("SCRAPER_DEBUG", "0").lower() in {"1", "true", "yes"}

    minio_client = create_minio_client(endpoint_url, access_key_id, secret_access_key)
    ensure_bucket_exists(minio_client, BUCKET_NAME)
    links = scrape_props_links_across_pages(start_page=start_page, end_page=end_page, debug=1)
    print(f'links - {links}')
    print(f"Total unique property links found: {len(links)}")
    scrape_and_upload_ndjson(links, minio_client)
