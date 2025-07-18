import os
import time
import json
import requests
import io
import datetime

from typing import List, Dict
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from dotenv import load_dotenv

from pipelines.minio_upload import create_minio_client
from pipelines.helper_functions import transform_to_json_url, flatten_json

load_dotenv()


BASE_URL = os.getenv("BASE_URL")
BUCKET_NAME = "staging"
OBJECT_NAME_TEMPLATE = "raw/scraped_data_{date}/scraped_data_{date}.ndjson"


endpoint_url = os.getenv("MINIO_ENDPOINT_URL")
access_key_id = os.getenv("MINIO_ACCESS_KEY_ID")
secret_access_key = os.getenv("MINIO_SECRET_ACCESS_KEY")

minio_client = create_minio_client(endpoint_url, access_key_id, secret_access_key)

# Ensure output directory exists
# os.makedirs(SCRAPED_DATA_DIR, exist_ok=True)


date = datetime.date.today()
# date = datetime.date.today()+datetime.timedelta(days=1)


def scrape_props_links_across_pages(start_page=1, end_page=3) -> List[str]:
    options = Options()
    # options.add_argument("--headless=new")  # Uncomment to run headless
    driver = webdriver.Chrome(options=options)
    all_links = []

    try:
        for page in range(start_page, end_page + 1):
            print(f"🔎 Scraping page {page}...")
            driver.get(BASE_URL.format(page=page))

            WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".property-card"))
            )

            time.sleep(2)  # Allow lazy-loaded content

            cards = driver.find_elements(
                By.CSS_SELECTOR, ".property-card .property-name a"
            )
            page_links = [
                card.get_attribute("href")
                for card in cards
                if card.get_attribute("href")
            ]
            all_links.extend(page_links)

            if page != end_page:
                print("waiting 10 seconds before the next page...")
                time.sleep(10)

    except Exception as e:
        print(f"❌ Error scraping pages: {e}")
    finally:
        driver.quit()

    return all_links


def scrape_and_upload_ndjson(links: List[str]):
    # , start_page: int, end_page: int
    buffer = io.StringIO()

    for i, link in enumerate(links):
        try:
            json_url = transform_to_json_url(link)
            response = requests.get(json_url, timeout=10)

            if response.status_code == 200:
                raw_data = response.json()
                cleaned_data = flatten_json(raw_data)

                if cleaned_data:
                    buffer.write(json.dumps(cleaned_data) + "\n")
                    print(f"✅ {i+1}/{len(links)}: Written to buffer")

            else:
                print(
                    f"⚠️ Failed to fetch {json_url} – Status Code: {response.status_code}"
                )

        except Exception as e:
            print(f"❌ Error processing link {link}: {e}")

    # Convert buffer to BytesIO for MinIO upload
    byte_data = io.BytesIO(buffer.getvalue().encode("utf-8"))
    object_name = OBJECT_NAME_TEMPLATE.format(date=date)

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
    start_page = 24
    end_page = 24
    links = scrape_props_links_across_pages(start_page=start_page, end_page=end_page)
    print(f"🔗 Total property links found: {len(links)}")
    scrape_and_upload_ndjson(links)
    # , start_page=start_page, end_page=end_page
