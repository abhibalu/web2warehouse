
from dagster import asset
from pipelines.scrape_props import scrape_props_links_across_pages, scrape_and_upload_ndjson
from ..resources import MinioResource

@asset
def raw_properties(minio: MinioResource):
    """Scrapes property data and uploads it to MinIO."""
    start_page = 24
    end_page = 24
    links = scrape_props_links_across_pages(start_page=start_page, end_page=end_page)
    minio_client = minio.get_client()
    scrape_and_upload_ndjson(links, minio_client)

