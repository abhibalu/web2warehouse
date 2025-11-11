
from dagster import asset, get_dagster_logger
from pipelines.scrape_props import scrape_props_links_across_pages, scrape_and_upload_ndjson, BUCKET_NAME
from pipelines.minio_upload import ensure_bucket_exists
from ..resources import MinioResource

@asset
def raw_properties(minio: MinioResource):
    """Scrapes property data and uploads it to MinIO."""
    logger = get_dagster_logger()
    logger.info("Starting property scraping asset")
    
    start_page = 1  # Changed from 24 to 1 for a more representative demo
    end_page = 2    # Changed from 24 to 2 for a more representative demo
    try:
        links = scrape_props_links_across_pages(start_page=start_page, end_page=end_page)
        logger.info(f"Found {len(links)} property links to process")
        
        minio_client = minio.get_client()
        ensure_bucket_exists(minio_client, BUCKET_NAME)
        scrape_and_upload_ndjson(links, minio_client)
        logger.info(f"Successfully uploaded data for {len(links)} properties")
        
        return {"properties_scraped": len(links)}
    except Exception as e:
        logger.error(f"Error in raw_properties asset: {str(e)}")
        raise

