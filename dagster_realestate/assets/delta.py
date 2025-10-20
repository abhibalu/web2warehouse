from datetime import date
from dagster import asset, get_dagster_logger

from pipelines.delta_lake import ingest_raw, build_silver_incremental
from .scraping import raw_properties


@asset(deps=[raw_properties])
def raw_delta_ingest():
    """Load staging NDJSON into the append-only raw Delta table (dedup by property_id)."""
    logger = get_dagster_logger()
    logger.info("Starting raw Delta ingest")
    ingest_raw(date.today())
    logger.info("Finished raw Delta ingest")


@asset(deps=[raw_delta_ingest])
def silver_build_incremental():
    """Incrementally process raw → silver Delta tables using watermark checkpointing."""
    logger = get_dagster_logger()
    logger.info("Starting silver incremental build")
    build_silver_incremental()
    logger.info("Finished silver incremental build")
