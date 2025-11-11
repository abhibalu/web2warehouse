import os
from dagster import (
    Definitions,
    load_assets_from_modules,
    AssetSelection,
    define_asset_job,
    ScheduleDefinition,
)
from .assets import scraping, delta
from .resources import MinioResource
from dotenv import load_dotenv

load_dotenv()

# Build a single job that can materialize the full graph
etl_all = define_asset_job("etl_all", selection=AssetSelection.all())

defs = Definitions(
    assets=load_assets_from_modules([scraping, delta]),
    resources={
        "minio": MinioResource(
            endpoint_url=os.getenv("MINIO_ENDPOINT_URL", "http://localhost:9000"),
            access_key_id=os.getenv("MINIO_ACCESS_KEY_ID", "minioadmin"),
            secret_access_key=os.getenv("MINIO_SECRET_ACCESS_KEY", "minioadmin"),
        ),
    },
    jobs=[etl_all],
    schedules=[ScheduleDefinition(job=etl_all, cron_schedule="0 6 * * *")],
)
