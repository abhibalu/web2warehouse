import os
from dagster import Definitions, load_assets_from_modules
from dagster_dbt import DbtCliResource
from .assets import scraping, dbt
from .resources import MinioResource
from dotenv import load_dotenv

load_dotenv()

dbt_project_dir = "/Users/abhijithm/Documents/Code/Scrapping/real_estate_pipeline/dbt/real_estate"

defs = Definitions(
    assets=load_assets_from_modules([scraping, dbt]),
    resources={
        "dbt": DbtCliResource(project_dir=os.path.abspath(dbt_project_dir)),
        "minio": MinioResource(
            endpoint_url=os.getenv("MINIO_ENDPOINT_URL"),
            access_key_id=os.getenv("MINIO_ACCESS_KEY_ID"),
            secret_access_key=os.getenv("MINIO_SECRET_ACCESS_KEY"),
        ),
    },
)
