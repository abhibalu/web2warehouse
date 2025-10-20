import os
from dagster import Definitions, load_assets_from_modules, AssetSelection
from dagster_dbt import DbtCliResource
from .assets import scraping, dbt
from .resources import MinioResource
from dotenv import load_dotenv

load_dotenv()

# Define the DBT project directory
dbt_project_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "dbt", "real_estate")

defs = Definitions(
    assets=load_assets_from_modules([scraping, dbt]),
    resources={
        "dbt": DbtCliResource(project_dir=os.path.abspath(dbt_project_dir)),
        "minio": MinioResource(
            endpoint_url=os.getenv("MINIO_ENDPOINT_URL", "http://localhost:9000"),
            access_key_id=os.getenv("MINIO_ACCESS_KEY_ID", "minioadmin"),
            secret_access_key=os.getenv("MINIO_SECRET_ACCESS_KEY", "minioadmin"),
        ),
    },
)
