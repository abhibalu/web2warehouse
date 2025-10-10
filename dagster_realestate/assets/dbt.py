
from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

import os

dbt_project_dir = "/Users/abhijithm/Documents/Code/Scrapping/real_estate_pipeline/dbt/real_estate"

@dbt_assets(
    manifest=os.path.join(dbt_project_dir, "target", "manifest.json"),
)
def real_estate_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

