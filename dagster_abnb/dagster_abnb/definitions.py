import os

from dagster import Definitions, EnvVar, define_asset_job, AssetSelection, AssetKey, ScheduleDefinition
from dagster_snowflake_pandas import snowflake_pandas_io_manager
from dagster_snowflake import snowflake_resource


from .resources import dbt_resource
from .assets import dbt_assets, raw_listings, raw_hosts, raw_reviews, review_negative_count_chart

asset_snapshot_key_list = [AssetKey(["scd_raw_listings"]), AssetKey(["scd_raw_hosts"])]
defs = Definitions(
    assets=[dbt_assets, raw_listings, raw_hosts, raw_reviews, review_negative_count_chart],
    resources={
        "dbt": dbt_resource,
        "io_manager": snowflake_pandas_io_manager.configured({
            "account":"bfjtkyd-mt92587",
            "database":"AIRBNB",
            "user" : {"env": "SNOWFLAKE_USER"},
            "password" : {"env": "SNOWFLAKE_PASSWORD"},
            "role" : "transform",
            "schema": "RAW",
            "warehouse": "COMPUTE_WH"}),
        "snowflake": snowflake_resource.configured({
            "account": "bfjtkyd-mt92587",
            "database": "AIRBNB",
            "user": {"env": "SNOWFLAKE_USER"},
            "password": {"env": "SNOWFLAKE_PASSWORD"},
            "role": "transform",
            "schema": "RAW",
            "warehouse": "COMPUTE_WH"})
    },
    jobs=[
        define_asset_job(
            name="abnb_snapshots",
            selection=AssetSelection.keys(*asset_snapshot_key_list)
        )
    ],
    schedules=[
        ScheduleDefinition(
            name="abnb_snapshots_schedule",
            job_name="abnb_snapshots",
            cron_schedule="* * * * *",
        )
    ],
)