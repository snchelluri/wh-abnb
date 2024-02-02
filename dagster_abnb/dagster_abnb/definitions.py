import os

from dagster import Definitions

from .resources import dbt_resource
from .assets import dbt_assets

defs = Definitions(
    assets=[dbt_assets],
    resources={
        "dbt": dbt_resource,
    },
)