from dagster import get_dagster_logger, file_relative_path, EnvVar, AssetKey
from dagster_dbt import DbtCliResource, DagsterDbtTranslator
from dagster_snowflake_pandas import snowflake_pandas_io_manager
from dagster_snowflake import snowflake_resource

logger = get_dagster_logger()

dbt_resource = DbtCliResource(
    project_dir=file_relative_path(__file__, "../../dbt_project")
)

dbt_parse_invocation = dbt_resource.cli(["parse"], manifest={}).wait()
dbt_manifest_path = dbt_parse_invocation.target_path.joinpath("manifest.json")

# snowflake: snowflake_resource.configured({
#     "account": "bfjtkyd-mt92587",
#     "database": "AIRBNB",
#     "user": {"env": "SNOWFLAKE_USER"},
#     "password": {"env": "SNOWFLAKE_PASSWORD"},
#     "role": "transform",
#     "schema": "RAW",
#     "warehouse": "COMPUTE_WH"})


# snowflake_resource = snowflake_pandas_io_manager(
#     account="bfjtkyd-mt92587",
#     database="AIRBNB",
#     user = EnvVar("SNOWFLAKE_USER"),
#     password = EnvVar("SNOWFLAKE_PASSWORD"),
#     role = "transform",
#     schema= "RAW",
#     warehouse= "COMPUTE_WH"
# )

class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    @classmethod
    def get_asset_key(cls, dbt_resource_props) -> AssetKey:
        return AssetKey(dbt_resource_props["name"])

    def get_group_name(cls, dbt_resource_props) -> str:
        return "prepared"