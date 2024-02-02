from dagster import get_dagster_logger, file_relative_path
from dagster_dbt import DbtCliResource

logger = get_dagster_logger()

dbt_resource = DbtCliResource(
    project_dir=file_relative_path(__file__, "../../dbt_project")
)

dbt_parse_invocation = dbt_resource.cli(["parse"], manifest={}).wait()
dbt_manifest_path = dbt_parse_invocation.target_path.joinpath("manifest.json")
