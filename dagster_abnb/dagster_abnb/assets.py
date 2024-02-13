import os
import pandas as pd
import plotly.express as px
from dagster import AssetExecutionContext, asset, MetadataValue
from dagster_dbt import dbt_assets, DbtCliResource, get_asset_key_for_model


from .resources import dbt_manifest_path, CustomDagsterDbtTranslator, snowflake_resource
from .constants import dbt_project_dir


@asset(
    compute_kind="snowflake",
    group_name="raw_data",
    description="raw listings from s3"
)
def raw_listings() -> pd.DataFrame:
    return pd.read_csv(
        "https://dbtlearn.s3.amazonaws.com/listings.csv",
        parse_dates=['created_at','updated_at'],
    )

@asset(
    compute_kind="snowflake",
    group_name="raw_data",
    description="raw hosts from s3"
)
def raw_hosts() -> pd.DataFrame:
    return pd.read_csv(
        "https://dbtlearn.s3.amazonaws.com/hosts.csv",
        parse_dates=['created_at', 'updated_at'],
    )

@asset(
    compute_kind="snowflake",
    group_name="raw_data",
    description="raw reviews from s3"
)
def raw_reviews() -> pd.DataFrame:
    return pd.read_csv(
        "https://dbtlearn.s3.amazonaws.com/reviews.csv",
        parse_dates=['date'],
    )

@asset(
    compute_kind="python",
    group_name="reports",
    deps=["fct_reviews"],
    required_resource_keys={'snowflake'}
)
def review_negative_count_chart(context: AssetExecutionContext) -> None:
    # with context.resources.snowflake.get_connection() as conn:
    result_df = context.resources.snowflake.execute_query(
        "select count(review_id) reviews, review_sentiment sentiment "
        " from AIRBNB.DEV.FCT_REVIEWS group by review_sentiment",
        fetch_results=True,
        use_pandas_result=True
    )
    fig = px.histogram(result_df, x="SENTIMENT", y="REVIEWS")
    fig.update_layout(bargap=0.2)
    save_chart_path = dbt_project_dir.joinpath("review_sentiment_count_chart.html")
    fig.write_html(save_chart_path, auto_open=True)

    # tell Dagster about the location of the HTML file,
    # so it's easy to access from the Dagster UI
    context.add_output_metadata(
        {"plot_url": MetadataValue.url("file://" + os.fspath(save_chart_path))}
    )

    # with snowflake_resource.
    #     result_df = (
    #         conn
    #         .execute_query(
    #             "select count(review_id) reviews, review_sentiment sentiment from AIRBNB.DEV.FCT_REVIEWS "
    #             "group by review_sentiment",
    #             fetch_results=True,
    #             use_pandas_result=True
    #         )
    #         .fetch_pandas_all()
    #     )


@dbt_assets(
    manifest=dbt_manifest_path, dagster_dbt_translator=CustomDagsterDbtTranslator()
)
def dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
