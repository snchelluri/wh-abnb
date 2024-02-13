from setuptools import find_packages, setup

setup(
    name="dagster_abnb",
    packages=find_packages(exclude=["dagster_abnb_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "snowflake-connector-python[pandas]",
        "dagster-dbt",
        "pandas",
        "pyarrow",
        "dbt-snowflake",
        "dagster-snowflake-pandas",
        "plotly"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
