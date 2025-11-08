import os
from datetime import datetime, timezone
import pandas as pd
from prefect import task, get_run_logger

from data_utils.pythonic_utils import read_file_content

from ludeos_metadata_analyzer.config import (
    SNOWFLAKE_RESULTS_DB, SNOWFLAKE_RESULTS_TABLE, SNOWFLAKE_RESULTS_SCHEMA, SNOWFLAKE_READ_OPERATIONS_DB
)
from ludeos_metadata_analyzer.drivers import snowflake_driver

from config import SNOWFLAKE_READ_DB

ENV = os.getenv('ENV', 'local')
LUDEOS_QUERY = read_file_content('ludeos_metadata_analyzer/queries/top_ludeos_from_L3_ludeos_profile.sql')


@task
def fetch_ludeos_from_snowflake():
    logger = get_run_logger()
    games_for_reports_env_filter = _calculate_games_for_reports_filter()
    query = LUDEOS_QUERY.format(
        snowflake_read_db=SNOWFLAKE_READ_DB,
        snowflake_read_operations_db=SNOWFLAKE_READ_OPERATIONS_DB,
        games_for_reports_filter=games_for_reports_env_filter
    )
    try:
        logger.info("Fetching ludeos from Snowflake...")
        results = snowflake_driver.get_query_results(query)
        logger.info(f"Retrieved {len(results)} ludeos from Snowflake")
        return results
    except Exception as e:
        logger.error(f"Error fetching data from Snowflake: {str(e)}")
        return []


def _calculate_games_for_reports_filter():
    if ENV == 'staging':
        filter_str = 'is_staging = TRUE'
    else:
        filter_str = 'is_relevant = TRUE'
    return filter_str


@task
def save_results_to_snowflake(data):
    logger = get_run_logger()

    try:
        df = pd.DataFrame(data)
        df["inserted_at"] = datetime.now(timezone.utc)

        snowflake_driver.write_df_to_table(
            df=df,
            database=SNOWFLAKE_RESULTS_DB,
            schema=SNOWFLAKE_RESULTS_SCHEMA,
            table_name=SNOWFLAKE_RESULTS_TABLE,
            overwrite=False
        )
        logger.info(f"Successfully saved {len(df)} rows to Snowflake table {SNOWFLAKE_RESULTS_DB}.{SNOWFLAKE_RESULTS_SCHEMA}.{SNOWFLAKE_RESULTS_TABLE}")
    except Exception as e:
        logger.error(f"Failed to save to Snowflake: {str(e)}")
