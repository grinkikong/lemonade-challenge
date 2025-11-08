from typing import List
from prefect import get_run_logger
import json

from health_checker.tasks.tasks_config import ENV, DATALAKE_FRESHNESS_CHECKS
from health_checker.tasks.drivers import snowflake_driver


def _is_snowpipe_auto_refresh_running(table: str, params):
    logger = get_run_logger()
    logger.info(f'--  checking snowpipe freshness for: {table}')
    full_table_name, is_staging_check  = params['table_name'] ,params['is_staging_check']
    if ENV == 'staging' and not is_staging_check:
        logger.info(f'skipping table: {table} because is_staging_check=False')
        return
    try:
        system_query_structure = f"system$auto_refresh_status('{full_table_name.lower()}')"
        system_query = f"SELECT {system_query_structure};"
        query_results = snowflake_driver.get_query_results(system_query)
        if len(query_results) == 0:
            failure_msg = f'{table} Snowpipe auto-refresh cant be found'
            logger.error(failure_msg)
            return failure_msg

        snowpipe_metadata = json.loads(query_results[0][system_query_structure])
        pipe_state = snowpipe_metadata.get('executionState')
        if pipe_state == 'RUNNING':
            logger.info(f'{full_table_name} Snowpipe auto-refresh is active')
        else:
            failure_msg = f'{full_table_name} Snowpipe auto-refresh is NOT active'
            logger.error(failure_msg)
            return failure_msg

    except Exception as e:
        failure_msg = f'Error checking Snowpipe auto-refresh status for table {full_table_name}: {e}'
        logger.error(failure_msg)
        return failure_msg


def health_check_snowpipe_freshness() -> List:
    logger = get_run_logger()
    failures = []
    for table, params in DATALAKE_FRESHNESS_CHECKS.items():
        try:
            results = _is_snowpipe_auto_refresh_running(table, params)
            if results:
                failures.append(results)
        except Exception as e:
            logger.error(f'failed to check snowpipe: {table} due to: {e}')

    if len(failures) == 0:
        logger.info('all snowpipes are active')
    else:
        logger.info(f'found {len(failures)} snowpipe that are not active: {failures}')
    return failures
