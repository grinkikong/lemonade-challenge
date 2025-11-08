from datetime import datetime, timedelta
from typing import List
from prefect import get_run_logger


from health_checker.tasks.tasks_config import ENV, DATALAKE_FRESHNESS_CHECKS, \
    ATHENA_FRESHNESS_QUERY, SNOWFLAKE_FRESHNESS_QUERY, TIMESTAMP_FORMAT
from health_checker.tasks.drivers import athena_driver, snowflake_driver


def _generate_failure_messages(db, table, time_diff, max_created_at):
    minutes_behind = round(time_diff.total_seconds() / 60, 1)
    failure_msg = f'table: {table} [{db}] is not fresh\ntime behind: {minutes_behind} minutes ' \
                  f'[max created_at: {max_created_at}]'
    return failure_msg


def _get_athena_max_timestamp_value(query_raw_results):
    max_created_at = datetime.strptime(query_raw_results[0]['max_timestamp'], TIMESTAMP_FORMAT)
    return max_created_at


def _get_snowflake_max_timestamp_value(query_raw_results):
    max_created_at = query_raw_results[0]['max_timestamp']
    return max_created_at


def check_athena_freshness(table, timestamp_column):
    query = ATHENA_FRESHNESS_QUERY.format(timestamp_column=timestamp_column, table_name=table)
    query_results = athena_driver.query_results_to_list(query)
    max_created_at = _get_athena_max_timestamp_value(query_results)
    return max_created_at


def check_snowflake_freshness(table, timestamp_column):
    query = SNOWFLAKE_FRESHNESS_QUERY.format(timestamp_column=timestamp_column, table_name=table)
    query_results = snowflake_driver.get_query_results(query)
    max_created_at = _get_snowflake_max_timestamp_value(query_results)
    return max_created_at


def _run_single_table_check(table, params) -> str:
    logger = get_run_logger()
    logger.info(f'--  checking datalake freshness for: {table}')
    db, table, timestamp_column, is_staging_check = params['db'], params['table_name'], params['timestamp_column'], params['is_staging_check']
    if ENV == 'staging' and not is_staging_check:
        logger.info(f'skipping table: {table} because is_staging_check=False')
        return None
    max_timestamp = _get_max_timestamp(db, table, timestamp_column)
    if not max_timestamp:
        logger.info(f'table: {db}.{table} is NOT fresh [max_timestamp is None]')
        failure_msg = f'max_timestamp is NONE. no recent data in table: {db}.{table}'
        return failure_msg
    time_diff = datetime.utcnow() - max_timestamp.replace(tzinfo=None)
    time_diff_seconds = round(time_diff.total_seconds() / 60, 1)
    if time_diff > timedelta(minutes=params['freshness_threshold_minutes']):
        logger.info(f'table: {db}.{table} is NOT fresh [max_timestamp is {time_diff_seconds} minutes behind]')
        results = _generate_failure_messages(db, table, time_diff, max_timestamp)
    else:
        logger.info(f'table: {db}.{table} is fresh [max_timestamp is {time_diff_seconds} minutes behind]')
        results = None
    logger.info(f'-- done checking table: {table}')
    return results


def _get_max_timestamp(db, table, timestamp_column):
    if db == 'athena':
        max_timestamp = check_athena_freshness(table, timestamp_column)
    elif db == 'snowflake':
        max_timestamp = check_snowflake_freshness(table, timestamp_column)
    else:
        raise ValueError(f'unsupported db: {db}')

    return max_timestamp


def health_check_datalake_freshness() -> List:
    logger = get_run_logger()
    failures = []
    for table, params in DATALAKE_FRESHNESS_CHECKS.items():
        try:
            results = _run_single_table_check(table, params)
            if results:
                failures.append(results)
        except Exception as e:
            logger.error(f'failed to check table: {table} due to: {e}')
    if len(failures) == 0:
        logger.info('all tables are fresh')
    else:
        logger.info(f'found {len(failures)} tables that are not fresh: {failures}')
    return failures
