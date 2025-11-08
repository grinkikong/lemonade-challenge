from prefect import flow, task, get_run_logger
import os
import pandas as pd
import shutil
from typing import List, Dict, Any, Optional

from data_utils.pythonic_utils import read_file_content
from config import (
    ENV, generate_date_range, BUCKET_NAME, LOCAL_PATH_PREFIX,
    SF_TARGET_DATABASE, SF_TARGET_SCHEMA, SF_TARGET_TABLE
)
from drivers import snowflake_driver, s3_driver
from utils import (
    get_start_and_end_dates, create_directory_if_not_exists,
    prepare_df_for_snowflake, validate_df_columns, filter_df_columns,
    process_s3_object
)

EXPECTED_COLUMNS = [
    'event_key', 'created_at', 'event_value', 'draft_id',
    'game_id', 'gameplay_id', 'game_session_id', 'inserted_at'
]

all_games_finished_successfully = True


@task
def get_game_ids() -> List[Dict[str, Any]]:
    logger = get_run_logger()
    logger.info('Fetching production game IDs')
    if ENV == 'production':
        query = read_file_content('drafts_events_from_s3/queries/get_production_games.sql')
    else:
        query = read_file_content('drafts_events_from_s3/queries/get_staging_games.sql')
    results = snowflake_driver.get_query_results(query)

    logger.info(f'Found {len(results)} production games')
    return results


def download_drafts_from_s3(game_id: str, date: str) -> List[Dict[str, str]]:
    logger = get_run_logger()
    logger.info(f'Downloading drafts from S3 for game {game_id} on {date}')

    # Create base directory if it doesn't exist
    base_path = f'{LOCAL_PATH_PREFIX}/{game_id}/{date}'
    create_directory_if_not_exists(base_path)

    # Set up pagination for S3 objects
    paginator = s3_driver.s3_client.get_paginator('list_objects_v2')
    prefix = f"{game_id}/{date}/"
    downloaded_files = []

    # Process each page of S3 objects
    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix):
        for obj in page.get('Contents', []):
            file_info = process_s3_object(
                s3_driver.s3_client, BUCKET_NAME, obj['Key'], base_path, logger
            )
            if file_info:
                downloaded_files.append(file_info)

    logger.info(f"Downloaded {len(downloaded_files)} drafts files")
    return downloaded_files


def convert_csv_to_df(
        file_path: str,
        game_id: str,
        gameplay_id: str,
        draft_id: str,
        game_session_id: str
) -> Optional[pd.DataFrame]:
    logger = get_run_logger()
    logger.info(f'Converting CSV to DataFrame: {file_path}')

    try:
        # Read CSV and convert column names to lowercase
        df = pd.read_csv(file_path, delimiter=';')
        df.columns = df.columns.str.lower()

        # Prepare DataFrame with necessary transformations
        df = prepare_df_for_snowflake(df, game_id, gameplay_id, draft_id, game_session_id)

        # Validate and filter columns
        validate_df_columns(df, EXPECTED_COLUMNS)
        df_final = filter_df_columns(df, EXPECTED_COLUMNS)

        if df_final.empty:
            logger.info(f'DataFrame is empty for draft_id: {draft_id}')
            return None

        return df_final
    except Exception as e:
        logger.error(f'Error converting CSV to DataFrame: {e}')
        return None


def process_files_to_dataframes(
        game_id: str,
        downloaded_files: List[Dict[str, str]]
) -> List[pd.DataFrame]:
    logger = get_run_logger()
    logger.info(f'Processing {len(downloaded_files)} files for game ID {game_id}')

    dfs = []
    for file_info in downloaded_files:
        df = convert_csv_to_df(
            file_info['file_path'],
            game_id,
            file_info['gameplay_id'],
            file_info['draft_id'],
            file_info['game_session_id']
        )

        if df is not None:
            logger.info(f'Adding DataFrame with {len(df)} rows')
            dfs.append(df)

    return dfs


@task
def process_date_for_game(game_id: str, game_name, date: str) -> List[pd.DataFrame]:
    logger = get_run_logger()
    logger.info(f'Processing date {date} for game {game_name} [{game_id}]')

    try:
        downloaded_files = download_drafts_from_s3(game_id, date)
        if not downloaded_files:
            logger.info(f'No files found for game {game_id} on date {date}')
            return []

        dataframes = process_files_to_dataframes(game_id, downloaded_files)
        if not dataframes:
            logger.info(f'No valid DataFrames for game {game_id} on date {date}')
            return []

        logger.info(f'Processed {len(dataframes)} DataFrames for game {game_id} on date {date}')
        return dataframes

    except Exception as e:
        logger.error(f'Error processing date {date} for game {game_id}: {e}')
        return []


@task
def upload_to_snowflake(dataframes: List[pd.DataFrame], game_id) -> int:
    """Upload combined DataFrames to Snowflake"""
    logger = get_run_logger()

    if not dataframes:
        logger.info('No DataFrames to upload to Snowflake')
        return 0

    combined_df = pd.concat(dataframes, ignore_index=True)
    row_count = len(combined_df)
    logger.info(f'Inserting DataFrame ({row_count} rows) to Snowflake')

    try:
        # Set database context
        cursor = snowflake_driver.conn.cursor()
        cursor.execute(f"USE DATABASE {SF_TARGET_DATABASE}")
        cursor.execute(f"USE SCHEMA {SF_TARGET_SCHEMA}")
        cursor.close()

        # Write to Snowflake
        snowflake_driver.write_df_to_table(
            combined_df, SF_TARGET_DATABASE, SF_TARGET_SCHEMA, SF_TARGET_TABLE, auto_create_table=True
        )
        logger.info(f'Successfully inserted {row_count} rows to Snowflake')
        return row_count
    except Exception as e:
        logger.error(f'Failed to upload to Snowflake [game_id: {game_id}]. error: {e}')
        raise


@task
def process_game(game_id: str, game_name: str, dates: List[str]) -> int:
    """Process all dates for a specific game and insert to snowflake"""
    logger = get_run_logger()
    logger.info(f'Processing game {game_name} ({game_id}) for {len(dates)} dates')
    all_dates_dataframes = []
    for date in dates:
        date_dataframes = process_date_for_game(game_id, game_name, date)
        all_dates_dataframes.extend(date_dataframes)
    if not all_dates_dataframes:
        logger.info(f'No data to upload for game {game_id}')
        return 0
    try:
        logger.info(f'Uploading {len(all_dates_dataframes)} DataFrames for game {game_id}')
        num_rows = upload_to_snowflake(all_dates_dataframes, game_id)
    except Exception as e:
        logger.error(f'Error uploading data to snowflake for game_id: {game_id}: {e}')
        global all_games_finished_successfully
        all_games_finished_successfully = False
        num_rows = 0
    return num_rows


@task
def cleanup_tmp_files():
    logger = get_run_logger()
    tmp_dir = os.path.dirname(LOCAL_PATH_PREFIX)

    try:
        if os.path.exists(tmp_dir):
            logger.info(f"Cleaning up temporary files directory: {tmp_dir}")
            shutil.rmtree(tmp_dir)
            logger.info(f"Successfully deleted temporary files directory")
        else:
            logger.info(f"No temporary files directory found at: {tmp_dir}")
    except Exception as e:
        logger.error(f"Error cleaning up temporary files: {e}")


@flow
def drafts_events_from_s3_to_snowflake_flow(start_date_param: str = "", end_date_param: str = ""):
    logger = get_run_logger()
    logger.info('STARTING Drafts from S3 to Snowflake flow')

    if start_date_param:
        logger.info(f"Running with custom date parameters: start_date={start_date_param}, end_date={end_date_param}")

    total_rows_inserted = 0

    try:
        start_date, end_date = get_start_and_end_dates(start_date_param, end_date_param)
        dates = generate_date_range(start_date, end_date)
        games = get_game_ids()
        logger.info(f'Processing {len(games)} games over {len(dates)} dates: {start_date} to {end_date}')

        for game in games:
            game_id, game_name = game['game_id'], game['game_name']
            logger.info(f'\n{"*" * 20}\nProcessing game: {game_name} ({game_id})')
            rows_inserted = process_game(game_id, game_name, dates)
            total_rows_inserted += rows_inserted

        logger.info(f'Total rows inserted: {total_rows_inserted}')

    except Exception as e:
        logger.error(f'Error in drafts_from_s3_to_snowflake_flow: {e}')

    finally:
        logger.info('Cleaning up resources')
        cleanup_tmp_files()
        logger.info('FINISHED Drafts from S3 to Snowflake flow')
        if not all_games_finished_successfully:
            raise Exception('Not all games finished successfully. See logs for details')


if __name__ == '__main__':
    drafts_events_from_s3_to_snowflake_flow()
