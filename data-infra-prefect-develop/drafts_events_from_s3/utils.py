import logging
import os
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from data_utils.logger import init_logger

from drivers import snowflake_driver
from config import SF_TARGET_DATABASE, SF_TARGET_SCHEMA, SF_TARGET_TABLE, MAX_DAYS_BACK_TO_RUN

logger = init_logger('drafts_events_from_s3.utils')


def get_start_and_end_dates(
        custom_start_date: Optional[str] = None, custom_end_date: Optional[str] = None
) -> Tuple[str, str]:

    if custom_start_date and custom_end_date:
        logger.info(f"Using custom date range: {custom_start_date} to {custom_end_date}")
        return custom_start_date, custom_end_date

    logger.info("Determining date range dynamically")

    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    try:
        query = f"""
        SELECT 
            TO_CHAR(MAX(TO_DATE(SUBSTRING(created_at, 1, 10))), 'YYYY-MM-DD') as max_date 
        FROM {SF_TARGET_DATABASE}.{SF_TARGET_SCHEMA}.{SF_TARGET_TABLE}
        """
        result = snowflake_driver.get_query_results(query)

        if result and result[0]['max_date']:
            max_date = result[0]['max_date']
            start_date = (datetime.strptime(max_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')

            logger.info(f"Found max date in Snowflake: {max_date}, setting start_date to {start_date}")
        else:
            start_date = (datetime.strptime(yesterday, '%Y-%m-%d') - timedelta(days=MAX_DAYS_BACK_TO_RUN)).strftime(
                '%Y-%m-%d')
            logger.info(f"No data found in table, using default 7-day lookback: {start_date}")

    except Exception as e:
        logger.error(f"Error getting max date from Snowflake: {e}")
        start_date = (datetime.strptime(yesterday, '%Y-%m-%d') - timedelta(days=MAX_DAYS_BACK_TO_RUN)).strftime(
            '%Y-%m-%d')
        logger.info(f"Using default 7-day lookback due to error: {start_date}")

    min_allowed_date = (datetime.strptime(yesterday, '%Y-%m-%d') - timedelta(days=MAX_DAYS_BACK_TO_RUN)).strftime(
        '%Y-%m-%d')
    if start_date < min_allowed_date:
        logger.info(f"Limiting start_date to 7 days before yesterday: {min_allowed_date}")
        start_date = min_allowed_date

    end_date = yesterday

    logger.info(f"Final date range: {start_date} to {end_date}")
    return start_date, end_date


def create_directory_if_not_exists(directory_path: str) -> None:
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)


def extract_path_components(s3_key: str) -> Dict[str, str]:
    path_parts = s3_key.split('/')

    # Only process files that match the expected pattern
    if len(path_parts) >= 5:  # game_id/date/game_session_id/gameplay_id/draft_id.csv
        return {
            'game_session_id': path_parts[2],
            'gameplay_id': path_parts[3],
            'filename': path_parts[-1],
            'draft_id': path_parts[-1].split('.')[0]
        }
    return {}


def prepare_df_for_snowflake(
        df: pd.DataFrame,
        game_id: str,
        gameplay_id: str,
        draft_id: str,
        game_session_id: str
) -> pd.DataFrame:
    # Add extra columns
    df['game_id'] = game_id
    df['draft_id'] = draft_id
    df['gameplay_id'] = gameplay_id
    df['game_session_id'] = game_session_id
    df['inserted_at'] = datetime.utcnow().isoformat() + 'Z'

    # Rename columns
    df.rename(columns={
        'eventkey': 'event_key',
        'event_time': 'created_at',
        'gameid': 'game_id',
        'gameplayid': 'gameplay_id',
    }, inplace=True)

    # Ensure event_key and event_value are always strings
    if 'event_key' in df.columns:
        df['event_key'] = df['event_key'].astype(str)

    if 'event_value' in df.columns:
        df['event_value'] = df['event_value'].astype(str)

    # Format datetime field
    if 'created_at' in df.columns:
        df['created_at'] = pd.to_datetime(df['created_at'], unit='ms', utc=True).dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    return df


def validate_df_columns(df: pd.DataFrame, expected_columns: List[str]) -> None:
    """Validate that DataFrame contains all expected columns"""
    missing_cols = [col for col in expected_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing columns in the DataFrame: {missing_cols}")


def filter_df_columns(df: pd.DataFrame, columns_to_keep: List[str]) -> pd.DataFrame:
    """Return DataFrame with only the specified columns"""
    return df[columns_to_keep]


def process_s3_object(
        s3_client,
        bucket_name: str,
        key: str,
        base_path: str,
        logger: Optional[logging.Logger] = None
) -> Optional[Dict[str, str]]:
    """Process a single S3 object and download it if it's a CSV file"""
    if not key.endswith('.csv'):
        return None

    path_components = extract_path_components(key)
    if not path_components:
        return None

    game_session_id = path_components['game_session_id']
    gameplay_id = path_components['gameplay_id']
    filename = path_components['filename']
    draft_id = path_components['draft_id']

    # Create proper directory structure
    local_dir = os.path.join(base_path, game_session_id, gameplay_id)
    create_directory_if_not_exists(local_dir)

    local_file_path = os.path.join(local_dir, filename)

    # Download file using S3 client
    try:
        s3_client.download_file(bucket_name, key, local_file_path)
        return {
            'file_path': local_file_path,
            'gameplay_id': gameplay_id,
            'draft_id': draft_id,
            'game_session_id': game_session_id
        }
    except Exception as e:
        if logger:
            logger.error(f"Failed to download {key}: {e}")
        return None
