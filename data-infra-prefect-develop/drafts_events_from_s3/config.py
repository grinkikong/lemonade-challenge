import os
from datetime import datetime, timedelta

ENV = os.environ['ENV']
LOCAL_PATH_PREFIX = 'tmp_files/s3_drafts'
MAX_DAYS_BACK_TO_RUN = 7

BUCKET_ENV_PREFIX = 'prod' if ENV == 'production' else 'stg'
BUCKET_NAME = f'{BUCKET_ENV_PREFIX}-use1-platform-sdk'

SF_DB_SUFFIX = '' if ENV == 'production' else '_STAGING'
SF_TARGET_DATABASE = f'DATALAKE{SF_DB_SUFFIX}'
SF_TARGET_SCHEMA, SF_TARGET_TABLE = 'S3', 'DRAFTS_EVENTS'


"""
bucket schema for IDs:
    s3://prod-use1-platform-sdk/gameId/date/gameSessionId/gameplayId/draftId.csv
"""


def generate_date_range(start_date, end_date):
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    return [(start + timedelta(days=n)).strftime('%Y%m%d') for n in range((end - start).days + 1)]
