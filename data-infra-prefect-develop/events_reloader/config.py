import os

ENV = os.environ['ENV']
TARGET_DB = 'datalake' if ENV == 'production' else 'datalake_staging'
EVENTS_HANDLER_API_URL = ('http://data-api.ludeo.com' if ENV == 'production' else 'https://data-api.stg.use1.ludeo.com')
SLACK_CHANNEL = ('#data-engineering-notifications' if ENV == 'production' else '#monitoring-data-staging')
NUM_THREADPOOL_EXECUTORS = 60 if ENV == 'production' else 10
SEND_SLACK_THRESHOLD = 10

# API Retry Configuration
MAX_RETRIES = 3
BACKOFF_FACTOR = 1
STATUS_FORCELIST = [500, 502, 503, 504]
