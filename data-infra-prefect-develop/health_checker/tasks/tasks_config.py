import os

ENV = os.environ['ENV']
TIMESTAMP_FORMAT = '%Y-%m-%d %H:%M:%S.%f %Z'

# DBs
ATHENA_DB = 'eventflow'
RAW_EVENTS_TABLE = 'raw_events'
SF_ACCOUNT_VIRGINIA = 'gfb45097.us-east-1'
SNOWFLAKE_DB = 'DATALAKE' if ENV == 'production' else 'DATALAKE_STAGING'
SNOWFLAKE_SPARK_SCHEMA = 'SPARK_EMR'

# APi's
APIS_BASE_PATH = f'http://data-api.stg.use1.ludeo.com' if ENV == 'staging' else 'http://data-api.ludeo.com'
RECOMMENDATIONS_EOR = 'recommendations_eor'
RECOMMENDATIONS_IN_GAME_DISCOVERY = 'recommendations_in_game_discovery'


# Queries
ATHENA_FRESHNESS_QUERY = (
    'SELECT MAX({timestamp_column}) as max_timestamp '
    'FROM {table_name} '
    'WHERE DATE({timestamp_column}) >= DATE_ADD(\'day\', -2, CURRENT_DATE)'
)
SNOWFLAKE_FRESHNESS_QUERY = (
    'SELECT MAX({timestamp_column}) as max_timestamp '
    'FROM {table_name} '
    'WHERE DATE({timestamp_column}) >= CURRENT_DATE-2'
)

# Freshness
DATALAKE_FRESHNESS_CHECKS = {
    'raw_events': {
        'db': 'snowflake',
        'table_name': f'{SNOWFLAKE_DB}.{SNOWFLAKE_SPARK_SCHEMA}.RAW_EVENTS',
        'timestamp_column': 'timestamp',
        'freshness_threshold_minutes': 30,
        'is_staging_check': True
    },
    'enriched_events': {
        'db': 'snowflake',
        'table_name': f'{SNOWFLAKE_DB}.{SNOWFLAKE_SPARK_SCHEMA}.ENRICHED_EVENTS',
        'timestamp_column': 'inserted_at',
        'freshness_threshold_minutes': 30,
        'is_staging_check': True
    },
    'game_events': {
        'db': 'snowflake',
        'table_name': f'{SNOWFLAKE_DB}.{SNOWFLAKE_SPARK_SCHEMA}.GAME_EVENTS',
        'timestamp_column': 'created_at',
        'freshness_threshold_minutes': 60*8,
        'is_staging_check': False
    },
    'system_events': {
        'db': 'snowflake',
        'table_name': f'{SNOWFLAKE_DB}.{SNOWFLAKE_SPARK_SCHEMA}.SYSTEM_EVENTS',
        'timestamp_column': 'created_at',
        'freshness_threshold_minutes': 30,
        'is_staging_check': True
    },
    'sdk_events': {
        'db': 'snowflake',
        'table_name': f'{SNOWFLAKE_DB}.{SNOWFLAKE_SPARK_SCHEMA}.SDK_EVENTS',
        'timestamp_column': 'created_at',
        'freshness_threshold_minutes': 60*8,
        'is_staging_check': False
    },
}

# Recommendations API
HEALTH_CHECKER_API_PAYLOADS = {
    'production': {
        'recommendations_eor': {
            "userId": "81654b1a-6411-48a5-a7fc-bb6f50a8ed89",
            "gameId": "b19a05ca-c951-4976-a1bf-9e6e18ca25ff",
            "args": {
                "count": 5
            },
            "dryRun": True
        },
        'recommendations_in_game_discovery': {
            "userId": "81654b1a-6411-48a5-a7fc-bb6f50a8ed89",
            "gameId": "b19a05ca-c951-4976-a1bf-9e6e18ca25ff",
            "args": {
                "count": 6
            },
            "dryRun": True
        }
    },
    'staging': {
        'recommendations_eor': {
            "userId": "9197f0b6-8a25-401d-be64-fbdd66abc1f9",
            "gameId": "0eaa2036-748a-42df-9e79-70f46165fa1a",
            "args": {
                "ludeoId": "66c37acc-9a4c-4a96-aee6-86e5caefaf94",
                "count": 5
            },
            "dryRun": True
        },
        'recommendations_in_game_discovery': {
            "userId": "c641310c-65dd-4f5d-998c-eff7144d8ce9",
            "gameId": "0eaa2036-748a-42df-9e79-70f46165fa1a",
            "args": {
                "count": 5
            },
            "dryRun": True
        }
    }
}

API_URLS = [
    {
        'name': RECOMMENDATIONS_EOR,
        'url': f'{APIS_BASE_PATH}/recommendations/endOfRound',
        'payload': HEALTH_CHECKER_API_PAYLOADS[ENV][RECOMMENDATIONS_EOR]
    },
    {
        'name': RECOMMENDATIONS_IN_GAME_DISCOVERY,
        'url': f'{APIS_BASE_PATH}/recommendations/inGame/recommended',
        'payload': HEALTH_CHECKER_API_PAYLOADS[ENV][RECOMMENDATIONS_IN_GAME_DISCOVERY]
    }
]
