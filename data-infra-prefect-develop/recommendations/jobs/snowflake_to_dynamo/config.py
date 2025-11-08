import os
from typing import List
from pydantic import BaseModel

from project_utils import secrets_manager_driver

ENV = os.environ['ENV']

COLD_START_TABLE_NAME = 'prod-use1-data-cold-start-ludeos' if ENV == 'production' else 'stg-use1-data-cold-start-ludeos'
RANDOM_LUDEOS_TABLE_NAME = 'prod-use1-data-random-ludeos' if ENV == 'production' else 'stg-use1-data-random-ludeos'

MODELS_TO_DYNAMO = {
    COLD_START_TABLE_NAME: {
        'sf_table_name': 'LUDEOS_SERVE',
        'dynamo_table_name': COLD_START_TABLE_NAME,
        'entity': 'ludeos',
        'snowflake_list_column': 'sorted_ludeo_ids'
    },
    RANDOM_LUDEOS_TABLE_NAME: {
        'sf_table_name': 'RANDOM_LUDEOS_SERVE',
        'dynamo_table_name': RANDOM_LUDEOS_TABLE_NAME,
        'entity': 'ludeos',
        'snowflake_list_column': 'sorted_ludeo_ids'
    }
}

# Kafka flow configuration - each query gets its own instance
MODELS_TO_KAFKA = {
    'most_popular': {
        'model_kind': 'most_popular',
        'query_file': 'kind/most_popular.sql'
    },
    'random_ludeos': {
        'model_kind': 'random',
        'query_file': 'kind/random_ludeos.sql'
    },
    'most_popular_all_games': {
        'model_kind': 'most_popular_all_games',
        'query_file': 'kind/most_popular_all_games.sql'
    }
}



class KafkaRecommendationMessage(BaseModel):
    model_kind: str
    game_id: str
    game_name: str
    sorted_ludeos: List[str]


class KafkaAllGamesMessage(BaseModel):
    model_kind: str
    sorted_ludeos: List[str]


KAFKA_TOPIC = 'data.recommendations.cold-start'

snowflake_secrets = secrets_manager_driver.get_secret('snowflake/dbt')
dbt_cloud_api_secrets = secrets_manager_driver.get_secret('dbt-cloud/api')
