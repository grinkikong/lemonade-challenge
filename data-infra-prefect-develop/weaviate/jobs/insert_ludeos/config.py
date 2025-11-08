import logging

from data_utils.logger import init_logger

from project_utils import secrets_manager_driver

WEAVIATE_BASE_PATH = 'weaviate'

# COHERE_KEY = os.environ['COHERE_KEY']
# OPENAI_API_KEY = os.environ['OPENAI_API_KEY']

snowflake_secrets = secrets_manager_driver.get_secret('snowflake/prefect')

logger = init_logger('weaviate_writer')
httpcore_logger = logging.getLogger('httpcore.http11')
httpcore_logger.setLevel(logging.INFO)
httpcore_logger = logging.getLogger('httpx')
httpcore_logger.setLevel(logging.INFO)
httpcore_logger = logging.getLogger('httpcore.connection')
httpcore_logger.setLevel(logging.INFO)

GAME_IDS = {
    'production':
        """
        '3ff22a7e-0352-4edb-a8b0-841fced1716e', -- NecroBouncer
        '8256bed6-9a13-44d1-845c-5dfb65a0d1bb', -- 1V1.LOL
        '25cfee51-aee0-4a46-9fa3-25ec5cc6872e', -- redacted
        'f47176b0-883c-40be-a5f8-10f4c3e7da9c', -- hitman
        '847e75ac-ad17-4458-b569-034dbb2fe1e9', -- deadlink
        '9469d358-cf38-4f69-b8a7-17bbbc558552', -- lost_castle
        """,
    'staging':
        """
        '459b4787-efd8-4fb1-a2ca-356602f3fd6d', -- My FPSGameStarterKit
        '0eaa2036-748a-42df-9e79-70f46165fa1a'  -- freedom forces
        """,
    'local':
        """
        '3ff22a7e-0352-4edb-a8b0-841fced1716e', -- NecroBouncer
        '8256bed6-9a13-44d1-845c-5dfb65a0d1bb', -- 1V1.LOL
        '25cfee51-aee0-4a46-9fa3-25ec5cc6872e', -- redacted
        'f47176b0-883c-40be-a5f8-10f4c3e7da9c', -- hitman
        '847e75ac-ad17-4458-b569-034dbb2fe1e9', -- deadlink
        '9469d358-cf38-4f69-b8a7-17bbbc558552', -- lost_castle
        """
}
