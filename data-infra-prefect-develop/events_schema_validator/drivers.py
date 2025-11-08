from data_utils.logger import init_logger
from data_utils.aws_secrets_manager_utils import AWSSecretsManagerDriver
from data_utils.snowflake_utils import SnowflakeDriver
from data_utils.slack_utils import SlackDriver
from data_utils.github_utils import GithubDriver
from data_utils.kafka_utils import DeadLetterProducer

from config import ENV, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, KAFKA_BROKER


# LOGGERS
logger = init_logger('events-schema-validator')
logger.info(f'running on ENV: {ENV}')



def _get_secrets():
    secrets_manager_driver = AWSSecretsManagerDriver(ENV)
    slack_secrets = secrets_manager_driver.get_secret('slack/ludata_bot')
    github_secrets = secrets_manager_driver.get_secret('tools/github')
    snowflake_secrets = secrets_manager_driver.get_secret('snowflake/prefect')
    return {
        'slack': slack_secrets, 
        'github': github_secrets,
        'snowflake': snowflake_secrets
    }


def init_drivers():
    logger.info('initializing drivers...')
    secrets = _get_secrets()
    
    # Initialize Snowflake driver
    sf_api_user = secrets['snowflake']['username']
    sf_api_password = secrets['snowflake']['password']
    snowflake_driver = SnowflakeDriver(
        username=sf_api_user, 
        password=sf_api_password, 
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )
    
    # Initialize other drivers
    slack_driver = SlackDriver(secrets['slack']['token'])
    github_driver = GithubDriver(secrets['github']['token'])
    dlq = DeadLetterProducer(KAFKA_BROKER, service_name=f'event_schema_validator')
    
    logger.info('finished initializing drivers')
    return snowflake_driver, slack_driver, github_driver, dlq
