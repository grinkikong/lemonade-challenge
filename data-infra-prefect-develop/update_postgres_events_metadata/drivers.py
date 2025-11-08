import os

from data_utils.aws_secrets_manager_utils import AWSSecretsManagerDriver
from data_utils.sqlalchemy_utils import SqlAlchemyDriver
from data_utils.snowflake_utils import SnowflakeDriver
from data_utils.s3_utils import S3Driver
from data_utils.slack_utils import SlackDriver


ENV = os.environ.get('ENV', 'local')
POSTGRES_HOST = os.environ.get('POSTGRES_HOST')
POSTGRES_PORT = os.environ.get('POSTGRES_PORT', '5432')
POSTGRES_DATABASE_NAME = 'eventflow'
KAFKA_BROKER = os.environ['KAFKA_RND_BROKER']

secrets_manager_driver = AWSSecretsManagerDriver(ENV)

postgres_secrets = secrets_manager_driver.get_secret('rds-instances/postgres/operations')
snowflake_secrets = secrets_manager_driver.get_secret('snowflake/data-apis')
slack_secrets = secrets_manager_driver.get_secret('slack/ludata_bot')

postgres_driver = SqlAlchemyDriver(
    username=postgres_secrets['username'],
    password=postgres_secrets['password'],
    host=POSTGRES_HOST,
    port=POSTGRES_PORT,
    db_name=POSTGRES_DATABASE_NAME,
)

snowflake_driver = SnowflakeDriver(snowflake_secrets['username'], snowflake_secrets['password'])
slack_driver = SlackDriver(slack_secrets['token'])
s3_driver = S3Driver()
