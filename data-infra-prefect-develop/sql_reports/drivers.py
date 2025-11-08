import os
from prefect.blocks.system import JSON

from data_utils.aws_secrets_manager_utils import AWSSecretsManagerDriver
from data_utils.snowflake_utils import SnowflakeDriver
from data_utils.slack_utils import SlackDriver
from data_utils.kafka_utils import KafkaProducerDriver
from data_utils.sqlalchemy_utils import SqlAlchemyDriver

ENV = os.environ['ENV']
KAFKA_BROKER = JSON.load("databases-hosts").value['kafka_brokers']
POSTGRES_HOST = JSON.load("databases-hosts").value['postgres_host']
POSTGRES_DATABASE_NAME = 'data_tools'
POSTGRES_PORT = 5432

secrets_manager_driver = AWSSecretsManagerDriver(env=ENV)

snowflake_secrets = secrets_manager_driver.get_secret('snowflake/prefect')
slack_secrets = secrets_manager_driver.get_secret('slack/ludata_bot')
postgres_secrets = secrets_manager_driver.get_secret('rds-instances/postgres/operations')

snowflake_driver = SnowflakeDriver(username=snowflake_secrets['username'], password=snowflake_secrets['password'])
postgres_driver = SqlAlchemyDriver(
    username=postgres_secrets['username'],
    password=postgres_secrets['password'],
    host=POSTGRES_HOST,
    port=POSTGRES_PORT,
    db_name=POSTGRES_DATABASE_NAME,
)

slack_driver = SlackDriver(slack_secrets['token'])
kafka_producer = KafkaProducerDriver(KAFKA_BROKER)
