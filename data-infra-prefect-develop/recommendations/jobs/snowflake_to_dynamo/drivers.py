import os

from data_utils.aws_secrets_manager_utils import AWSSecretsManagerDriver
from data_utils.snowflake_utils import SnowflakeDriver
from data_utils.dynamodb_utils import DynamoDBDriver
from data_utils.kafka_utils import KafkaProducerDriver

ENV = os.environ['ENV']

SNOWFLAKE_DATABASE = 'RECOMMENDATION' if ENV == 'production' else 'RECOMMENDATION_STAGING'
KAFKA_SNOWFLAKE_DATABASE = 'DATA_WAREHOUSE' if ENV == 'production' else 'DATA_WAREHOUSE_STAGING'
KAFKA_BROKER = os.environ['KAFKA_BROKER']

secrets_manager_driver = AWSSecretsManagerDriver(env=ENV)
snowflake_secrets = secrets_manager_driver.get_secret('snowflake/prefect')

snowflake_driver = SnowflakeDriver(
    username=snowflake_secrets['username'],
    password=snowflake_secrets['password'], 
    warehouse='small'
)

aws_region = 'us-east-1'
dynamodb_driver = DynamoDBDriver(region=aws_region)

kafka_producer = KafkaProducerDriver(KAFKA_BROKER).kafka_producer

PREFECT_DBT_JOB_NAME = "dbt_hourly_recommendation_ludeos"
PREFECT_DBT_FLOW_NAME = "dbt-hourly-recommendation-ludeos-flow"
