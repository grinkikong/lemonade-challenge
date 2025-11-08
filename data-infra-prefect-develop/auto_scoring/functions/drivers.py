import os
from data_utils.aws_secrets_manager_utils import AWSSecretsManagerDriver
from data_utils.snowflake_utils import SnowflakeDriver


ENV = os.environ["ENV"]
SF_ACCOUNT_VIRGINIA = 'gfb45097.us-east-1'

secrets_manager_driver = AWSSecretsManagerDriver(env=ENV)


snowflake_secrets = secrets_manager_driver.get_secret('snowflake/prefect')
snowflake_driver = SnowflakeDriver(
    account=SF_ACCOUNT_VIRGINIA, username=snowflake_secrets['username'], password=snowflake_secrets['password'], warehouse='small'
)
