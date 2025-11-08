import os

from data_utils.aws_secrets_manager_utils import AWSSecretsManagerDriver
from data_utils.snowflake_utils import SnowflakeDriver
from data_utils.s3_utils import S3Driver

ENV = os.environ['ENV']

secrets_manager_driver = AWSSecretsManagerDriver(env=ENV)
snowflake_secrets = secrets_manager_driver.get_secret('snowflake/prefect')

snowflake_driver = SnowflakeDriver(username=snowflake_secrets['username'], password=snowflake_secrets['password'])
s3_driver = S3Driver()
