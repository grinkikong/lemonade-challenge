from data_utils.athena_utils import AthenaDriver
from data_utils.snowflake_utils import SnowflakeDriver
from project_utils import secrets_manager_driver
from health_checker.tasks.tasks_config import SF_ACCOUNT_VIRGINIA, ATHENA_DB


snowflake_secrets = secrets_manager_driver.get_secret('snowflake/prefect')

snowflake_driver = SnowflakeDriver(
    account=SF_ACCOUNT_VIRGINIA, username=snowflake_secrets['username'], password=snowflake_secrets['password'], warehouse='small'
)

athena_driver = AthenaDriver(database=ATHENA_DB)
