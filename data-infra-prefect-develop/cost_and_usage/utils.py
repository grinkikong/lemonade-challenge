import requests
from prefect import get_run_logger
from prefect.blocks.system import JSON
from data_utils.aws_secrets_manager_utils import AWSSecretsManagerDriver
from data_utils.snowflake_utils import SnowflakeDriver
from config import ENV
import datetime

# Secrets
secrets_manager_driver = AWSSecretsManagerDriver(env=ENV)
snowflake_secrets = secrets_manager_driver.get_secret('snowflake/prefect')
sf_api_user = snowflake_secrets['username']
sf_api_password = snowflake_secrets['password']

# Vars
cost_and_usage_block = JSON.load("cost-and-usage")
cost_and_usage_metadata = cost_and_usage_block.value
sf_schema = cost_and_usage_metadata["schema"]
sf_source_db = cost_and_usage_metadata["source_db"]
sf_target_db = cost_and_usage_metadata["target_db"]

# Anodot API credentials
anodot_secrets = secrets_manager_driver.get_secret('tools/anodot-api')
anodot_api_user = anodot_secrets['USERNAME']
anodot_api_password = anodot_secrets['PASSWORD']

# Initialize drivers
sf_driver = SnowflakeDriver(username=sf_api_user,
                            password=sf_api_password,
                            database=sf_source_db,
                            schema=sf_schema)


def _get_anodot_api_token():
    logger = get_run_logger()
    # Make request to get API token
    auth_creds_url = "https://tokenizer.umbrellacost.io/prod/credentials"
    auth_payload = {
        "username": anodot_api_user,
        "password": anodot_api_password
    }
    headers = {
        "Content-Type": "application/json"
    }

    auth_response = requests.post(auth_creds_url, headers=headers, json=auth_payload)

    if auth_response.status_code == 200:
        try:
            response_json = auth_response.json()
            authorization = response_json["Authorization"]
            apikey = response_json["apikey"]
            return authorization, apikey
        except ValueError:
            logger.error("Failed to parse JSON response.")
    else:
        logger.error(f"Failed to authenticate: {auth_response.status_code} - {auth_response.text}")


def get_anodot_accounts_metadata(authorization, api_key):
    logger = get_run_logger()
    users_url = "https://api.umbrellacost.io/api/v1/users"
    headers = {
        "apikey": api_key,
        "Authorization": authorization,
        "Content-Type": "application/json"
    }
    response = requests.get(users_url, headers=headers)
    if response.status_code == 200:
        try:
            accounts = response.json()['accounts']
            return accounts
        except Exception as err:
            logger.error(err)
    else:
        return None


def _concat_table_last_sync(table_name):
    return f"{table_name}_last_sync"


def _form_anodot_account_api_key(api_key: str, accountkey: str, divisionid: str):
    return f"{api_key[:-3]}:{accountkey}:{divisionid}"


def _get_last_sync(table_name):
    table_last_sync = _concat_table_last_sync(table_name)
    last_sync_str = cost_and_usage_metadata.get(table_last_sync, None)

    if last_sync_str:
        last_synced_date = datetime.datetime.strptime(last_sync_str, "%Y-%m-%d").date()
        start_date_time = last_synced_date + datetime.timedelta(days=1)  # ADD 1 DAY!
    else:
        # First run - start from 13 months ago
        start_date_time = datetime.datetime.utcnow().date() - datetime.timedelta(days=13 * 30)

    # End date is always 2 days before today (latest available data)
    end_date_time = datetime.datetime.utcnow().date() - datetime.timedelta(days=2)

    start_date = start_date_time.strftime('%Y-%m-%d')
    end_date = end_date_time.strftime('%Y-%m-%d')

    return start_date, end_date


def _update_last_sync(table_name, end_date):
    table_last_sync = _concat_table_last_sync(table_name)
    cost_and_usage_metadata[table_last_sync] = end_date
    cost_and_usage_block.save(overwrite=True)
