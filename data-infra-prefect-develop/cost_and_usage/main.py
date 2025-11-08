from prefect import flow, task, get_run_logger
import pandas as pd
import datetime
import requests
from utils import _form_anodot_account_api_key, sf_driver, _update_last_sync, _get_last_sync, get_anodot_accounts_metadata, _get_anodot_api_token, sf_source_db, sf_schema
from config import (
    AWS_GAMELYFT_SERVICE_NAME,
    CLOUD_PROVIDERS,
    TARGET_TABLE
)

COST_AND_USAGE_URL = "https://api.umbrellacost.io/api/v2/invoices/cost-and-usage"


@task
def _transform_data(data: list[dict], account_config: dict, account_id: str, cloud_provider: str):
    logger = get_run_logger()
    df = pd.DataFrame(data)
    if df.empty:
        logger.info(f'No Anodot {cloud_provider.upper()} data fetched')
        return df
    
    df['account_id'] = account_id
    df['account_name'] = account_config['name']
    df['cloud_provider'] = cloud_provider.upper()

    if cloud_provider == 'gcp':
        df = df.drop(columns=['group_by','group_by_secondary', 'resource_name', 'resource_id'])
        df.rename(columns={'region_tag_name': 'region'}, inplace=True)
        df['instance_type'] = df['instance_type'].replace("Not Available", None)

    elif cloud_provider == 'aws':
        df = df.drop(columns=['group_by', 'group_by_secondary', 'resource_name', 'resource_id'])
        df.rename(columns={"usage_type": "instance_type"}, inplace=True)
        df.rename(columns={'region_tag_name': 'region'}, inplace=True)
        df['service_name'] = AWS_GAMELYFT_SERVICE_NAME

    
    return df

@task
def insert_data_to_snowflake(df: pd.DataFrame, table_name):
    logger = get_run_logger()
    logger.info(f'Inserting {table_name} data to snowflake')
    df['inserted_at'] = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    sf_driver.write_df_to_table(df, database=sf_source_db, schema=sf_schema, table_name=table_name)


@task
def fetch_anodot_data(account_id: str, account_config: dict, cloud_provider: str, anodot_account_id: str, start_date_str: str, end_date_str: str, account_metadata: dict, authorization: str, apikey: str):
    logger = get_run_logger()
    logger.info(f'Fetching {cloud_provider.upper()} data from Anodot for account {account_id}')
    
    # Find the correct account metadata
    anodot_account = None
    for account in account_metadata:
        if account.get('accountId') == anodot_account_id:
            anodot_account = account
            break
    
    if not anodot_account:
        logger.error(f"No Anodot account found for {anodot_account_id}")
        return pd.DataFrame()
    
    account_api_key = _form_anodot_account_api_key(apikey, anodot_account['accountKey'], anodot_account['divisionId'])
    
    # Build request parameters
    request_params = {
        "startDate": start_date_str,
        "endDate": end_date_str,
        "costType": "cost",
        "filters[linkedaccid]": account_id,
        **account_config['request_params']
    }
    
    headers = {
        "apikey": account_api_key,
        "Authorization": authorization,
        "Content-Type": "application/json"
    }
    
    response = requests.get(COST_AND_USAGE_URL, headers=headers, params=request_params)
    
    if response.status_code == 200:
        data = response.json()['data']
        df = _transform_data(data, account_config, account_id, cloud_provider)
        return df
    else:
        logger.error(f"Failed to fetch {cloud_provider.upper()} data: {response.status_code} - {response.text}")
        return pd.DataFrame()


@task
def run_process(account_id, account_config, cloud_provider, anodot_account_id, start_date, end_date, accounts_metadata, authorization, apikey):
    logger = get_run_logger()
    logger.info(f"Processing {cloud_provider.upper()} account {account_id} ({account_config['name']})")
    
    try:
        data = fetch_anodot_data(account_id, account_config, cloud_provider, anodot_account_id, start_date, end_date, accounts_metadata, authorization, apikey)
        if not data.empty:
            insert_data_to_snowflake(data, TARGET_TABLE)
        else:
            logger.info(f"No data to fetch for {TARGET_TABLE} in account {account_id}")
    except Exception as err:
        logger.error(f"Error processing account {account_id}: {err}")


@flow()
def cost_and_usage_flow():
    logger = get_run_logger()
    
    # Get Anodot API credentials and accounts metadata
    authorization, apikey = _get_anodot_api_token()
    accounts_metadata = get_anodot_accounts_metadata(authorization, apikey)
    
    if not accounts_metadata:
        logger.error("Failed to get Anodot accounts metadata")
        return

    # Get date range
    start_date, end_date = _get_last_sync(TARGET_TABLE)
    logger.info("Syncing data between %s and %s", start_date, end_date)
    # Process each cloud provider and their linked accounts
    for cloud_provider, provider_config in CLOUD_PROVIDERS.items():
        logger.info(f"Processing {cloud_provider.upper()} accounts")
        anodot_account_id = provider_config['anodot_account_id']
        
        for account_id, account_config in provider_config['linked_account'].items():
            run_process(account_id, account_config, cloud_provider, anodot_account_id, start_date, end_date, accounts_metadata, authorization, apikey)
    
    _update_last_sync(TARGET_TABLE, end_date)


if __name__ == "__main__":
    cost_and_usage_flow()
