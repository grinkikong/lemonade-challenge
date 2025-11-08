import prefect.utilities.context
from prefect.blocks.system import Secret
from prefect import flow, task, get_run_logger
import os
from data_utils.datadog_utils import init_datadog
from dotenv import load_dotenv
from functions.correlation_matrix import calc_correlation_matrix
from functions.ratio_matrix import calc_ratio_matrix
from functions.upload_csv_files import upload_csv_files
from functions.fetch_game_events import fetch_game_events
from prefect.blocks.system import JSON


# initializations
load_dotenv()
DD_AGENT_HOST = os.environ['DD_AGENT_HOST']
ENV = os.environ['ENV']
init_datadog(statsd_host=DD_AGENT_HOST)


auto_scoring = JSON.load("auto-scoring")


def generate_bucket_name():
    base_bucket_name = 'advanced-scoring'
    if ENV in ('local', 'staging'):
        prefix = 'stg-use1-data'
    elif ENV == 'production':
        prefix = 'prod-use1-data'

    return f'{prefix}-{base_bucket_name}'

def generate_db_name():
    if ENV in ('local', 'staging'):
        return {'datalake_db': 'datalake_staging', 'data_warehouse_db': 'data_warehouse_staging'}
    elif ENV == 'production':
        return {'datalake_db': 'datalake', 'data_warehouse_db': 'data_warehouse'}
    else:
        raise ValueError("Invalid environment")

game_ids = auto_scoring.value["game_ids"]
endpoint = auto_scoring.value["endpoint"]
max_date = auto_scoring.value["max_date"]
auth_token = auto_scoring.value["auth_token"]
bucket_name = generate_bucket_name()


@task
def fetch_game_events_task(game_id, max_date, db_names):
    df = fetch_game_events(game_id, max_date, db_names)
    return df

@task
def calc_correlation_matrix_task(df, game_id, bucket_name):
    calc_correlation_matrix(df, game_id, bucket_name)

@task
def calc_ratio_matrix_task(df, game_id, bucket_name):
    calc_ratio_matrix(df, game_id, bucket_name)

@task
def upload_csv_files_task(game_id, process_id, endpoint, bucket_name, auth_token):
    upload_csv_files(game_id, process_id, endpoint, bucket_name, auth_token)

@task
def update_max_date_task(df, game_id):
    logger = get_run_logger()
    # update max date in prefect block
    new_max_date = df['TIMESTAMP'].max()
    auto_scoring.value["game_ids"][game_id]["max_date"] = new_max_date
    auto_scoring.save(name="auto-scoring", overwrite=True)
    logger.info(f"Updated max date to {new_max_date}")



@flow
def auto_scoring_flow():
    logger = get_run_logger()
    print(game_ids)
    keys = game_ids.keys()
    print(keys)
    for game_id in game_ids.keys():
        last_max_date = game_ids[game_id]["max_date"]
        logger.info(f"Processing game_id: {game_id} with last_max_date: {last_max_date}")
        df = fetch_game_events_task(game_id, last_max_date, generate_db_name())

        if df.empty:
            logger.info(f"No new data to process for {game_id} , end the flow.")
            continue

        calc_correlation_matrix_task(df, game_id, bucket_name)
        calc_ratio_matrix_task(df, game_id, bucket_name)

        process_id = prefect.utilities.context.get_flow_run_id()
        upload_csv_files_task(game_id, process_id, endpoint, bucket_name, auth_token)
        update_max_date_task(df, game_id)


if __name__ == "__main__":
    auto_scoring_flow()







