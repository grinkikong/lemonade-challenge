import requests
import uuid
from datetime import datetime
from functions.s3_utils import S3Utils
from functions.correlation_to_snowflake import load_correlation_to_snowflake
from functions.ratio_to_snowflake import load_ratio_to_snowflake
from prefect import get_run_logger

current_datetime = datetime.now()
formatted_datetime = current_datetime.strftime("%Y_%m_%d")


def _upload_file_to_endpoint(file_path, url, file_id, auth_token):
    logger = get_run_logger()
    with open(file_path, 'rb') as file:
        payload = file.read()

    headers = {'Content-Type': 'application/octet-stream', 'filename': file_id, 'Authorization': auth_token}

    logger.info(f"Uploading file to endpoint: {url}")

    response = requests.put(url, headers=headers, data=payload)

    logger.info(f"response code from endpoint: {response.status_code}")

    logger.info(f"Response from endpoint: {response.text}")

def upload_csv_files(game_id, process_id, endpoint, bucket_name, auth_token):


    ratio_file_name = "ratio_matrix.pkl"
    correlation_file_name = "correlation_matrix.pkl"

    # take the ratio matrix and send it to endpoint
    ratio_unique_id = str(uuid.uuid4())
    correlation_unique_id = str(uuid.uuid4())

    ratio_csv_path = f'/tmp/{game_id}/ratio_{game_id}_{formatted_datetime}_{ratio_unique_id}.csv'
    correlation_csv_path = f'/tmp/{game_id}/correlation_{game_id}_{formatted_datetime}_{correlation_unique_id}.csv'


    ratio_matrix = S3Utils.pickle_to_df(f'{game_id}/{ratio_file_name}', bucket_name)
    correlation_matrix = S3Utils.pickle_to_df(f'{game_id}/{correlation_file_name}', bucket_name)

    ratio_matrix.to_csv(ratio_csv_path, index=True)
    correlation_matrix.to_csv(correlation_csv_path, index=True)

    ratio_url = f'{endpoint}/{game_id}/ratio'
    correlation_url = f'{endpoint}/{game_id}/correlation'

    _upload_file_to_endpoint(ratio_csv_path, ratio_url, ratio_unique_id, auth_token)
    _upload_file_to_endpoint(correlation_csv_path, correlation_url, correlation_unique_id, auth_token)

    # Write results to Snowflake
    load_ratio_to_snowflake(ratio_matrix, game_id, process_id=process_id, file_id=ratio_unique_id)
    load_correlation_to_snowflake(correlation_matrix, game_id, process_id=process_id, file_id=correlation_unique_id)



