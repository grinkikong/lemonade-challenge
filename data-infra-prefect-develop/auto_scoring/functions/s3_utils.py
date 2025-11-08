import boto3
import pandas as pd
import os
from prefect import get_run_logger

ENV = os.environ.get('ENV', 'staging')

class S3Utils:

    @staticmethod
    def pickle_to_df(file_name, bucket_name):

        game_id = file_name.split('/')[0]

        # create temporary folder for game if not exists
        mkdir_command = f"mkdir -p /tmp/{game_id}"
        os.system(mkdir_command)

        # download from s3 only for non-local environments

        s3 = boto3.client('s3')
        s3.download_file(
            Bucket=bucket_name,  # Access using class name
            Key=file_name,
            Filename=f'/tmp/{file_name}'
        )

        df = pd.read_pickle(f'/tmp/{file_name}')

        return df

    @staticmethod
    def df_to_pickle(df, file_name, bucket_name):

        logger = get_run_logger()
        game_id = file_name.split('/')[0]

        # create temporary folder for game if not exists
        mkdir_command = f"mkdir -p /tmp/{game_id}"
        os.system(mkdir_command)

        df.to_pickle(f'/tmp/{file_name}')
        logger.info(f"file_name: {file_name} is saved to /tmp")

        s3 = boto3.client('s3')
        s3.upload_file(
            Filename=f'/tmp/{file_name}',
            Bucket=bucket_name,
            Key=file_name
        )



