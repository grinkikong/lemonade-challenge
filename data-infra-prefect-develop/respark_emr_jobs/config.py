import os

ENV = os.environ['ENV']
ICEBERG_TABLES_JOBS = ['iceberg-raw-events', 'iceberg-enriched-events', 'iceberg-game-events', 'iceberg-system-events', 'iceberg-sdk-events', 'iceberg-external-events']

SLACK_CHANNEL = (
    '#data-engineering-notifications' if ENV == 'production' else '#monitoring-data-staging'
)
EMR_SPARK_S3_BUCKET = f"{'prod' if ENV == 'production' else 'stg'}-use1-data-emr-spark"
CLUSTER_NAME = "staging-use1-eks" if ENV == "staging" else "production-use1-eks"
