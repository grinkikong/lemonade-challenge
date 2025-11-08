import json
import os
from time import sleep
import numpy as np
import weaviate.classes as wvc
from weaviate.classes.config import Property, DataType
from prefect import get_run_logger

from data_utils.pythonic_utils import read_file_content
from data_utils.snowflake_utils import SnowflakeDriver
from data_utils.weaviate_utils import WeaviateDriver

from config import WEAVIATE_BASE_PATH, snowflake_secrets

ENV = os.environ['ENV']
weaviate_driver = WeaviateDriver()


class WeaviateObjectsInserter:
    def __init__(self):
        self.client = weaviate_driver.client
        self.snowflake_driver = SnowflakeDriver(
            username=snowflake_secrets['username'],
            password=snowflake_secrets['password'],
            warehouse='small'
        )

    @staticmethod
    def _load_schema_properties(path):
        output = []
        with open(path, 'r') as file:
            schema = json.load(file)
            # Assuming the first class in the array is the target class
            for prop in schema['classes'][0]['properties']:
                type_from_schema = prop['dataType'][0]
                if '[]' in type_from_schema:
                    type = type_from_schema.replace('[]', '_array')
                    output.append(Property(name=prop['name'], data_type=getattr(DataType, type.upper())))
                else:
                    output.append(Property(name=prop['name'], data_type=getattr(DataType, type_from_schema.upper())))
        return output

    def create_collection(self, name, schema):
        logger = get_run_logger()
        properties = self._load_schema_properties(schema)
        self.client.collections.delete(name)
        self.client.collections.create(
            name=name,
            properties=properties,
            vectorizer_config=wvc.config.Configure.Vectorizer.text2vec_transformers(),
        )
        logger.info(f'Collection {name} created successfully')

    @staticmethod
    def _get_snowflake_query(query_name):
        query_path = f'{WEAVIATE_BASE_PATH}/queries/{query_name}.sql'
        query = read_file_content(query_path.format(query_name=query_name))
        query = query.format(limit='' if ENV == 'production' else 'LIMIT 30')
        return query

    def get_data_from_snowflake(self, query_name):
        import json
        logger = get_run_logger()
        logger.info('getting objects from snowflake')
        query = self._get_snowflake_query(query_name)
        logger.info(f'executing query: {query}')
        data = self.snowflake_driver.get_query_results_as_df(query)
        data.columns = data.columns.str.lower()
        data = data.replace({np.nan: None})

        def parse_row(row):
            row = {k: (v if v != '' else None) for k, v in row.items()}

            # Convert comma-separated 'tags' string to list
            if 'tags' in row and isinstance(row['tags'], str):
                row['tags'] = [t.strip() for t in row['tags'].split(',') if t.strip()]

            # Convert comma-separated 'tags' string to list
            if 'environment_analysis' in row and isinstance(row['environment_analysis'], str):
                row['environment_analysis'] = [t.strip() for t in row['environment_analysis'].split(',') if t.strip()]

            # score_params: JSON string in Snowflake → store as string
            if 'score_params' in row and isinstance(row['score_params'], str):
                try:
                    parsed = json.loads(row['score_params'])
                    row['score_params'] = json.dumps(parsed, ensure_ascii=False)
                except json.JSONDecodeError:
                    pass

            return row

        data_list_of_dicts = [parse_row(row) for row in data.to_dict(orient='records')]
        logger.info(f'got {len(data_list_of_dicts)} results back\n')
        return data_list_of_dicts

    def insert_objects(self, collection_name, objects):
        logger = get_run_logger()
        collection = self.client.collections.get(collection_name)
        list_chunks = self.chunk_list(objects, chunk_size=100)
        for chunk in list_chunks:
            for _object in chunk:
                try:
                    uuid = collection.data.insert(_object, uuid=_object['ludeo_id'])
                    logger.info(f'inserted object with uuid:{uuid.__str__()}, ludeo_id: {_object["ludeo_id"]}')
                except Exception as err:
                    logger.error(f'error inserting object with ludeo_id: {_object["ludeo_id"]}. error: {err}')
                    continue
            logger.info(f'inserted {len(chunk)} objects. sleeping few seconds')
            sleep(5)

    def check_if_data_exists(self, collection_name):
        logger = get_run_logger()
        logger.info('checking if data exists:')
        collection = self.client.collections.get(collection_name)
        # get some objects
        some_objects = collection.query.fetch_objects(limit=3)
        logger.info(f'some objects: {some_objects}')

        # get near objects
        near_objects = collection.query.near_text(query="kick some asses", limit=3)
        logger.info(f'near objects: {near_objects}')
        return len(some_objects.objects) > 0

    @staticmethod
    def chunk_list(input_list, chunk_size=100):
        return [input_list[i:i + chunk_size] for i in range(0, len(input_list), chunk_size)]
