from prefect import flow, task, get_run_logger

from config import WEAVIATE_BASE_PATH
from inserter import WeaviateObjectsInserter

inserter = WeaviateObjectsInserter()


@task()
def get_objects_from_snowflake(query_name):
    logger = get_run_logger()
    logger.info(f'getting objects from snowflake for query: {query_name}')
    objects = inserter.get_data_from_snowflake(query_name)
    logger.info(f'got {len(objects)} objects from snowflake')
    return objects


@task()
def create_collection(collection_name):
    logger = get_run_logger()
    logger.info(f'creating collection: {collection_name}')
    inserter.create_collection(name=collection_name, schema=f'{WEAVIATE_BASE_PATH}/schemas/ludeos_profile.json')


@task()
def insert_objects(collection_name, objects):
    logger = get_run_logger()
    logger.info(f'inserting objects to collection: {collection_name}')
    inserter.insert_objects(collection_name, objects)


@task()
def check_if_data_exists(collection_name):
    logger = get_run_logger()
    logger.info(f'checking if data inserted properly...')
    results = inserter.check_if_data_exists(collection_name)
    return results


@flow
def weaviate_insert_ludeos_flow(is_recreate_collection: bool = True):
    logger = get_run_logger()
    collection_name = 'LudeosProfile'
    objects = get_objects_from_snowflake(query_name='ludeos_profile')
    if is_recreate_collection:
        create_collection(collection_name)

    insert_objects(collection_name, objects)
    results = check_if_data_exists(collection_name)
    logger.info(f'check_if_data_exists result: {results}')


if __name__ == "__main__":
    is_recreate_collection = True
    weaviate_insert_ludeos_flow(is_recreate_collection)
