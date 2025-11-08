import logging
import os
import weaviate

from data_utils.logger import init_logger

logger = init_logger('weaviate_reader')
httpcore_logger = logging.getLogger('httpcore.http11')
httpcore_logger.setLevel(logging.INFO)
httpcore_logger = logging.getLogger('httpx')
httpcore_logger.setLevel(logging.INFO)
httpcore_logger = logging.getLogger('httpcore.connection')
httpcore_logger.setLevel(logging.INFO)

COHERE_KEY = os.environ['COHERE_KEY']
OPENAI_API_KEY = os.environ['OPENAI_API_KEY']


class WeaviateObjectsReader:
    def __init__(self):
        # self.client = weaviate.connect_to_embedded()
        self.client = weaviate.connect_to_local(
            port=8080, grpc_port=50051, skip_init_checks=True,
            headers={
                "X-Cohere-Api-Key": COHERE_KEY,
                "X-OpenAI-Api-Key": OPENAI_API_KEY
            }
        )

    def get_objects(self, collection_name):
        logger.info(f'checking if data exists in collection {collection_name}')
        collection = self.client.collections.get(collection_name)

        all_objects = collection.query.fetch_objects()
        logger.info(f'there are {len(all_objects.objects)} objects in the collection')
        print('\n')

        some_objects = collection.query.fetch_objects(limit=3)
        logger.info(f'some objects: {some_objects}')

        near_objects = collection.query.near_text(query="kick some asses", limit=3)
        logger.info(f'near objects: {near_objects}')
        return len(some_objects.objects) > 0


if __name__ == "__main__":
    collection_name = 'LudeosProfile'
    reader = WeaviateObjectsReader()
    reader.get_objects(collection_name)
