import logging
import os
from azure.cosmos import CosmosClient, PartitionKey, exceptions
from azure.cosmos.container import ContainerProxy
from azure.cosmos.database import DatabaseProxy

class DatabaseConnection():
    def __init__(self):
        # Initialize Cosmos Client
        self.client = CosmosClient(os.environ["ACCOUNT_URI"], credential=os.environ["ACCOUNT_KEY"])
        self.container_id = os.environ["CONTAINER"]
        self.database_id = os.environ["DATABASE"]
    

    # Create database
    def init_database(self, database_id=os.environ["DATABASE"]) -> DatabaseProxy:
        try:
            self.database_id = database_id
            return self.client.create_database_if_not_exists(database_id)
        except Exception as e:
            raise e


    # Create a container
    def init_container(self, database_id=os.environ["DATABASE"], container_id=os.environ["CONTAINER"]) -> ContainerProxy:
        try:
            db_client = self.init_database(database_id)
            self.container_id = container_id
            return db_client.create_container_if_not_exists(id=container_id, partition_key=PartitionKey(path='/topic'))
        except Exception as e:
            raise e
    

    # Upsert item to the container
    def upsert_item(self, item):
        try:
            container = self.init_container()
            return container.upsert_item(item)
        except exceptions.CosmosResourceNotFoundError as e:
            logging.exception(f'A collection with id "{self.container_id}" does not exist')
            raise e
        except Exception as e:
            raise e
    

    # Delete item to the container
    def delete_item(self, item):
        query = f'SELECT * FROM c WHERE c.id = "{item["id"]}"'
        try:
            result = []
            container = self.init_container()
            for item in container.query_items(query=query, enable_cross_partition_query=True):
                container.delete_item(item, partition_key=item["topic"])
                result.append(item)
            return result
        except exceptions.CosmosResourceNotFoundError as e:
            logging.exception(f'A collection with id "{self.container_id}" does not exist')
            raise e
        except Exception as e:
            raise e


    # Read item from container
    def read_item(self, item):
        query = f'SELECT * FROM c WHERE c.id = "{item["id"]}"'
        print('query:', query)
        try:
            container = self.init_container()
            result = list(container.query_items(query=query, enable_cross_partition_query=True, max_item_count=10))
            print(result)
            return result
        except exceptions.CosmosResourceNotFoundError as e:
            logging.exception(f'A collection with id "{self.container_id}" does not exist')
            raise e
        except Exception as e:
            raise e
    

    # Read all items from container
    def read_items(self):
        try:
            container = self.init_container()
            return list(container.read_all_items(enable_cross_partition_query=True, max_item_count=10))
        except exceptions.CosmosResourceNotFoundError as e:
            logging.exception(f'A collection with id "{self.container_id}" does not exist')
            raise e
        except Exception as e:
            raise e
