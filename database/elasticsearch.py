# pylint: disable=import-error
# pylint: disable=no-name-in-module
from elasticsearch import Elasticsearch

from utils.configmanager import ConfigManager

# db_config = ConfigManager.get_config_value("database", "elasticsearch")
# es = Elasticsearch(db_config["hosts"])


def make_es(retries=30, db_config=ConfigManager.get_config_value("database", "elasticsearch")):
    while retries != 0:
        try:
            es = Elasticsearch(db_config["hosts"])
            init_es(es)
            print("Elasticsearch is online!")
            return es
        except:
            import time

            time.sleep(5)
            print(f"Elasticsearch is not ready... Retrying in 5s (retries: {retries})")
            retries -= 1


def init_es(es, indices=ConfigManager.get_config_value("search", "indices")):
    for index in indices:
        if not es.indices.exists(index):
            es.indices.create(index)
