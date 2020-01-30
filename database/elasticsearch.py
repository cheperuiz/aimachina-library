# pylint: disable=import-error
# pylint: disable=no-name-in-module
from elasticsearch import Elasticsearch

from utils.configmanager import ConfigManager

db_config = ConfigManager.get_config_value("database", "elasticsearch")
es = Elasticsearch(db_config["hosts"])

