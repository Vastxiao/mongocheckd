from loguru import logger

from framework.mongoclient import mongo_src
from framework.mongoclient import mongo_dst


logger.info(f"mongo_src {mongo_src}")
logger.info(f"mongo_dst {mongo_dst}")
