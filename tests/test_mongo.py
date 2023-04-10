import asyncio
from loguru import logger

from framework.mongoclient import mongo_src
from framework.mongoclient import mongo_dst


logger.info(f"mongo_src {mongo_src}")
logger.info(f"mongo_dst {mongo_dst}")


async def run():
    src_data = await mongo_src.get_last_id("equip", "zm5")
    print("src", src_data)
    dst_data = await mongo_src.get_last_id("equip", "zm5")
    print("src", dst_data)


asyncio.run(run())
