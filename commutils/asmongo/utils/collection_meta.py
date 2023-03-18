"""
在collection中创建一个 collection_meta 表用来记录需要自增id的表
并且在调用后每次返回一个新的自增id
"""
from pymongo import ReturnDocument

from ..base import AsMongo


class MongoCollMetaSeqs(AsMongo):
    collection_name = "collection_meta"

    async def get_next_sequence(self, coll_name: str) -> int:
        """，成功返回自增id，
        失败 raise AsMongoError，"""
        sequence_document = {"_id": coll_name}
        update_document = {"$inc": {"value": 1}}
        result = await self.connect(
            self.collection.find_one_and_update(
                sequence_document, update_document,
                upsert=True, return_document=ReturnDocument.AFTER)
        )
        return result["value"]

    async def set_next_sequence(self, coll_name: str, seq_value: int) -> int:
        """设置自增id的当前值，返回设置的值（即下次获取的值是当前值+1）"""
        filter_document = {"_id": coll_name}
        update_document = {"$set": {"value": seq_value}}
        result = await self.connect(
            self.collection.find_one_and_update(
                filter_document, update_document,
                upsert=True, return_document=ReturnDocument.AFTER)
        )
        return result["value"]
