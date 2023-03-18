from typing import AsyncIterable
from munch import DefaultMunch
from loguru import logger
from commutils.asmongo import AsMongo, AsMongoError, TypeMongoId
from .config import get_settings

settings = get_settings()


class MongoOp(AsMongo):

    async def get_db_names(self) -> list[str]:
        """获取所有db名称 （排除 config admin local）"""
        dbs: list[str] = await self.connect(self.client.list_database_names())
        try:
            dbs.remove("admin")
            dbs.remove("config")
            dbs.remove("local")
        except ValueError:
            pass
        return dbs

    async def get_collection_names(self, db_name: str) -> list[str]:
        db = self.client.get_database(db_name) if db_name else self.db
        return await self.connect(db.list_collection_names())

    async def get_list_by_id(self, collection: str, db_name: str = None, *,
                             id_offset: TypeMongoId = "",
                             skip: int = 0, limit: int = 50) -> AsyncIterable[DefaultMunch]:
        """获取 _id 列表"""
        if skip > 10000 or limit > 10000:
            raise AsMongoError("skip 和 limit 不能大于 10000")
        db = self.client.get_database(db_name) if db_name else self.db
        coll = db.get_collection(collection)
        if id_offset:
            # as_cursor = db.user.find({}, {"_id": 1}, max_time_ms=5000).sort({"_id": 1}).skip(skip).limit(limit)
            as_cursor = coll.find({"_id": {"$gt": id_offset}}, {"_id": 1}).sort(
                [("_id", 1)]).skip(skip).limit(limit).max_time_ms(5000)
        else:
            as_cursor = coll.find({}, {"_id": 1}).sort(
                [("_id", 1)]).skip(skip).limit(limit).max_time_ms(5000)
        async for data in as_cursor:
            yield DefaultMunch(**data)
            # 返回数据： {'_id': "2894359138941981"}

    async def get_last_id(self, collection: str, db_name: str = None) -> TypeMongoId:
        """获取最后的 _id"""
        db = self.client.get_database(db_name) if db_name else self.db
        coll = db.get_collection(collection)

        # as_cursor = db.user.find({}, {"_id": 1}).sort([("_id", -1)]).skip(0).limit(1).max_time_ms(5000)
        # async for data in as_cursor:
        #     d = DefaultMunch(**data)
        #     return d.get("_id")

        data = await self.connect(
            coll.find_one({}, {"_id": 1}, sort=[("_id", -1)], max_time_ms=5000)
        )
        return data.get("_id")

    async def count_id(self, collection: str, db_name: str = None) -> int:
        """获取数据数量"""
        db = self.client.get_database(db_name) if db_name else self.db
        coll = db.get_collection(collection)
        count = await self.connect(
            # coll.count_documents({}, hint="_id")
            coll.estimated_document_count(maxTimeMS=9000)  # 这个才是db.collection.count() 命令
        )
        logger.debug(f"Mongo {db_name}.{collection} count {count}")
        return count

    async def find_id_info(self, doc_id: TypeMongoId, collection: str, db_name: str = None) -> dict:
        """更新UP主信息"""
        db = self.client.get_database(db_name) if db_name else self.db
        coll = db.get_collection(collection)
        return await coll.find_one({"_id": doc_id})


mongo_src = MongoOp(settings.mongo_src_uri)
mongo_dst = MongoOp(settings.mongo_dst_uri)

__all__ = [
    "mongo_src",
    "mongo_dst"
]
