from bson.objectid import ObjectId
from typing import AsyncIterable, Literal

from munch import DefaultMunch
from pymongo.results import InsertOneResult
from pymongo import ReturnDocument
from pymongo import ASCENDING, DESCENDING
from pymongo.results import DeleteResult

from ..base import AsMongo, AsMongoError, TypeMongoId


_direction = {
    1: ASCENDING,
    -1: DESCENDING,
    "ASCENDING": ASCENDING,
    "DESCENDING": DESCENDING,
}


class MongoColl(AsMongo):
    async def is_exist(self) -> bool:
        """判断collection是否存在"""
        collection_names = await self.autoconnect(
            self.db.list_collection_names()
        )
        return self.collection_name in collection_names

    async def create_index(self, key: str,
                           direction: Literal[1, -1, "ASCENDING", "DESCENDING"] = 1,
                           unique: bool = False) -> str:
        """创建表索引，
        返回创建的索引名称。"""
        return await self.autoconnect(
            self.collection.create_index(
                [(key, _direction[direction])], unique=unique, background=True)
        )

    async def add_one(self, doc: dict) -> TypeMongoId:
        result: InsertOneResult = await self.autoconnect(
            self.collection.insert_one(doc)
        )
        return result.inserted_id

    async def get_one(self, filter_doc: dict, projection=None, *args, **kwargs) -> dict:
        """查询数据，
        成功返回文档数据,
        失败raise AsMongoError
        """
        result = await self.autoconnect(
            self.collection.find_one(filter_doc, projection, *args, **kwargs)
        )
        if result:
            return result
        else:
            raise AsMongoError(f"mongodb 未获取到 {filter_doc} ")

    async def delete_one(self, delete_doc_filter: dict) -> DeleteResult:
        """删除数据，处理成功返回结果"""
        return await self.autoconnect(
            self.collection.delete_one(delete_doc_filter)
        )

    async def update_one_by_id(self, doc_id: str, new_data: dict,
                               upsert: bool = False) -> dict:
        """更新UP主信息"""
        return await self.autoconnect(
            self.collection.find_one_and_update(
                {'_id': f'{doc_id}'},
                {'$set': new_data},
                upsert=upsert,
                return_document=ReturnDocument.AFTER)
        )

    async def update_set(self, filter_doc: dict, update_doc: dict,
                         upsert=False, return_after=True) -> dict:
        """更新数据"""
        return_doc = ReturnDocument.AFTER if return_after else None
        return await self.autoconnect(
            self.collection.find_one_and_update(
                filter_doc, {'$set': update_doc},
                upsert=upsert,
                return_document=return_doc)
        )

    async def get_list(self, skip: int = 0, limit: int = 50,
                       filter_doc: dict = None, filter_key: dict = None,
                       id_offset: TypeMongoId = None,
                       ) -> AsyncIterable[DefaultMunch]:
        """获取列表"""
        filter_doc = filter_doc or {}
        filter_key = filter_key or {}
        if id_offset:
            if filter_doc:
                raise ValueError("filter_doc 和 id_offset 参数不能同时使用")
            if skip > 10000 or limit > 10000:
                raise ValueError("skip 和 limit 不能大于 10000")
            filter_doc.update({"id": {"$gt": id_offset}})
        # as_cursor = self.collection.find({}, {"_id": 0, "uid": 1}).sort({"uid": 1}).skip(skip).limit(limit)
        as_cursor = self.collection.find(
            filter_doc, filter_key).sort([("_id", 1)]).skip(skip).limit(limit).max_time_ms(5000)
        async for data in as_cursor:
            yield DefaultMunch(**data)

    async def count(self) -> int:
        """获取数据数量"""
        # estimated_document_count 这个才是db.collection.count() 命令
        count = await self.autoconnect(
            self.collection.estimated_document_count(maxTimeMS=9000)
        )
        return count
