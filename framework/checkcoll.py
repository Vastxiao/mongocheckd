import asyncio
from typing import Final
from pathlib import Path
from bson.objectid import ObjectId
import aiofiles
from loguru import logger
from aiofiles.threadpool.text import AsyncTextIOWrapper
from deepdiff import DeepDiff

from commutils.asmongo import TypeMongoId
from .mongoclient import mongo_src
from .mongoclient import mongo_dst


__all__ = ["DataCheck"]


# skip_type_map = {
#     "ObjectId": ObjectId,
#     "str": str,
#     "int": int,
#     "float": float,
#     "bool": bool,
# }


def get_skip_id_obj(skip_id: str, skip_id_type: str) -> TypeMongoId:
    if skip_id_type == "ObjectId":
        return ObjectId(skip_id)
    elif skip_id_type == "str":
        return skip_id
    elif skip_id_type == "int":
        return int(skip_id)
    elif skip_id_type == "float":
        return float(skip_id)
    elif skip_id_type == "bool":
        return bool(skip_id)
    else:
        raise ValueError("skip_id type error!!!")


def get_skip_id_meta(skip_id_obj: TypeMongoId) -> tuple[str, str]:
    if isinstance(skip_id_obj, ObjectId):
        return f"{skip_id_obj}", "ObjectId"
    elif isinstance(skip_id_obj, str):
        return skip_id_obj, "str"
    elif isinstance(skip_id_obj, int):
        return f"{skip_id_obj}", "int"
    elif isinstance(skip_id_obj, float):
        return f"{skip_id_obj}", "float"
    elif isinstance(skip_id_obj, bool):
        return f"{skip_id_obj}", "bool"
    else:
        raise ValueError("skip_id_obj type error!!!")


class DataCheck:
    concurrent: int = 50

    check_success_fobj: AsyncTextIOWrapper = None
    check_failure_fobj: AsyncTextIOWrapper = None

    result_path: Path = Path("result")

    def __init__(self, *, db_name: str, collection: str,
                 concurrent: int = None
                 ):
        # raise RuntimeError(f"{self.__class__} 不允许实力化。")
        self.db_name: Final[str] = db_name
        self.collection: Final[str] = collection

        self.concurrent = concurrent or self.concurrent

        # self.skip_id: str = ""
        # self.skip_id_type: str = ""
        self.skip_id_obj: TypeMongoId = None
        self.result_path.mkdir(exist_ok=True)
        pre_path = self.result_path.as_posix()
        self.skip_id_file: Final[str] = f'{pre_path}/{db_name}.{collection}.skip.txt'
        self.check_success_file: Final[str] = f'{pre_path}/{db_name}.{collection}.check.success.txt'
        self.check_failure_file: Final[str] = f'{pre_path}/{db_name}.{collection}.check.failure.txt'

    def __repr__(self):
        return f"<{self.__class__.__name__} {self.db_name}.{self.collection}>"

    async def read_skip_id_from_file(self):
        if self.skip_id_obj:
            logger.warning(f"已存在，不应该再从 {self.skip_id_file} 文件获取。")
            return
        skip_id: str = ""
        skip_id_type: str = ""
        try:
            async with aiofiles.open(self.skip_id_file, mode='r') as f:
                contents = await f.read()
            skip_detail = contents.strip().replace('\n', '').split('\t')
            if len(skip_detail) != 2:
                raise ValueError(f"{self.skip_id_file} 文件内容格式错误")
            skip_id = skip_detail[0]
            skip_id_type = skip_detail[1]
        except FileNotFoundError:
            skip_id = ""
            skip_id_type = ""
        except ValueError:
            logger.warning(f"{self.skip_id_file} 文件内容错误。")
            skip_id = ""
            skip_id_type = ""
        finally:
            if skip_id and skip_id_type:
                self.skip_id_obj = get_skip_id_obj(skip_id, skip_id_type)
            logger.info(f"mongo数据从 {self.skip_id_obj} 开始处理。")

    async def write_skip_id_to_file(self):
        if not self.skip_id_obj:
            logger.error(f"{self} skip_id 内容错误。")
            return
        skip_id, skip_id_type = get_skip_id_meta(self.skip_id_obj)
        async with aiofiles.open(self.skip_id_file, mode='w') as f:
            await f.write(f"{skip_id}\t{skip_id_type}")

    async def print_check_id_data(self, data_id: TypeMongoId):
        src_data, dst_data = await asyncio.gather(
            mongo_src.find_id_info(data_id, self.collection, self.db_name),
            mongo_dst.find_id_info(data_id, self.collection, self.db_name)
        )
        if not src_data or not dst_data:
            print("no data mongo two.")
            return
        print(f"check {data_id}:", await asyncio.to_thread(DeepDiff, src_data, dst_data))

    async def check_id_data(self, data_id: TypeMongoId):
        src_data, dst_data = await asyncio.gather(
            mongo_src.find_id_info(data_id, self.collection, self.db_name),
            mongo_dst.find_id_info(data_id, self.collection, self.db_name)
        )
        data_id_str, data_id_type = get_skip_id_meta(data_id)
        if src_data == dst_data:
            await self.check_success_fobj.write(f"{data_id_str} {data_id_type}\n")
        else:
            result = await asyncio.to_thread(DeepDiff, src_data, dst_data)
            await self.check_failure_fobj.write(f"{data_id_str} {data_id_type} {result}\n")

    async def flush_fobj_and_close(self):
        await self.check_success_fobj.flush()
        await self.check_failure_fobj.flush()
        await self.check_success_fobj.close()
        await self.check_failure_fobj.close()

    async def init_check_files(self):
        """初始化对比需要读写的文件"""
        # if cls.user_count > 0:
        #     logger.error("cls.user_count error.")
        #     return

        await self.read_skip_id_from_file()

        if not self.skip_id_obj:
            # 清空结果文件
            async with aiofiles.open(self.check_failure_file, mode='w') as f:
                await f.write("")
            async with aiofiles.open(self.check_success_file, mode='w') as f:
                await f.write("")

        # async with aiofiles.open(cls.check_success_file, mode='a') as csf:
        #     cls.check_success_fobj = csf
        # async with aiofiles.open(cls.check_failure_file, mode='a') as csf:
        #     cls.check_failure_fobj = csf
        self.check_success_fobj = await aiofiles.open(self.check_success_file, mode='a')
        self.check_failure_fobj = await aiofiles.open(self.check_failure_file, mode='a')

    async def start_just_test_1(self, data_id: TypeMongoId = 1):
        """只是测试用的"""
        await self.check_id_data(data_id)

    async def start(self):
        logger.info(f"启动检测 {self.db_name}.{self.collection} ...")
        await self.init_check_files()

        max_id_obj = await mongo_src.get_last_id(self.collection, self.db_name)
        # assert isinstance(max_user_id, str), f"zmwz_src.get_last_id(): {max_user_id} error."
        while not self.skip_id_obj \
                or self.skip_id_obj < max_id_obj:
            logger.info(f"检查 mongo_src 从 _id {self.skip_id_obj} 开始的 {self.concurrent} 条数据...")
            tasks = []
            last_coll_id = [None]
            async for data in mongo_src.get_list_by_id(
                    id_offset=self.skip_id_obj, limit=self.concurrent,
                    collection=self.collection, db_name=self.db_name):
                try:
                    last_coll_id[0] = data.get("_id")
                except ValueError:
                    logger.error(f"mongo_src {self.db_name}.{self.collection} "
                                 f"_id（{last_coll_id[0]}） {data} 错误。")
                    return
                assert last_coll_id[0], f"mongo_src user _id（{last_coll_id}） 错误。"
                tasks.append(self.check_id_data(last_coll_id[0]))
            await asyncio.gather(*tasks)
            await self.check_success_fobj.flush()
            await self.check_failure_fobj.flush()
            self.skip_id_obj = last_coll_id[0]
            await self.write_skip_id_to_file()
