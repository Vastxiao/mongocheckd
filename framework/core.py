import asyncio

from aslooper import looper

# windows 系统不支持 uvloop，兼容 windows
try:
    import uvloop
except ImportError:
    class __Uvloop:
        @classmethod
        def install(cls):
            pass
    uvloop = __Uvloop

from .logs import logger
from .config import get_settings
from .mongoclient import mongo_src
from .checkcoll import DataCheck

settings = get_settings()

# def __sig_cancel_run():
#     loop = asyncio.get_running_loop()
#     loop.run_until_complete(CheckMain.flush_fobj_and_close())


async def get_all_check_coll_name() -> list[str]:
    all_collection_string: list[str] = []
    filter_dbs = settings.check_dbs
    filter_collections = settings.check_collections

    if filter_collections:
        all_collection_string.extend(filter_collections)

    if filter_dbs:
        for db in filter_dbs:
            coll_s = await mongo_src.get_collection_names(db)
            all_collection_string.extend([f"{db}.{c}" for c in coll_s])
    elif not filter_collections:
        all_dbs = await mongo_src.get_db_names()
        for db in all_dbs:
            coll_s = await mongo_src.get_collection_names(db)
            all_collection_string.extend([f"{db}.{c}" for c in coll_s])
    all_collection_string = list(set(all_collection_string))
    return all_collection_string


def get_coll_meta(db_coll_string: str) -> tuple[str, str]:
    """返回:db_name coll_name"""
    assert db_coll_string, "get_coll_meta 不能是空值，请检查错误"
    data_l = db_coll_string.split('.')
    assert len(data_l) == 2, "check_collections 配置错误"
    return data_l[0], data_l[1]


# @looper(__sig_cancel_run)
@looper()
async def main():
    logger.info("start...")

    sem = asyncio.Semaphore(settings.task_concurrent)
    all_coll_s = await get_all_check_coll_name()

    logger.info(f'检查目标 {all_coll_s}')
    async with sem:
        async with asyncio.TaskGroup() as tg:
            for coll_string in all_coll_s:
                logger.debug(f"创建 {coll_string} 对比任务")
                db, coll = get_coll_meta(coll_string)
                tg.create_task(DataCheck(
                    db_name=db, collection=coll).start(), name=f"DataCheck-{db}.{coll}")


def run():
    uvloop.install()
    # Python 3.7 required
    asyncio.run(main())
