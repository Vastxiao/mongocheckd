# -*- coding: utf-8 -*-

__version__ = "0.0.4"


import asyncio
from typing import final, Final, Literal
from typing import Awaitable
from bson.objectid import ObjectId
from munch import DefaultMunch
from loguru import logger
from motor.motor_asyncio import AsyncIOMotorClient
from motor.motor_asyncio import AsyncIOMotorDatabase, AsyncIOMotorCollection
from pymongo.errors import AutoReconnect, PyMongoError
from pymongo.errors import OperationFailure
from pymongo import ReadPreference


_read_preference = dict(PRIMARY=ReadPreference.PRIMARY,
                        PRIMARY_PREFERRED=ReadPreference.PRIMARY_PREFERRED,
                        SECONDARY=ReadPreference.SECONDARY,
                        SECONDARY_PREFERRED=ReadPreference.SECONDARY_PREFERRED)


TypeMongoId = ObjectId | str | int | float | bool


class AsMongoError(Exception):
    """用于AsMongo处理的的错误类

    msg: 错误信息
    err: Exception对象
    """
    def __init__(self, msg: str, *,
                 err: PyMongoError | OperationFailure | Exception = None) -> None:
        super(AsMongoError, self).__init__(msg)
        self.msg = msg
        self.exception = err

    def __repr__(self):
        return f"{self.__class__} {self.msg}"

    def __str__(self):
        return f"{self.__class__.__name__}: {self.msg}"


class AsMongo:
    """对motor封装的mongo类"""
    URI: str = "mongodb://127.0.0.1:27017/test"
    collection_name: str = ""
    reconnect_retry: int = 1
    reconnect_retry_delay: float = 0.5
    max_pool_size: int = 100
    min_pool_size: int = 0
    max_idle_ms: int = None
    max_connecting: int = 2
    timeout_ms: int = 15000  # 超时时间一般只要设置这个值
    socket_timeout_ms: int = 0
    connect_timeout_ms: int = 20000

    def __init__(self, uri: str = "", *,
                 collection_name: str = None,
                 read_preference: Literal[
                     "PRIMARY", "PRIMARY_PREFERRED", "SECONDARY", "SECONDARY_PREFERRED"] = None,
                 reconnect_retry: int = None,
                 reconnect_retry_delay: float = None,
                 max_pool_size: int = None,
                 min_pool_size: int = None,
                 max_idle_ms: int = None,
                 max_connecting: int = None,
                 timeout_ms: int = None,  # 超时时间一般只要设置这个值
                 socket_timeout_ms: int = None,
                 connect_timeout_ms: int = None,
                 ):
        """
        url                    mongo连接的uri地址，只支持url模式
        collection_name        集合名称
        read_preference        设置Mongo副本读写优化

        reconnect_retry        使用reconnect操作失败重连次数
        reconnect_retry_delay  reconnect操作失败重连等待(秒)

        max_pool_size      （连接池）允许连接mongo的最大并发连接数，默认100，0为不限制
        min_pool_size      （连接池）维护的最小并发连接数，默认0。
        max_idle_ms        （连接池）保持空闲连接的时间，默认0，不会超时销毁。
        max_connecting     （连接池）每个池可以同时建立的最大连接数。默认为 2。

        timeout_ms          mongodriver执行操作（包括重试尝试）的超时时间，0为不超时。

        这两个一般不用设置:
        socket_timeout_ms   在发送普通（非监控）数据库操作后等待响应的时间，默认为0不超时。
        connect_timeout_ms  控制新Socket连接服务器的超时时间，0为没有超时，默认为20000毫秒。
        """
        self.URI: Final[str] = uri or self.URI
        self.__client: AsyncIOMotorClient = None
        self.collection_name: Final[str] = collection_name or self.collection_name
        self.read_preference = _read_preference.get(read_preference) if read_preference else None

        self.reconnect_retry: int = reconnect_retry if \
            reconnect_retry is not None else self.reconnect_retry
        self.reconnect_retry_delay: float = reconnect_retry_delay \
            if reconnect_retry_delay is not None else self.reconnect_retry_delay

        self.max_pool_size: Final[int] = max_pool_size if max_pool_size is not None else self.max_pool_size
        self.min_pool_size: Final[int] = min_pool_size if min_pool_size is not None else self.min_pool_size
        self.max_idle_ms: Final[int] = max_idle_ms or self.max_idle_ms
        self.max_connecting: Final[int] = max_connecting if max_connecting is not None else self.max_connecting
        self.timeout_ms: Final[int] = timeout_ms if timeout_ms is not None else self.timeout_ms
        self.socket_timeout_ms: Final[int] = socket_timeout_ms or self.socket_timeout_ms
        self.connect_timeout_ms: Final[int] = connect_timeout_ms \
            if connect_timeout_ms is not None else self.connect_timeout_ms

        if not self.URI:
            raise AsMongoError(f"{self.__class__} 实例对象必须存在URI, eg: "
                               f"URI = 'mongodb://user:pass@127.0.0.1:27017/test?authSource=admin'")

    def __repr__(self):
        return f'<{self.__class__.__name__} {self.URI}>'

    @final
    def init_connect(self):
        """初始化 mongo 客户端"""
        if not self.__client:
            self.__client: AsyncIOMotorClient = AsyncIOMotorClient(
                self.URI, document_class=DefaultMunch, connect=True,
                maxPoolSize=self.max_pool_size, minPoolSize=self.min_pool_size,
                maxIdleTimeMS=self.max_idle_ms, maxConnecting=self.max_connecting,
                timeoutMS=self.timeout_ms, socketTimeoutMS=self.socket_timeout_ms,
                connectTimeoutMS=self.connect_timeout_ms,)

    @property
    def client(self) -> AsyncIOMotorClient:
        """获取 mongo uri 配置的 mongo 客户端连接对象"""
        self.init_connect()
        return self.__client

    @property
    def db(self) -> AsyncIOMotorDatabase:
        """获取 mongo uri 所配置的名称的数据库对象"""
        return self.client.get_default_database(read_preference=self.read_preference)

    @property
    def collection(self) -> AsyncIOMotorCollection:
        """获取集合对象"""
        if not isinstance(self.collection_name, str):
            raise AsMongoError(f"{self} collection_name 错误，为"
                               f"{self.collection_name if self.collection_name else '空'}。")
        return self.db[self.collection_name]

    @final
    async def reconnect(self, awt: Awaitable, retry: int = None, delay: float = None):
        """mongo连接的装饰器，处理mongodb操作数据自动重连机制。

        参数：
          retry 为 -1 表示一直重试
          delay 为重试间隔时间（秒）
        """
        i = retry = retry if retry is not None else self.reconnect_retry
        sleep_sec = delay if delay is not None else self.reconnect_retry_delay
        while i != 0:
            try:
                return await awt
            except AutoReconnect:
                if i > 0:
                    i = i - 1
                if i == 0:
                    raise AsMongoError(f"mongo断线重连{retry}次失败：{awt}")
                await asyncio.sleep(sleep_sec)
                logger.error(f"mongo失败尝试重连...")

    @final
    async def connect(self, awt: Awaitable):
        """mongo连接的装饰器，自动处理异常，全部抛出AsMongoError"""
        try:
            return await awt
        except OperationFailure as e:
            raise AsMongoError(
                f"pymongo.errors.{e.__class__.__name__} {e.details}", err=e)
        except PyMongoError as e:
            raise AsMongoError(
                f"pymongo.errors.{e.__class__.__name__} {e.args}", err=e)
        except AsMongoError as e:
            raise e
        except Exception as e:
            raise AsMongoError(f"{e}", err=e)

    @final
    async def autoconnect(self, awt: Awaitable, retry: int = None, delay: float = None):
        """mongo连接的装饰器，自动处理mongodb操作异常和自动重连机制。"""
        return await self.connect(self.reconnect(awt, retry, delay))
