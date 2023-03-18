"""
这是一个封装了motor的异步mongodb请求库。
"""

__version__ = "0.0.3"


from .base import AsMongo
from .base import AsMongoError
from .base import TypeMongoId

from .utils.collection_meta import MongoCollMetaSeqs
from .utils.usefull import MongoColl
