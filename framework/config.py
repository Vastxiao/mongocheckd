"""
这个是环境变量配置文件

Settings将会从环境变量读取变量，如shell中设置变量：
export APP_NAME="my name"

或者如果设置例如从.env文件中读取配置变量：
APP_NAME="Ch_App"

注意：
  在环境变量和.env文件都配置的情况下，环境变量优先级高。
  并且在实例化Settings类对象时，如果传递参数_env_file，则将优先加载参数设置的文件。
  如果_env_file=None时，则不加载文件
详细参考：https://docs.pydantic.dev/usage/settings/#dotenv-env-support
"""
from typing import Literal
from functools import lru_cache
from pydantic import BaseSettings, BaseConfig
from pydantic import MongoDsn

__all__ = ["get_settings", "Settings",
           # "STATIS_SETTINGS",
           # "StaticSettings"
           ]

#
# class StaticSettings:
#     def __init__(self):
#         raise ValueError("不允许实例化StaticSettings")
#
#     def __repr__(self):
#         return "<StaticSettings 全局静态配置项>"
#
#     ASGI_APPNAME: str = "framework.web:app"
#
#     fastapi_title = "Arco Design Pro Web Api"
#     fastapi_description = "Arco Design Pro Web 的后台 Api 接口。"
#     fastapi_contact = {
#         "name": "Vastxiao Github",
#         "url": "https://vastxiao.github.io/",
#         "email": "vastxiao@gmail.com",
#     }


class Settings(BaseSettings):
    # 这个配置将允许环境变量从 .env 配置文件读取
    # 注意，使用dotenv需要库支持： pip3 install python-dotenv
    # 如果env_file传入元祖或列表，则后面的文件优先级高。
    # 如： env_file = '.env', '.env.prod'
    class Config(BaseConfig):
        env_file = ".env", "etc/.env", "etc/config.env"
        env_file_encoding = 'utf-8'

        # 处理自定义字段配置内容
        @classmethod
        def parse_env_var(cls, field_name: str, raw_val: str):
            if field_name in ['check_dbs', 'check_collections']:
                return [x for x in raw_val.split(',')]
            return cls.json_loads(raw_val)

    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    mongo_src_uri: MongoDsn
    mongo_dst_uri: MongoDsn

    check_dbs: list[str] = []
    check_collections: list[str] = []

    task_concurrent: int = 2


@lru_cache()
def get_settings():
    # return Settings(_env_file="etc/config.env")
    return Settings()


# STATIS_SETTINGS = StaticSettings
