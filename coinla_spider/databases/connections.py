# -*- coding: utf-8 -*-

from stompest.sync import Stomp
from stompest.config import StompConfig
from txmongo.connection import ConnectionPool

from .dbops import *
from ..settings import MYSQL_URI

cache = CacheOperator()

redis_db = RedisDB(
    host=REDIS_DB_HOST,
    port=REDIS_DB_PORT,
    password=REDIS_DB_PASSWORD,
)

redis_internal = RedisDB(
    host=REDIS_INTERNAL_HOST,
    port=REDIS_INTERNAL_PORT,
    password=REDIS_INTERNAL_PASSWORD,
    reconnect=False
)

sql_db = MySQLDB(
    MYSQL_URI,
    pool_size=5,
    max_overflow=20,
    autocommit=True
)

_mongo_cli = ConnectionPool(
    uri=MONGO_URI,
    pool_size=50,
    retry_delay=5,
    w=1, wtimeout=20
)
mongo_db = _mongo_cli.get_default_database()
mongo_db.client = _mongo_cli

stomp = Stomp(StompConfig(MQ_URI))
