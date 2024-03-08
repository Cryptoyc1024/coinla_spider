# -*- coding: utf-8 -*-

import os
import sys

current_url = os.path.dirname(__file__)
parent_url = os.path.abspath(os.path.join(current_url, os.pardir))
sys.path.append(parent_url)

from redis import StrictRedis
from pymongo import MongoClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from coinla_spider.settings import *

__all__ = ['cache_cli', 'mongo_db', 'sql_session']

cache_cli = StrictRedis(REDIS_HOST, REDIS_PORT, password=REDIS_PASSWORD,
                        max_connections=20, decode_responses=True,
                        socket_timeout=1, socket_connect_timeout=1)

mongo_cli = MongoClient(
    host=MONGO_URI,
    connect=False,
    maxPoolSize=10,
)
mongo_db = mongo_cli.get_database()

engine = create_engine(MYSQL_URI, pool_size=5, pool_recycle=3600,
                       pool_timeout=10, max_overflow=20)
sql_session = sessionmaker(bind=engine, autoflush=True,
                           autocommit=False)
