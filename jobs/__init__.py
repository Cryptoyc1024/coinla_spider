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
from logging.handlers import RotatingFileHandler
from functools import wraps
from raven import Client
import logging
import time

# __all__ = ['cache_cli', 'mongo_db', 'sql_session', 'jobs_logger', 'logging_decorator']
__all__ = ['cache_cli', 'mongo_db', 'sql_session', 'jobs_logger']

cache_cli = StrictRedis(CACHE_HOST, CACHE_PORT, password=CACHE_PASSWORD,
                        max_connections=50, decode_responses=True,
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


def get_logger(name, module='%(module)s'):
    logger = logging.getLogger(name)
    if not logger.hasHandlers():
        logger.setLevel(logging.DEBUG)
        pattern = '%(asctime)s [{}-%(process)d] %(message)s'.format(module)
        fmt = logging.Formatter(pattern, '%Y-%m-%d %H:%M:%S')
        sh = logging.StreamHandler()
        sh.setFormatter(fmt)
        sh.setLevel(logging.DEBUG)
        logger.addHandler(sh)
        fh = RotatingFileHandler(os.path.join(parent_url, 'errors.log'),
                                 maxBytes=64 * 1024 * 1024, backupCount=1)
        fh.setFormatter(fmt)
        fh.setLevel(logging.ERROR)
        logger.addHandler(fh)
    return logger


jobs_logger = get_logger('jobs')


def logging_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if PROJECT_ENV == 'dev' or PROJECT_ENV == 'test':
            return func(*args, **kwargs)
        start = time.time()
        job_logger = get_logger(
            func.__name__,
            func.__name__
        )
        sentry = Client(dsn=SENTRY_DSN,
                        environment=PROJECT_ENV,
                        site=func.__name__)
        try:
            func(*args, **kwargs)
            run_time = round(time.time() - start, 2)
            job_logger.debug('Run {} sec'.format(run_time))
        except Exception as e:
            sentry.captureException(e)
            job_logger.error(e)
            raise e

    return wrapper
