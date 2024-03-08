# -*- coding: utf-8 -*-

import logging
from functools import reduce

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from twisted.internet.defer import inlineCallbacks
from txredisapi import lazyConnectionPool

from coinla_spider.settings import *


class MySQLDB(object):

    def __init__(self, uri, pool_size=5, max_overflow=20,
                 autoflush=True, autocommit=True):
        self._engine = create_engine(uri, pool_size=pool_size, pool_recycle=3600,
                                     pool_timeout=10, max_overflow=max_overflow)
        _Session = sessionmaker(bind=self._engine, autoflush=autoflush,
                                autocommit=autocommit, expire_on_commit=False)
        self.session = _Session()
        # from coinla_spider.models import Base
        # Base.metadata.create_all(self._engine)

    def select(self, model, filters):
        return self.session.query(model).filter(*filters).first()

    def insert(self, model, field_map):
        try:
            obj = model()
            if field_map is not None:
                list(map(lambda x: setattr(
                    obj, x[0], x[1]), field_map.items()))
            self.session.add(obj)
            self.session.flush()
        except Exception as e:
            self.session.rollback()
            raise e
        return obj

    def update(self, model, obj_id, field_map):
        try:
            self.session.query(model).filter_by(
                id=obj_id).update(field_map)
        except Exception as e:
            self.session.rollback()
            raise e

    def commit(self):
        try:
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            raise e

    def close_session(self):
        self.session.close()

    def close_db(self):
        self._engine.dispose()




class RedisDB(object):

    def __init__(self, host, port, password, poolsize=20, reconnect=True):
        self.cli = lazyConnectionPool(
            host=host, port=port, password=password, poolsize=poolsize,
            reconnect=reconnect, connectTimeout=3, replyTimeout=3
        )

    @inlineCallbacks
    def set(self, key, value):
        try:
            yield self.cli.set(key, value)
        except Exception as e:
            logging.error(e)

    @inlineCallbacks
    def hmset(self, key, mapping):
        try:
            yield self.cli.hmset(key, mapping)
        except Exception as e:
            logging.error(e)

    @inlineCallbacks
    def close(self):
        try:
            yield self.cli.disconnect()
        except Exception as e:
            logging.error(e)


class DummyClass(object):
    """ 用于连接失败后，忽略所有方法的报错 """

    def __getattribute__(self, item):
        def dummy_func(*args, **kwargs):
            pass

        return dummy_func
