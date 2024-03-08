# -*- coding: utf-8 -*-

from coinla_spider.databases.connections import cache, mongo_db


class ConnectionBaseMiddleware(object):

    def __init__(self):
        self._cache = cache
        self._mongo_db = mongo_db
