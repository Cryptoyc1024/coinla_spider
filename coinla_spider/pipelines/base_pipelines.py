# -*- coding: utf-8 -*-

from scrapy import signals
from twisted.internet.defer import inlineCallbacks

from coinla_spider.databases import connections


class ConnectionPipeline(object):

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def __init__(self, crawler):
        crawler.signals.connect(self.spider_closed, signal=signals.spider_closed)
        self._cache = connections.cache
        self._mongo_db = connections.mongo_db
        self._sql_db = connections.sql_db
        self._redis_db = connections.redis_db
        self._stomp = connections.stomp

    @inlineCallbacks
    def spider_closed(self, spider, **kwargs):
        try:
            self._stomp.disconnect(timeout=0.2)
            yield self._stomp.disconnected
            yield self._redis_db.disconnect()
            self._sql_db.close_db()
            yield self._mongo_db.client.disconnect()
            yield self._cache.close()
        except Exception:
            pass
