# -*- coding: utf-8 -*

from scrapy.exceptions import DropItem
from twisted.internet.defer import inlineCallbacks

from coinla_spider.databases.connections import redis_internal
from .base_pipelines import ConnectionPipeline


class ExratePipeline(ConnectionPipeline):

    @inlineCallbacks
    def process_item(self, item, spider):
        short_name = item['ccy_short_name']
        if short_name is None:
            raise DropItem('缺少必要字段')
        yield self._cache.save('ExrateCNY', short_name, item['exrate'])
        db_key = 'Exrate:{}'.format(short_name)
        yield self._redis_db.set(db_key, item['exrate'])
        yield redis_internal.set('Exrate:{}'.format(short_name), item['exrate'])
        if short_name == 'USD':
            yield redis_internal.set('SpiderCache:ExrateCNY:USD', item['exrate'])
        return item
