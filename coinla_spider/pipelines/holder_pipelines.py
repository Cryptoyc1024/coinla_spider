# -*- coding: utf-8 -*-

import logging

from pymongo.errors import OperationFailure
from twisted.internet.defer import inlineCallbacks

from coinla_spider.databases.getters import get_ccy_data
from .base_pipelines import ConnectionPipeline
from ..items import DevelopDataItem


class HolderPipeline(ConnectionPipeline):

    def __init__(self, crawler):
        super(HolderPipeline, self).__init__(crawler)
        self._total_cache = dict()
        self._share_count = dict()

    @inlineCallbacks
    def process_item(self, item, spider):
        if not 1 <= item['rank'] <= 50:
            logging.error('错误的Rank值：{}'.format(item))
            return item
        ccy_id = item['ccy_id']
        if not item.get('share', None):
            total = self._total_cache.get(ccy_id)
            if total is None:
                total = self._total_cache[ccy_id] = \
                    yield get_ccy_data(ccy_id, 'total')
            item['share'] = item['balance'] / total * 100 if total else 0
        self._share_count[ccy_id] = self._share_count.get(ccy_id, 0) + item['share']
        try:
            yield self._mongo_db[item.__collection__].update_one(
                {'ccy_id': ccy_id, 'rank': item['rank']}, {'$set': item}, upsert=True)
        except OperationFailure as e:
            logging.error(e)
        return item

    @inlineCallbacks
    def close_spider(self, spider):
        yield self.set_share()

    @inlineCallbacks
    def set_share(self):
        for ccy_id, share in self._share_count.items():
            if float(share) > 100:
                continue
            dev_item = DevelopDataItem(
                holder_topten_share=share,
                has_holder_list=1
            )
            yield self._mongo_db[dev_item.__collection__].update_many(
                {'ccy_id': ccy_id}, {'$set': dev_item}, timeout=20)
