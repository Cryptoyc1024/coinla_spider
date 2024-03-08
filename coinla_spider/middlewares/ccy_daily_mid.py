# -*- coding: utf-8 -*-

import logging

from scrapy.exceptions import IgnoreRequest
from twisted.internet.defer import inlineCallbacks

from .base_mid import ConnectionBaseMiddleware


class CurrencyDailyDataDownloaderMiddleware(ConnectionBaseMiddleware):

    @inlineCallbacks
    def process_request(self, request, spider):
        item = request.meta.get('item', None)
        if item is None:
            return None
        short_name = item['ccy_short_name']
        cache_key = '{}-{}*'.format(short_name, item['ccy_en_name'][:8])
        ccy_id = yield self._cache.load('Currency', cache_key)
        if not ccy_id:
            logging.info('{} 币种缓存不存在'.format(short_name))
            raise IgnoreRequest()
        item['ccy_id'] = ccy_id
        chd = yield self._mongo_db[item.__collection__].find_one(
            {'ccy_id': ccy_id}, {'last_data_time': 1})
        if chd is not None:
            item['doc_id'] = chd['_id']
            item['last_data_time'] = chd['last_data_time']
