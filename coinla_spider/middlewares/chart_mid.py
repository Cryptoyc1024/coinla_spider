# -*- coding: utf-8 -*-

import logging
import time

from scrapy.exceptions import IgnoreRequest
from twisted.internet.defer import inlineCallbacks
from txmongo.filter import sort

from .base_mid import ConnectionBaseMiddleware
from ..databases.getters import get_tocny_exrate
from ..items import ChartItem


class ChartDownloaderMiddleware(ConnectionBaseMiddleware):
    _usd_exrate = None

    @inlineCallbacks
    def process_request(self, request, spider):
        if not isinstance(request.meta.get('item'), ChartItem):
            return None

        meta = request.meta
        item = meta['item']
        coll = item.__collection__
        ccy_id = item.get('ccy_id', None)
        if spider.name == 'chart' and ccy_id is None:
            cache_key = '{}-{}'.format(item['ccy_short_name'], item['ccy_en_name'])
            ccy_id = yield self._cache.load('Currency', cache_key)
            if not ccy_id:
                logging.info('{} 币种ID缓存不存在'.format(item['ccy_short_name']))
                raise IgnoreRequest()
            item['ccy_id'] = int(ccy_id)
        step = meta.get('step', 0)

        # 处理所有区间的趋势图请求
        if not step:
            meta['original_time'] = yield self.get_last_data_time(coll, ccy_id)

        # 处理固定区间的趋势图请求
        else:
            # 链接已添加时间戳的不再处理
            if request.url[-10:-1].isdigit():
                return None
            original_time = meta.get('original_time', None)
            if original_time is None:
                if spider.name == 'chart':
                    original_time = yield self.get_last_data_time(coll, ccy_id)
                else:
                    doc = yield self._mongo_db[coll].find_one(
                        {}, {'timestamp': 1}, sort=sort([('timestamp', -1)]))
                    original_time = doc['timestamp'] if doc else 1367174800000
            end_time = meta.get('next_time', int(time.time()) * 1000)
            start_time = end_time - step
            url = '{}/{}/{}/'.format(meta['url_base'], start_time, end_time)
            new_meta = {
                'item': item,
                'original_time': original_time,
                'next_time': start_time,
                'step': step,
                'url_base': meta['url_base'],
            }
            return request.replace(url=url, meta=new_meta)

    @inlineCallbacks
    def get_last_data_time(self, coll, ccy_id):
        last_data_time = yield self._cache.load(coll, ccy_id)
        if last_data_time is not None:
            last_data_time = int(last_data_time)
        else:
            result = yield self._mongo_db[coll].aggregate([
                {'$match': {'ccy_id': ccy_id}},
                {'$group': {'_id': '$ccy_id', 'last_data_time': {'$max': '$timestamp'}}}
            ])
            if not result:
                return 0
            last_data_time = result[0]['last_data_time']
        return last_data_time

    @inlineCallbacks
    def process_response(self, request, response, spider):
        if request.callback == spider.parse_chart:
            request.meta['usd_exrate'] = self._usd_exrate or \
                                         (yield get_tocny_exrate('USD'))
        return response
