# -*- coding: utf-8 -*-

import json
from datetime import datetime

from scrapy.exceptions import DropItem
from twisted.internet.defer import inlineCallbacks

from coinla_spider.databases.getters import get_otc_ege_id, get_tocny_exrate
from .base_pipelines import ConnectionPipeline
from ..formatters import Price
from ..items import OTCPriceItem, QuotationItem


class OTCPricePipeline(ConnectionPipeline):

    def __init__(self, crawler):
        super(OTCPricePipeline, self).__init__(crawler)
        self.price_stats = dict()

    def process_item(self, item, spider):
        if not isinstance(item, OTCPriceItem):
            return item
        short_name = item['ccy_short_name']
        price = item['otc_price']
        if short_name is None or not price:
            raise DropItem('缺少必要字段')
        if short_name not in self.price_stats:
            self.price_stats[short_name] = price
        else:
            self.price_stats[short_name] = (self.price_stats[short_name] + price) / 2
        return item

    @inlineCallbacks
    def open_spider(self, spider):
        self.otc_ege_id = yield get_otc_ege_id()

    @inlineCallbacks
    def close_spider(self, spider):
        queue = spider.settings['MQ_QUEUE']
        yield self._stomp.connect()
        yield self.set_prices(queue)

    @inlineCallbacks
    def set_prices(self, queue):
        for short_name, price in self.price_stats.items():
            usd_exrate = yield get_tocny_exrate('USD')
            price_format = Price().format
            price_usd = price_format(float(price) / usd_exrate)
            price = price_format(price)
            yield self._mongo_db[QuotationItem.__collection__].update_many(
                {'currencyShortName': short_name, 'type': 0},
                {'$set': {'openNationalLowPrice': price}}, timeout=20)
            yield self._mongo_db[QuotationItem.__collection__].update_many(
                {'currencyShortName': short_name, 'type': 1},
                {'$set': {'openNationalLowPrice': price_usd}}, timeout=20)
            # 将OTC价格作为行情发送MQ
            ccy_id = yield self._cache.load('Currency', '{}-*'.format(short_name))
            cqn_data = {
                'exchangeId': self.otc_ege_id,
                'ccyId': ccy_id,
                'currency': short_name,
                'base': 'CNY',
                'last': price,
                'lastUsd': price_usd,
                'lastOrigin': price,
                'turnover': 0,
                'turnoverUsd': 0,
                'volume': 0,
                'ratio': 0,
                'riseFall': None,
                'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            yield self._stomp.send(queue, json.dumps(cqn_data).encode())
