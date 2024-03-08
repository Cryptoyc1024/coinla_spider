# -*- coding: utf-8 -*-

import json

from twisted.internet.defer import inlineCallbacks

from coinla_spider.databases.getters import \
    get_ccy_id, get_common_ege_id, get_legal_ids, get_tocny_exrate
from .base_pipelines import ConnectionPipeline
from ..items import QuotationItem, CurrencyItem


class QuotationPipeline(ConnectionPipeline):

    def __init__(self, crawler):
        super(QuotationPipeline, self).__init__(crawler)
        self.queue = crawler.settings['MQ_QUEUE']

    @inlineCallbacks
    def process_item(self, item, spider):
        if not isinstance(item, QuotationItem):
            return item

        # 构建需要发送到MQ的JSON
        cqn_data = {
            'exchangeId': item['ege_id'],
            'ccyId': item['pair_left_id'],
            'currency': item['pair_left'],
            'base': item['pair_right'],
            'last': item.get('price_cny', None),
            'lastUsd': item.get('price_usd', None),
            'lastOrigin': item['price_origin'],
            'turnover': item['turnover_cny'],
            'turnoverUsd': item.get('turnover_usd', None) or item['turnover_cny'] / self.usd_exrate,
            'time': item['update_time'].strftime('%Y-%m-%d %H:%M:%S')
        }

        # ----------------------------  全网交易对  ----------------------------
        if item['ege_id'] < 0:
            pair_left = item['pair_left']
            if pair_left == 'USDT':
                cqn_data['lastUsd'] = item['price_usd'] = item['price_cny'] / self.usd_exrate

            # 人民币价格更新到Redis作为汇率
            if item['pair_left_id'] not in self.legal_ids:
                yield self._cache.save('ExrateCNY', pair_left, item['price_cny'])

            cqn_data.update(
                exchangeId=self.common_ege_id,
                riseFall=item['change'] if 'change' in item and -1000 < item['change'] < 1000 else None,
            )
            data = {
                'price_cny': item['price_origin'],
                'turnover_cny': item['turnover_cny'],
                'update_time': cqn_data['time']
            }
            yield self._cache.save('CurrencyData', item['pair_left_id'], data,
                                   command='hmset', expire=3600 * 24 * 15)
            for field in ['circulate_total', 'circulate_value', 'total']:
                yield self._cache.save('CurrencyData', item['pair_left_id'], field, 0, command='hsetnx')

        # ----------------------------  交易所交易对  ----------------------------
        else:
            # 交易对的各项数据更新到MQ
            cqn_data.update(
                volume=item['volume'],
                ratio=item['ratio']
            )
            # USDT相关的交易对需要与USD比例换算
            if item['pair_right'] == 'USDT' and 'feixiaohao' in item.get('url', ''):
                cqn_data['last'] = item['price_cny'] / self.price_ratio
                cqn_data['lastUsd'] = item['price_usd'] / self.price_ratio

        # 行情数据发送到MQ，并保存缓存
        yield self._stomp.send(self.queue, json.dumps(cqn_data).encode())
        cqn_key = '{}-{}-{}'.format(
            item['ege_id'], item['pair_left'], item.get('pair_left_en', item['pair_right']))
        cache = {
            'price_origin': item['price_origin'],
            'turnover_cny': item['turnover_cny'],
            'volume': item.get('volume', 0),
            'update_time': cqn_data['time']
        }
        yield self._cache.save('NewestQuotation', cqn_key, cache,
                               command='hmset', expire=3600 * 24 * 8)
        return item

    @inlineCallbacks
    def open_spider(self, spider):
        yield self._stomp.connect()
        self.usd_exrate = yield get_tocny_exrate('USD')
        if spider.name == 'exchange':
            yield self.init_legal_ccy(spider.settings.getdict('LEGAL_CURRENCY'))
            self.usdt_exrate = yield get_tocny_exrate('USDT')
            self.price_ratio = self.usdt_exrate / self.usd_exrate
        else:
            self.common_ege_id = yield get_common_ege_id()
            self.legal_ids = yield get_legal_ids()
            spider.crawler.stats.set_value('legal_ids', self.legal_ids)

    @inlineCallbacks
    def init_legal_ccy(self, legal_ccy):
        """ 初始化法币 """
        for short_name, name in legal_ccy.items():
            cache_key = '{}-{}'.format(short_name, name[0])
            ccy_id = yield self._cache.load('Currency', cache_key)
            if ccy_id is None:
                item = CurrencyItem(
                    currencyName=name[1],
                    english=name[0],
                    shortName=short_name,
                    recordStatus=-1
                )
                field_map = {
                    'currency_name': name[1],
                    'english': name[0],
                    'short_name': short_name,
                    'record_status': -1
                }
                yield get_ccy_id(item, field_map)
