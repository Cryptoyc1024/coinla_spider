# -*- coding: utf-8 -*-

import logging
import random

from twisted.internet.defer import inlineCallbacks

from .base_mid import ConnectionBaseMiddleware
from ..databases.getters import get_tocny_exrate
from ..formatters import Price
from ..items import QuotationItem


class QuotationSpiderMiddleware(ConnectionBaseMiddleware):

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.stats)

    def __init__(self, stats):
        super(QuotationSpiderMiddleware, self).__init__()
        stats.set_value('cqn_keys', set())
        self.stats = stats
        self.price_fmt = Price()

    @inlineCallbacks
    def process_spider_output(self, response, result, spider):
        items = list()
        turnover_total = 0
        ege_id = 0

        for item in result:
            if not isinstance(item, QuotationItem):
                items.append(item)
                continue
            if item.get('ege_id') is None:
                logging.error('{} 行情缺少交易所ID'.format(item))
                continue
            if item.get('price_origin', 'price_usd') == 0:
                logging.info('{} 缺少价格'.format(item))
                continue

            # 过滤相同交易所和交易对的行情
            cqn_key = '{}-{}-{}'.format(
                item['ege_id'], item['pair_left'], item.get('pair_left_en', item['pair_right']))
            if self.check_repeat(cqn_key) is True:
                logging.info('{} 被去重'.format(cqn_key))
                continue

            # 价格和成交额没有变化的不进行更新
            cqn_cache = (yield self._cache.load('NewestQuotation', cqn_key, command='hgetall'))
            if self.compare_cqn(cqn_cache, item) is True:
                logging.info('{} 价格和成交额无变化被过滤'.format(cqn_key))
                continue

            # 读取交易对ID
            if 'pair_left_id' not in item:
                pair_left_id = yield self.get_pair_ccy_id(
                    item['pair_left'], item.get('pair_left_en', None), item.get('url', None)
                )
                if not pair_left_id or pair_left_id <= 0:
                    logging.info('{} 交易对左侧无ID缓存'.format(cqn_key))
                    continue
                item['pair_left_id'] = pair_left_id

            # 全网行情过滤法币
            if item['ege_id'] < 0 and item['pair_left_id'] in self.stats.get_value('legal_ids', list()):
                logging.info('{} 法币被过滤'.format(cqn_key))
                continue

            # 只有原始价格的需要进行换算
            if 'price_cny' not in item:
                usd_exrate = yield get_tocny_exrate('USD')
                if 'price_usd' in item:
                    item.update(
                        price_cny=item['price_usd'] * usd_exrate,
                        turnover_cny=item['turnover_usd'] * usd_exrate
                    )
                    if self.compare_cqn(cqn_cache, item) is True:
                        logging.info('{} 价格和成交额无变化被过滤'.format(cqn_key))
                        continue
                else:
                    exrate_to_cny = yield get_tocny_exrate(item['pair_right'])
                    if exrate_to_cny is None:
                        logging.info('{} 交易对右侧换算时缺少价格'.format(cqn_key))
                        continue
                    exrate_to_usd = exrate_to_cny / usd_exrate
                    item.update(
                        price_cny=item['price_origin'] * exrate_to_cny,
                        price_usd=item['price_origin'] * exrate_to_usd,
                    )
                    item.update(
                        turnover_cny=item['volume'] * item['price_cny'],
                        turnover_usd=item['volume'] * item['price_usd'],
                    )
                # 交易所行情需要合计成交额
                if spider.name == 'exchange':
                    turnover_total += item['turnover_cny']
                    ege_id = item['ege_id']

            # 对爬取的完整价格制造偏差
            offset = random.uniform(-0.001, 0.001)
            item['price_cny'] *= (1 - offset)
            item['price_usd'] *= (1 - offset)

            items.append(item)

        if spider.name == 'exchange' and ege_id != 0:
            # 行情补充成交额占比
            for item in items:
                if isinstance(item, QuotationItem):
                    item['ratio'] = item['turnover_cny'] / turnover_total if turnover_total else 0
            # 缓存中标记交易所为活跃
            yield self._cache.save('Active', 'egeId:{}'.format(ege_id),
                                   '', expire=3600 * 24 * 3)
        return items

    @inlineCallbacks
    def get_pair_ccy_id(self, short_name, en_name, origin_url):
        """ 不同的网站读取不同的缓存 """
        if origin_url:
            pair_left_id = yield self._cache.load('CurrencyUrl', origin_url)
            # 没有URL缓存则根据币种名查询ID
            if pair_left_id is None:
                pair_left_id = yield self._cache.load(
                    'Currency', '{}-{}'.format(short_name, en_name))
                # 如查到ID则保存URL缓存
                if pair_left_id is not None:
                    ex = 3600 * 12 if pair_left_id <= 0 else None
                    yield self._cache.save('CurrencyUrl', origin_url, pair_left_id, expire=ex)
        else:
            pair_left_id = yield self._cache.load(
                'Currency', '{}-{}'.format(short_name, en_name))
        return pair_left_id

    @staticmethod
    def compare_cqn(cqn_cache, item):
        """ 对比交易对在缓存和Item的数据是否相同，优先对比成交量(针对交易所的交易对)"""
        if cqn_cache and float(cqn_cache['price_origin']) == item.get('price_origin', 0) and \
                (float(cqn_cache['volume']) == item.get('volume', 0) or
                 float(cqn_cache['turnover_cny']) == item.get('turnover_cny', 0)):
            return True
        return False

    def check_repeat(self, key):
        """ 检查是否为重复行情 """
        fingerprint = self.stats.get_value('cqn_keys')
        if key in fingerprint:
            return True
        else:
            fingerprint.add(key)
            return False
