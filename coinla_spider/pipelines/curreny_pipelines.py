# -*- coding: utf-8 -*-

import logging
import re

from twisted.internet.defer import inlineCallbacks
from txmongo.errors import TimeExceeded

from coinla_spider.databases.getters import get_circulate_value_map, get_ccy_id
from coinla_spider.databases.models import Currency
from .base_pipelines import ConnectionPipeline
from ..items import CurrencyItem, \
    CurrencyCoreDataItem, DevelopDataItem, EventItem


class CurrencyPipeline(ConnectionPipeline):

    @inlineCallbacks
    def process_item(self, item, spider):
        if not isinstance(item, CurrencyItem):
            return item

        if item['ccyId'] is None:
            field_map = {
                'currency_name': item['currencyName'],
                'english': item['english'],
                'short_name': item['shortName'],
                'initiate_create_date': item['initiateCreateDate'],
                'guanw': item['guanw'],
                'white_paper_zh': item['whitePaperZh'],
                'white_paper_en': item['whitePaperEn'],
                'block_chain': item['blockChain'],
                'record_status': item['recordStatus']
            }
            yield get_ccy_id(item, field_map)

        else:
            # 若允许更新图片，判断币种在SQL是否有图片链接，没有则加入
            if spider.settings.getbool('ALLOW_PIC') is True and item['pic']:
                ccy = self._sql_db.select(Currency, {Currency.id == item['ccyId']})
                if not ccy.pic:
                    pic_name = re.sub(r'[？?\\*|“<>:/\s]', '', '{}-{}.{}'.format(
                        item['english'], item['shortName'], 'png'))
                    item['pic'] = 'http://image.coinla.com/currency/logo2/' + pic_name
                    item._pic_name = pic_name

            # 若允许更新SQL，则更新数据字段
            if spider.settings.getbool('ALLOW_UPDATE_SQL') is True:
                field_map = {
                    'circulate_total': item['circulateTotal'],
                    'sort_circulate_total_value': item['sortCirculateTotalValue'],
                    'circulate_total_value': item['circulateTotalValue'],
                    'sort_total_value': item['sortTotalValue'],
                    'total_value': item['totalValue'],
                    'total': item['total'],
                    'market_qty': item['numberOfExchange']
                }
                self._sql_db.update(Currency, item['ccyId'], field_map)

            try:
                yield self._mongo_db[item.__collection__].update_one(
                    {'ccyId': item['ccyId']}, {'$set': item}, timeout=20)
            except TimeExceeded as e:
                logging.warning(e)
            except Exception as e:
                logging.error(e)

            # Redis保存行情信息
            data = {
                'total': item._total,
                'totalValue': item['sortTotalValue'],
                'totalValueUsd': item._total_value_usd,
                'circulateTotal': item._circulate_total,
                'circulateTotalValue': item['sortCirculateTotalValue'],
                'circulateTotalValueUsd': item._circulate_usd
            }
            redis_key = 'quotation:currencyOpen:{}'.format(item['ccyId'])
            yield self._redis_db.hmset(redis_key, data)

            data = {
                'circulate_total': item._circulate_total,
                'circulate_value': item['sortCirculateTotalValue'],
                'total': item._total
            }
            yield self._cache.save('CurrencyData', item['ccyId'], data,
                                   command='hmset', expire=3600 * 24 * 15)
        return item


class CurrencyCoreDataPipeline(ConnectionPipeline):

    def __init__(self, crawler):
        super(CurrencyCoreDataPipeline, self).__init__(crawler)
        self.insert_fields = self.init_insert_fields()

    @inlineCallbacks
    def process_item(self, item, spider):
        try:
            if isinstance(item, CurrencyCoreDataItem):
                yield self._mongo_db[item.__collection__].update_one(
                    {'ccy_id': item['ccy_id'], 'type': item['type']},
                    {'$set': item, '$setOnInsert': self.insert_fields},
                    upsert=True, timeout=20)

            elif isinstance(item, EventItem):
                # 事件字段只更新为空的情况
                yield self._mongo_db[item.__collection__].update_many(
                    {'ccy_id': item['ccy_id'], 'event': {'$in': [[], None]}},
                    {'$set': {'event': item['event']}}, timeout=20)

        except TimeExceeded as e:
            logging.warning(e)
        except Exception as e:
            logging.error(e)

        return item

    @inlineCallbacks
    def open_spider(self, spider):
        circ_value_map = yield get_circulate_value_map()
        circ_value_total = sum(circ_value_map.values())
        spider.crawler.stats.set_value('circ_value_map', circ_value_map)
        spider.crawler.stats.set_value('circ_value_total', circ_value_total)

    @inlineCallbacks
    def close_spider(self, spider):
        yield self.update_circ_value_rank()

    @staticmethod
    def init_insert_fields():
        """ 币种核心数据新建时的补充字段 """
        fields = dict()
        dev_item = DevelopDataItem()
        for k in dev_item.fields:
            if k not in ['ccy_id']:
                dev_item[k] = None
        dev_item.pop('update_time')
        fields.update(dev_item)
        fields['market_qty'] = 0
        fields['event'] = list()
        return fields

    @inlineCallbacks
    def update_circ_value_rank(self):
        """ 更新流通市值排名字段 """
        circ_value_map = yield get_circulate_value_map()
        for ccy_id, circ in circ_value_map.items():
            turnover = float((yield self._cache.load(
                'CurrencyData', ccy_id, 'turnover_cny', command='hget')) or 0)
            if not circ or turnover / circ < 0.001:
                circ_value_map[ccy_id] = 0
        other_docs = yield self._mongo_db[CurrencyItem.__collection__].find(
            {'ccyId': {'$nin': list(circ_value_map.keys())}}, {'ccyId': 1, '_id': 0})
        for d in other_docs:
            circ_value_map[d['ccyId']] = 0
        circ_value_sort = sorted(circ_value_map, key=lambda x: circ_value_map[x],
                                 reverse=True)
        for ccy_id in circ_value_map:
            rank = circ_value_sort.index(ccy_id) + 1
            yield self._mongo_db[CurrencyCoreDataItem.__collection__].update_many(
                {'ccy_id': ccy_id}, {'$set': {'circulate_total_value_rank': rank}})
