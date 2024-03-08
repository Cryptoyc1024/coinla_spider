# -*- coding: utf-8 -*-

"""
交易对行情数据计算，实时计算所有交易对的最新全网价格、交易量等数据
没有爬虫行为，只是为了统一使用了Scrapy运行调用
"""

import warnings
from datetime import datetime, timedelta

import numpy as np
from outliers import smirnov_grubbs as grubbs
from scrapy import Spider, Request
from twisted.internet.defer import inlineCallbacks
from txmongo.filter import sort

from coinla_spider.databases.connections import mongo_db
from coinla_spider.databases.getters import \
    get_common_ege_id, get_otc_ege_id, get_tocny_exrate
from ..items import CurrencyItem, QuotationItem


class CurrencyQuotationSpider(Spider):
    name = 'calc_cqn'
    custom_settings = {
        'ITEM_PIPELINES': {
            'coinla_spider.pipelines.QuotationPipeline': 300
        },
        'DOWNLOADER_MIDDLEWARES': {
            'coinla_spider.middlewares.DummyRequestMiddleware': 1,
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
        },
        'SPIDER_MIDDLEWARES': {
            'coinla_spider.middlewares.QuotationSpiderMiddleware': 1
        },
        'CLOSESPIDER_TIMEOUT': 30
    }

    def __init__(self, limit=None, *a, **kw):
        super(CurrencyQuotationSpider, self).__init__(*a, **kw)
        self.limit = limit
        if limit is not None:
            self.start_page, self.end_page = limit.split(',')

    def start_requests(self):
        yield Request(':', callback=self.calc_cqn)

    @inlineCallbacks
    def calc_cqn(self, response):
        usd_exrate = yield get_tocny_exrate('USD')
        common_ege_id = yield get_common_ege_id()
        otc_ege_id = yield get_otc_ege_id()

        # 若限制计算币种的数量，则根据流通市值从大到小取币种ID
        if self.limit is not None:
            start = int(self.start_page) - 1 if self.start_page else 0
            end = int(self.end_page) if self.end_page else 0
            ccy_top = yield mongo_db[CurrencyItem.__collection__].find(
                {'recordStatus': {'$ne': -1}}, {'ccyId': 1, '_id': 0},
                sort=sort([('sortCirculateTotalValue', -1)]),
                skip=start, limit=end
            )
            ccy_id_filter = {'$in': [i['ccyId'] for i in ccy_top]}
        else:
            # 排除法币
            legal_docs = yield mongo_db[CurrencyItem.__collection__].find(
                {'recordStatus': -1}, {'ccyId': 1, '_id': 0})
            ccy_id_filter = {'$nin': [d['ccyId'] for d in legal_docs]}

        docs = yield mongo_db[QuotationItem.__collection__].aggregate([
            {
                '$match': {
                    # 只计算24小时内的行情
                    'date': {'$gte': datetime.utcnow() - timedelta(days=1)},
                    'ccyId': ccy_id_filter,
                    'type': 0,
                    # 排除全网交易所
                    'egeId': {'$nin': [common_ege_id, otc_ege_id]},
                    'openTurnoverSort': {'$gt': 0},
                }
            },
            {
                '$group': {
                    '_id': '$ccyId',
                    'pair_left': {'$last': '$currencyShortName'},
                    'pair_left_en': {'$last': '$currencyEnglisgName'},
                    'price_cny': {'$push': '$openPrice'},
                    'turnover_cny': {'$push': '$openTurnoverSort'}
                }
            }
        ])

        def convert_item(doc):
            if len(doc['price_cny']) >= 3:
                # 通过格拉布斯准则去除异常数据
                warnings.filterwarnings('error', category=RuntimeWarning)
                try:
                    outlier_idx = grubbs.two_sided_test_indices(doc['price_cny'], alpha=0.95)
                    for idx in sorted(outlier_idx, reverse=True):
                        doc['price_cny'].pop(idx)
                        doc['turnover_cny'].pop(idx)
                except RuntimeWarning:
                    pass

                # 若数组极差大于平均值，则分成两组，取数量多的组
                if len(doc['price_cny']) > 3:
                    avg = np.mean(doc['price_cny'])
                    if np.ptp(doc['price_cny']) > avg:
                        min_group = {d: i for i, d in enumerate(doc['price_cny']) if d < avg}
                        max_group = {d: i for i, d in enumerate(doc['price_cny']) if d > avg}
                        more_group = min_group if len(min_group) > len(max_group) else max_group
                        doc['price_cny'] = list(more_group.keys())
                        doc['turnover_cny'] = [doc['turnover_cny'][i] for i in more_group.values()]

            # 按加权平均法计算全网价格
            weights = doc['turnover_cny'] if sum(doc['turnover_cny']) > 0 else None
            doc['price_cny'] = np.average(doc['price_cny'], weights=weights)
            doc['price_usd'] = doc['price_cny'] / usd_exrate
            doc['turnover_cny'] = sum(doc['turnover_cny'])
            doc['turnover_usd'] = doc['turnover_cny'] / usd_exrate
            doc.update(
                pair_left_id=doc.pop('_id'),
                pair_right='CNY',
                ege_id=-1,
                price_origin=doc['price_cny']
            )

            return QuotationItem(doc)

        return map(convert_item, docs)
