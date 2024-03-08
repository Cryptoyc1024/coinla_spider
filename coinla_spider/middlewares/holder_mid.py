# -*- coding: utf-8 -*-

from itertools import chain

from scrapy import Request
from twisted.internet.defer import inlineCallbacks

from .base_mid import ConnectionBaseMiddleware
from ..items import CurrencyItem


class HolderSpiderMiddleware(ConnectionBaseMiddleware):

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def __init__(self, crawler):
        super(HolderSpiderMiddleware, self).__init__()
        self.stats = crawler.stats

    @inlineCallbacks
    def process_start_requests(self, start_requests, spider):
        """ 查询属于ETH系列但没有持币排行的币种，追加Request """
        ccy_docs = yield self._mongo_db[CurrencyItem.__collection__].find(
            {'blockChain': {'$regex': r'(etherscan\.io/token/.+)|(chainz\.cryptoid\.info/.+)'}},
            {'ccyId': 1, 'blockChain': 1, '_id': 0})
        ccy_data = {d['ccyId']: d['blockChain'] for d in ccy_docs}
        requests = list()
        for k in ccy_data.keys():
            if 'etherscan' in ccy_data[k]:
                requests.append(Request(
                    ccy_data[k], callback=spider.parse_eth_holder_url,
                    headers={'Referer': 'https://etherscan.io/tokens'},
                    meta={'ccy_id': k, 'dont_redirect': True}
                ))
            elif 'cryptoid' in ccy_data[k]:
                if ccy_data[k][-1] != '/':
                    ccy_data[k] += '/'
                requests.append(Request(
                    ccy_data[k], callback=spider.parse_cryptoid_circulate,
                    headers={'Referer': 'https://chainz.cryptoid.info'},
                    meta={'ccy_id': k}
                ))
            self.stats.get_value('ccy_id').add(k)
        return chain(requests, start_requests)
