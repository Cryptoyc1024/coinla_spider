# -*- coding: utf-8 -*-

import json
import logging
from itertools import chain

from scrapy import Request
from scrapy.exceptions import IgnoreRequest
from twisted.internet.defer import inlineCallbacks

from .base_mid import ConnectionBaseMiddleware
from ..databases.getters import get_tocny_exrate
from ..settings import EXCLUDE_URL


class CurrencyDownloaderMiddleware(ConnectionBaseMiddleware):

    def __init__(self):
        super(CurrencyDownloaderMiddleware, self).__init__()
        self.ccy_ids = set()
        self.btc_id = None

    @inlineCallbacks
    def process_request(self, request, spider):
        # TODO 使用GetCcyIdMid替换
        if 'detail' in request.callback.__name__:
            url = request.meta['url']
            # 排除配置项指定的URL
            if url in EXCLUDE_URL:
                logging.info('{} 币种被预设排除'.format(url))
                raise IgnoreRequest()
            ccy_id = yield self._cache.load('CurrencyUrl', url)
            if ccy_id is None or ccy_id < 0:
                request.meta['ccy_id'] = None
                return None
            elif ccy_id == 0:
                logging.info('{} 币种ID=0 需人工处理'.format(url))
                raise IgnoreRequest()
            elif ccy_id in self.ccy_ids:
                logging.info('{} ccyId:{} 被去重'.format(url, ccy_id))
                raise IgnoreRequest()
            self.ccy_ids.add(ccy_id)
            request.meta['ccy_id'] = ccy_id

    @inlineCallbacks
    def process_response(self, request, response, spider):
        if 'detail' in request.callback.__name__ and response.status < 300:
            if request.callback == spider.parse_tokenclub_detail:
                data = json.loads(response.body.decode(response.encoding))
                if data['code'] != 0:
                    raise IgnoreRequest()
                data = data['data']
                if not data['websites'] or not data['websites'][0]:
                    raise IgnoreRequest()
            # 币种核心数据的计算类数据
            ccy_id = request.meta['ccy_id']
            if ccy_id is not None:
                request.meta.update((yield self.get_prices(ccy_id)))
                request.meta['turnover'] = yield self.get_turnover(ccy_id)
        return response

    @inlineCallbacks
    def get_prices(self, ccy_id):
        if self.btc_id is None:
            self.btc_id = yield self._cache.load('Currency', 'BTC-Bitcoin')
        price_cny = float((yield self._cache.load(
            'CurrencyData', ccy_id, 'price_cny', command='hget')) or 0)
        if price_cny == 0:
            return {'price_cny': 0, 'price_usd': 0, 'price_btc': 0}
        price_usd = price_cny / (yield get_tocny_exrate('USD'))
        price_btc = price_cny / (yield get_tocny_exrate('BTC')) \
            if ccy_id != self.btc_id else 1
        return {'price_cny': price_cny,
                'price_usd': price_usd,
                'price_btc': price_btc}

    @inlineCallbacks
    def get_turnover(self, ccy_id):
        return float((yield self._cache.load(
            'CurrencyData', ccy_id, 'turnover_cny', command='hget')) or 0)


class CurrencySpiderMiddleware(ConnectionBaseMiddleware):

    @inlineCallbacks
    def process_start_requests(self, start_requests, spider):
        add_ccy = yield self._cache.load(
            'Addition', 'Currency', command='smembers')
        yield self._cache.delete('Addition', 'Currency')
        requests = list()
        for url in add_ccy:
            if 'www.feixiaohao.com' in url:
                requests.append(
                    Request(url, callback=spider.parse_detail,
                            meta={'url': url})
                )
            elif 'schail' in url:
                requests.append(
                    Request(url, callback=spider.parse_tokenclub_detail,
                            meta={'url': url},
                            headers={'Referer': 'http://www.tokenclub.com/'},
                            priority=-1)
                )
        return chain(requests, start_requests)
