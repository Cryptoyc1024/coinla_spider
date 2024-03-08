# -*- coding: utf-8 -*-

from scrapy.exceptions import IgnoreRequest
from twisted.internet.defer import inlineCallbacks

from .base_mid import ConnectionBaseMiddleware
from ..databases.getters import get_tocny_exrate


class ExchangeDownloaderMiddleware(ConnectionBaseMiddleware):
    _usd_exrate = None

    @inlineCallbacks
    def process_request(self, request, spider):
        if request.callback == spider.parse_pairs:
            request.meta['ege_id'] = ege_id = \
                yield self._cache.load('ExchangeUrl', request.meta['url'])
            # 缓存中已标记为活跃则不更新交易所
            if request.callback == spider.parse_detail and \
                    (yield self._cache.is_exists(
                        'Acitve', 'egeId:{}'.format(ege_id))):
                raise IgnoreRequest()
            # 缓存中已有WebSocket更新则不更新交易对
            if (yield self._cache.cli.exists(
                    'CoinlaCache:websocket:{}'.format(ege_id))):
                request.meta['is_updated'] = True

            request.meta['usd_exrate'] = self._usd_exrate or (
                yield get_tocny_exrate('USD'))
