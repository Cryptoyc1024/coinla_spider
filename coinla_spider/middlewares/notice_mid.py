# -*- coding: utf-8 -*-

import logging

from scrapy.exceptions import IgnoreRequest
from twisted.internet.defer import inlineCallbacks

from .base_mid import ConnectionBaseMiddleware


class NoticeDownloaderMiddleware(ConnectionBaseMiddleware):

    @inlineCallbacks
    def process_request(self, request, spider):
        if 'ege_name' in request.meta:
            request.meta['ege_id'] = yield self._cache.load(
                'Exchange', '{}-*'.format(request.meta['ege_name']))
            if request.meta['ege_id'] is None:
                logging.error('{} 读取交易所ID缓存出错'.format(
                    request.meta['ege_name']))
                raise IgnoreRequest()

        elif 'ege_id' in request.meta:
            if (yield self._cache.is_exists('Notice', request.url)):
                logging.info('{} 已存在'.format(request.url))
                raise IgnoreRequest()
