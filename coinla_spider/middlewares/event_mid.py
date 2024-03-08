# -*- coding: utf-8 -*-

import logging

from scrapy.exceptions import IgnoreRequest
from twisted.internet.defer import inlineCallbacks

from .base_mid import ConnectionBaseMiddleware
from ..items import EventItem


class EventDownloaderMiddleware(ConnectionBaseMiddleware):

    @inlineCallbacks
    def process_request(self, request, spider):
        if 'event' in request.callback.__name__:
            # 事件字段不为空则不进行更新
            docs = list((yield self._mongo_db[EventItem.__collection__].find(
                {'ccy_id': request.meta['ccy_id'], 'type': 0,
                 'event': {'$nin': [[], None]}}, {'_id': 1})))
            if docs:
                logging.info('币种: {} ID: {} 已存在Event'.format(
                    request.meta.get('ccy_short_name', 'ccy_code'),
                    request.meta['ccy_id'])
                )
                raise IgnoreRequest()
