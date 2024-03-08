# -*- coding: utf-8 -*-

import logging

from twisted.internet.defer import inlineCallbacks

from .base_mid import ConnectionBaseMiddleware
from ..databases.getters import get_ccy_short_name_count
from ..items import FormatItem


class DevelopSpiderMiddleware(ConnectionBaseMiddleware):

    @inlineCallbacks
    def process_spider_output(self, response, result, spider):
        items = list()
        for item in result:
            if isinstance(item, FormatItem):
                short = item._ccy_short_name
                name_count = yield get_ccy_short_name_count(short)
                if name_count > 1:
                    log_level = self._get_log_level(response)
                    logging.log(log_level, '币种: {} 缓存不是唯一的'.format(short))
                    continue
                elif name_count == 0:
                    log_level = self._get_log_level(response)
                    logging.log(log_level, '币种: {} 缓存不存在'.format(short))
                    continue
                item['ccy_id'] = yield self._cache.load(
                    'Currency', '{}-*'.format(short))
            items.append(item)
        return items

    @staticmethod
    def _get_log_level(response):
        code_id = response.meta.get('coin_id')
        if code_id and int(code_id[0]) < 20:
            return logging.ERROR
        else:
            return logging.INFO
