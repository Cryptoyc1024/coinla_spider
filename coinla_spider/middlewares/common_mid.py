# -*- coding: utf-8 -*-

import logging
import random
import re

from scrapy.downloadermiddlewares.useragent import UserAgentMiddleware
from scrapy.exceptions import IgnoreRequest
from scrapy.http import HtmlResponse
from twisted.internet.defer import inlineCallbacks

from .base_mid import ConnectionBaseMiddleware
from ..databases.getters import get_ccy_short_name_count


class RandomUserAgentMiddleware(UserAgentMiddleware):

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings.getlist('USER_AGENTS'))

    def __init__(self, user_agent):
        self.user_agent = user_agent

    def process_request(self, request, spider):
        agent = random.choice(self.user_agent)
        request.headers['User-Agent'] = agent


class ProxyMiddleware(object):

    def process_request(self, request, spider):
        if 'proxy' in request.meta and not request.meta['proxy']:
            return None
        if 'splash' in request.meta:
            request.meta['splash']['args']['proxy'] = spider.settings.get('PROXY')
        else:
            request.meta['proxy'] = spider.settings.get('PROXY')


class DummyRequestMiddleware(object):

    def process_request(self, request, spider):
        if request.url == ':':
            return HtmlResponse(url=request.url)


class ApiHeaderMiddleware(object):

    def process_request(self, request, spider):
        if 'dncapi.feixiaohao.com' not in request.url:
            return None
        request.headers.update({
            'Accept': 'application/json, text/plain, */*',
            'Connection': 'keep-alive',
            'Host': 'dncapi.feixiaohao.com',
            'Origin': 'https://www.feixiaohao.com',
        })
        if request.method == 'POST':
            request.headers['Content-Type'] = 'application/json;charset=UTF-8'


class GetCcyIdMiddleware(ConnectionBaseMiddleware):

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.stats)

    def __init__(self, stats):
        super(GetCcyIdMiddleware, self).__init__()
        stats.set_value('ccy_id', set())
        self.stats = stats

    @inlineCallbacks
    def process_request(self, request, spider):
        if 'ccy_id' in request.meta:
            return None

        if request.meta.get('log', False) is True:
            log_level = logging.ERROR
        else:
            log_level = logging.INFO

        if 'ccy_short_name' in request.meta:
            short = request.meta['ccy_short_name']
            cache_key = '{}-{}'.format(short, request.meta.get('ccy_en_name', '*'))
            if '*' in cache_key:
                name_count = yield get_ccy_short_name_count(short)
                if name_count > 1:
                    logging.log(log_level, '币种: {} 缓存不是唯一的'.format(short))
                    raise IgnoreRequest()
                elif name_count == 0:
                    logging.log(log_level, '币种: {} 缓存不存在'.format(short))
                    raise IgnoreRequest()
            request.meta['ccy_id'] = yield self._cache.load('Currency', cache_key)
            if not request.meta['ccy_id']:
                logging.log(log_level, '币种: {} 缓存不存在'.format(short))
                raise IgnoreRequest()
            if self.fitter_ccy_id(request.meta['ccy_id']) is True:
                logging.log(log_level, '币种: {} 已被去重'.format(short))
                raise IgnoreRequest()

        elif 'ccy_url' in request.meta:
            request.meta['ccy_id'] = yield self._cache.load(
                'CurrencyUrl', request.meta['ccy_url'])
            if not request.meta['ccy_id']:
                logging.log(log_level, '币种: {} 缓存不存在'.format(
                    re.findall(r'/currencies/(.+)/', request.meta['ccy_url'])[0]))
                raise IgnoreRequest()
            elif self.fitter_ccy_id(request.meta['ccy_id']) is True:
                logging.log(log_level, '币种: {} 已被去重'.format(
                    re.findall(r'/currencies/(.+)/', request.meta['ccy_url'])[0]))
                raise IgnoreRequest()

    def fitter_ccy_id(self, key):
        """ 对相同的币种ID进行去重 """
        fingerprint = self.stats.get_value('ccy_id')
        if key in fingerprint:
            return True
        else:
            fingerprint.add(key)
            return False
