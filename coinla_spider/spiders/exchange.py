# -*- coding: utf-8 -*-

"""
交易所爬虫，爬取交易所信息及交易对的数据
"""

import json
import re

from scrapy import Spider, Request, FormRequest

from ..items import ExchangeItem, QuotationItem


class ExchangeSpider(Spider):
    name = 'exchange'
    custom_settings = {
        'ITEM_PIPELINES': {
            'coinla_spider.pipelines.ExchangePipeline': 300,
            'coinla_spider.pipelines.QuotationPipeline': 301
        },
        'DOWNLOADER_MIDDLEWARES': {
            'coinla_spider.middlewares.ExchangeDownloaderMiddleware': 2,
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'coinla_spider.middlewares.RandomUserAgentMiddleware': 110,
            'coinla_spider.middlewares.ApiHeaderMiddleware': 111,
        },
        'SPIDER_MIDDLEWARES': {
            'coinla_spider.middlewares.QuotationSpiderMiddleware': 1
        },
        'CONCURRENT_REQUESTS': 4,
        'RETRY_TIMES': 9,
        'CLOSESPIDER_TIMEOUT': 150
    }
    url_base = 'https://www.feixiaohao.com'

    def __init__(self, page=None, *a, **kw):
        super(ExchangeSpider, self).__init__(*a, **kw)
        if page is not None:
            self.start_page, self.end_page = page.split(',')

    def index_request(self, page):
        return FormRequest(
            'https://dncapi.bqiapp.com/api/v2/exchange/web-exchange',
            formdata={
                'page': str(page),
                'pagesize': '50',
                'isinnovation': '0',
                'type': 'all',
                'token': '',
                'webp': '1',
            },
            method='GET',
            meta={'page': int(page)},
            headers={'Referer': 'https://www.feixiaohao.com/exchange/?page=%s' % page}
        )

    def start_requests(self):
        start_page = getattr(self, 'start_page', '1')
        yield self.index_request(start_page)




    def parse(self, response):
        resp = json.loads(response.body.decode(response.encoding))
        for d in resp['data']:
            detail_url = self.url_base + '/exchange/%s/' % d['id']
            playload_data = {
                    "page":1,
                    "code":str(d['id']),
                    "pagesize":10000,
                    "token":"",
                    "webp":1
                    }
            yield Request(
                url='https://dncapi.bqiapp.com/api/exchange/coinpair_list',
                body=json.dumps(playload_data),
                method='POST',
                callback=self.parse_pairs,
                meta={'ege_name': d['name'],
                      'url': detail_url},
                headers={
                    'Referer': detail_url,
                    'Accept': 'application/json, text/plain, */*',
                    'Content-Type': 'application/json;charset=UTF-8',
                    'Origin': 'https://www.feixiaohao.com',
                   }
            )
        # 判断当前页是否是最后一页
        current_page = response.meta['page']
        last_page = int(resp['maxpage'])
        if current_page < last_page and current_page != int(getattr(self, 'end_page', 0)):
            next_page = current_page + 1
            yield self.index_request(next_page)

    def parse_pairs(self, response):
        data = json.loads(response.body.decode(response.encoding))['data']
        if not data:
            return None

        ege_id = response.meta['ege_id']
        if ege_id is None:
            yield Request(response.meta['url'],
                          callback=self.parse_detail,
                          meta={'ege_id': ege_id,
                                'ege_name': response.meta['ege_name'],
                                'url': response.meta['url']})

        # 交易所行情只更新已存在并且没有WebSocket更新的交易所
        elif response.meta.get('is_updated', False) is False:
            for d in data:
                # 判断时间，只更新不超过1天的数据
                update_time = d['update_time']
                time_kw = re.search(r'(刚刚)|(小时)|(分钟)|(秒)', update_time)
                if time_kw is None:
                    continue
                price_origin = d['price']
                if price_origin is None:
                    continue
                pair_left = re.search(r'[A-Z0-9]+', d['pair1'])[0]
                pair_right = re.search(r'[A-Z]+', d['pair2'])[0]

                yield QuotationItem(
                    url=self.url_base + '/currencies/%s/' % d['coincode'],
                    ege_id=ege_id,
                    pair_left=pair_left if pair_left != 'USD' else 'USDT',
                    pair_right=pair_right,
                    price_usd=d['price_usd'],
                    price_origin=price_origin,
                    volume=d['vol'],
                    turnover_usd=d['volume'],
                    ratio=str(d['accounting']) + '%',
                    update_time=update_time
                )
    def parse_detail(self, response):
        ege_name = response.meta['ege_name']
        info = response.xpath('//div[@class="infoList"]')
        ege_item = ExchangeItem(
            egeId=response.meta['ege_id'],
            exchangeName=ege_name,
            exchangeNameZh=ege_name,
            exchangeNameEn=response.xpath('//title/text()').re_first(
                r'{}-(.+)交易平台'.format(ege_name), default=ege_name),
            country=info.xpath('./div[1]/div[1]/span[2]/text()').re_first(r'\S+', default='未知'),
            link=info.xpath('./div[3]/div[1]/span[2]/a/@href').extract_first(),
            introduce=info.xpath('//div[@class="textBox"]/p/text()').extract_first(),
            recordStatus=1
        )
        ege_item._url = response.meta['url']
        return ege_item
