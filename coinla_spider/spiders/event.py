# -*- coding: utf-8 -*-

"""
币种事件爬虫，爬取币种的大事件时间节点和描述
"""

import json
import re

from scrapy import Spider, Request

from ..items import EventItem


class EventSpider(Spider):
    name = 'event'
    custom_settings = {
        'ITEM_PIPELINES': {
            'coinla_spider.pipelines.CurrencyCoreDataPipeline': 303,
        },
        'DOWNLOADER_MIDDLEWARES': {
            'coinla_spider.middlewares.GetCcyIdMiddleware': 1,
            'coinla_spider.middlewares.EventDownloaderMiddleware': 2,
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'coinla_spider.middlewares.RandomUserAgentMiddleware': 110,
        },
        'CONCURRENT_REQUESTS': 4,
        'DOWNLOAD_DELAY': 1,
        'CLOSESPIDER_TIMEOUT': 3600
    }
    aicoin_base = 'https://www.aicoin.net.cn/currencies/all/cny/%d/desc'

    def start_requests(self):
        yield Request(self.aicoin_base % 1, callback=self.parse,
                      meta={'page': 1})

    def parse(self, response):
        body = response.body.decode(response.encoding)
        ccy_body = re.findall(r'"list":(\[.*?\]),"', body)[0]
        data = json.loads(ccy_body)
        if not data:
            return None
        for d in data:
            yield Request('https://www.aicoin.net.cn/currencies/%s.html' % d['key'],
                          callback=self.parse_aicoin_event,
                          meta={'ccy_short_name': d['coin'].upper()})
        page = response.meta['page'] + 1
        yield Request(self.aicoin_base % page, callback=self.parse,
                      meta={'page': page})

    def parse_aicoin_event(self, response):
        body = response.body.decode(response.encoding)
        event_body = re.findall(r'"develop":(\[.*?\]),"', body)[0]
        data = json.loads(event_body)
        event = list()
        for d in data:
            if 'event' not in d:
                continue
            event.append({
                'time': d.get('time', ''),
                'title': d['event']
            })
        if event:
            return EventItem(
                ccy_id=response.meta['ccy_id'],
                event=event
            )

    def parse_fxh_index(self, response):
        for tr in response.xpath('//table[@id="table"]/tbody/tr'):
            ccy_code = tr.xpath('./td[2]/a/@href').re_first(r'/currencies/(.+)/')
            ccy_url = 'https://www.feixiaohao.com/currencies/{}/'.format(ccy_code)
            yield Request('https://api.feixiaohao.com/coinevent/{}/'.format(ccy_code),
                          callback=self.parse_fxh_event,
                          meta={'ccy_url': ccy_url, 'ccy_code': ccy_code})
        page_list = response.xpath('//div[1]/a[contains(@class, "btn btn-white")]/@href').extract()
        if page_list:
            next_page = page_list[-1]
            if next_page != '#':
                return Request(response.urljoin(next_page), callback=self.parse_fxh_index)

    def parse_fxh_event(self, response):
        event = list()
        if response.body:
            for li in response.xpath('//li'):
                # 日期字符串转标准格式
                date_str = li.xpath('./div[@class="time"]/text()').re_first(r'\S+')
                dt = re.findall(r'(\d{4})[\w/.](\d{1,2})[\w/.](\d{1,2})[\w/.]*', date_str)
                if dt:
                    date_str = '{}-{}-{}'.format(dt[0], dt[1], dt[2])
                event.append({
                    'time': date_str,
                    'title': li.xpath('./div[@class="tit"]/h3/text()').extract_first(default='').strip()
                })
        if event:
            return EventItem(
                ccy_id=response.meta['ccy_id'],
                event=event
            )
