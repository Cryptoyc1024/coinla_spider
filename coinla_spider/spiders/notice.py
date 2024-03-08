# -*- coding: utf-8 -*-

"""
主流交易所公告爬虫
"""

import re
from datetime import datetime

from scrapy.spiders import Spider, Request

from ..items import NoticeItem


class NoticeSpider(Spider):
    name = 'notice'
    custom_settings = {
        'ITEM_PIPELINES': {
            'coinla_spider.pipelines.NoticePipeline': 300
        },
        'DOWNLOADER_MIDDLEWARES': {
            'coinla_spider.middlewares.NoticeDownloaderMiddleware': 1,
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'coinla_spider.middlewares.RandomUserAgentMiddleware': 110,

        },
        'CONCURRENT_REQUESTS_PER_DOMAIN': 2,
        'DOWNLOAD_DELAY': 0.3,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 522, 524, 408, 403, 404],
        'RETRY_TIMES': 2,
        'CLOSESPIDER_TIMEOUT': 180,
    }

    def start_requests(self):
        yield Request('https://huobiglobal.zendesk.com/hc/zh-cn/categories/'
                      '360000031902-Huobi-Global-%E5%85%AC%E5%91%8A',
                      callback=self.parse_common_index, meta={'ege_name': '火币*'})
        yield Request('https://support.binance.com/hc/zh-cn/categories/'
                      '115000056351-%E5%85%AC%E5%91%8A%E4%B8%AD%E5%BF%83',
                      callback=self.parse_common_index, meta={'ege_name': '币安*'})
        yield Request('https://support.okex.com/hc/zh-cn/categories/'
                      '115000275131-%E5%85%AC%E5%91%8A%E4%B8%AD%E5%BF%83',
                      callback=self.parse_common_index, meta={'ege_name': 'OKEx'})
        yield Request('https://support.bitforex.com/hc/zh-cn/categories/360000629712',
                      callback=self.parse_common_index, meta={'ege_name': '币夫*'})
        yield Request('https://www.zb.com/i/blog?type=proclamation',
                      callback=self.parse_zb_index, meta={'ege_name': 'ZB'})

    def parse_common_index(self, response):
        for url in response.xpath('//ul[@class="article-list"]/li/a/@href').extract():
            yield Request(response.urljoin(url), callback=self.parse_common_detail,
                          meta={'ege_id': response.meta['ege_id']})

    def parse_common_detail(self, response):
        body = response.xpath('//div[@class="article-body"]/*')
        content = ''.join(body.getall())
        # 将相对路径转成绝对路径
        for url in body.xpath('.//@src | .//@href').extract():
            content = content.replace(url, response.urljoin(url), 1)
        update_date = response.xpath('//div[@class="article-meta"]/ul/li/time/@title').extract_first()
        yield NoticeItem(
            ege_id=response.meta['ege_id'],
            notice_title=response.xpath('//h1[@class="article-title"]/@title').extract_first(),
            notice_content=content,
            notice_date=self._extract_date(content, update_date),
            original_link=response.url
        )

    def parse_zb_index(self, response):
        for url in response.xpath('//figure/a/@href').extract():
            yield Request(response.urljoin(url), callback=self.parse_zb_detail,
                          meta={'ege_id': response.meta['ege_id']})

    def parse_zb_detail(self, response):
        body = response.xpath('//article/*')
        content = ''.join(body.getall())
        for node in body.xpath('.//*[@href]'):
            content = content.replace(node.get(), node.xpath('./text()').extract_first(default=''), 1)
        update_date = response.xpath('//div[@class="row"]/div/p/span[1]/text()').extract_first()
        yield NoticeItem(
            ege_id=response.meta['ege_id'],
            notice_title=response.xpath('//h2[@class="align-center"]/text()').extract_first(),
            notice_content=content,
            notice_date=self._extract_date(content, update_date),
            original_link=response.url
        )

    def _extract_date(self, content, spare):
        """ 提取公告日期 """
        date_str = re.findall(r'(\d{4}年\d{1,2}月\d{1,2}日)<', content[-30:])
        if date_str:
            return datetime.strptime(date_str[0], '%Y年%m月%d日')
        else:
            date_str = re.findall(r'\d{4}-\d{1,2}-\d{1,2}', spare)
            return datetime.strptime(date_str[0], '%Y-%m-%d')
