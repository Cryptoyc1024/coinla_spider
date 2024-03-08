# -*- coding: utf-8 -*-

"""
法币汇率爬虫，存储汇率用于前后端将市场价数据换算成多种法币
"""

from scrapy import Spider, Request

from ..items import ExrateItem


class ExrateSpider(Spider):
    name = 'exrate'
    custom_settings = {
        'ITEM_PIPELINES': {
           'coinla_spider.pipelines.ExratePipeline': 300
        },
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'coinla_spider.middlewares.RandomUserAgentMiddleware': 110,
        },
        'DOWNLOAD_DELAY': 0.3,
        'CLOSESPIDER_TIMEOUT': 600
    }
    allowed_domains = ['qq.ip138.com']
    start_urls = ['http://qq.ip138.com/hl.asp']

    def parse(self, response):
        for ccy in response.xpath('//*[@id="from"]/optgroup/option/@value').extract():
            if ccy == 'CNY':
                continue
            url = '{}?from={}&to=CNY&q=100'.format(self.start_urls[0], ccy)
            item = ExrateItem(ccy_short_name=ccy)
            yield Request(url, callback=self.parse_data, meta={'item': item})

    def parse_data(self, response):
        item = response.meta['item']
        item['ccy_cn_name'] = response.xpath('//table[@class="rate"]/tr[2]/td[1]/text()').extract_first()
        item['exrate'] = response.xpath(
            '//table[@class="rate"]/tr[3]/td[2]/text()').re_first(r'[\d\.]*\d$')
        if item['ccy_short_name'] == 'RUB':
            rur_item = item.copy()
            rur_item['ccy_short_name'] = 'RUR'
            rur_item['ccy_cn_name'] = '老卢布'
            yield rur_item
        if item['exrate']:
            yield item
