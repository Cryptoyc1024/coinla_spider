# -*- coding: utf-8 -*-

"""
币种每日行情数据爬虫，用于汇总数据
"""

import time
from datetime import datetime

from scrapy import Spider, Request

from ..items import CurrencyDailyDataItem


class CurrencyDailyDataSpider(Spider):
    name = 'ccy_daily'
    custom_settings = {
        'ITEM_PIPELINES': {
            'coinla_spider.pipelines.CurrencyDailyDataPipeline': 300
        },
        'DOWNLOADER_MIDDLEWARES': {
            'coinla_spider.middlewares.CurrencyDailyDataDownloaderMiddleware': 1,
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'coinla_spider.middlewares.RandomUserAgentMiddleware': 110,
        },
        'CONCURRENT_REQUESTS': 4,
        'DOWNLOAD_DELAY': 0.3,
        # 404 是没有数据的币种，需要忽略
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 522, 524, 408, 403],
        'RETRY_TIMES': 10,
        'CLOSESPIDER_TIMEOUT': 600
    }
    allowed_domains = ['coinmarketcap.com']
    start_urls = ['https://coinmarketcap.com/zh/coins/views/all/']

    def parse(self, response):
        for ccy in response.xpath('//tbody/tr'):
            item = CurrencyDailyDataItem(
                ccy_short_name=ccy.xpath('./td/span/a/text()').extract_first(),
                ccy_en_name=ccy.xpath(
                    './/td[@class="no-wrap currency-name"]/@data-sort').extract_first(),
            )
            detail_url = 'https://coinmarketcap.com{}historical-data/' \
                         '?start=20130101&end=20280101'.format(
                ccy.xpath('./td/span/a/@href').extract_first())
            yield Request(detail_url, meta={'item': item}, callback=self.parse_table)

    def parse_table(self, response):
        item = response.meta['item']
        last_data_time = item.get('last_data_time', None)
        item['data_list'] = list()
        for tr in response.xpath('//tbody/tr'):
            date_str = tr.xpath('./td[1]/text()').extract_first()
            if not date_str == "\n":


                date_time = datetime.strptime(date_str, '%Y年%m月%d日')
                if last_data_time is not None and last_data_time >= date_time:
                    break
                item['data_list'].append({
                    'date_time': int(time.mktime(date_time.timetuple()) * 1000),
                    'open_price': float(tr.xpath('./td[2]/@data-format-value').re_first(r'.*\d$', default='0')),
                    'high_price': float(tr.xpath('./td[3]/@data-format-value').re_first(r'.*\d$', default='0')),
                    'low_price': float(tr.xpath('./td[4]/@data-format-value').re_first(r'.*\d$', default='0')),
                    'close_price': float(tr.xpath('./td[5]/@data-format-value').re_first(r'.*\d$', default='0')),
                    # 24H成交额
                    'volume': float(tr.xpath('./td[6]/@data-format-value').re_first(r'.*\d$', default='0')),
                    # 流通市值
                    'total_value': float(tr.xpath('./td[7]/@data-format-value').re_first(r'.*\d$', default='0')),
                })
            if not item['data_list']:
                return None
            item['last_data_time'] = datetime.fromtimestamp(item['data_list'][0]['date_time'] / 1000)

            return item
