# -*- coding: utf-8 -*-

"""
币种总市值的爬虫
"""

import json

from scrapy.spiders import Spider, Request

from ..items import ChartItem


class TotalValueSpider(Spider):
    name = 'total_value'
    custom_settings = {
        'ITEM_PIPELINES': {
            'coinla_spider.pipelines.ChartPipeline': 300
        },
        'DOWNLOADER_MIDDLEWARES': {
            'coinla_spider.middlewares.ChartDownloaderMiddleware': 1,
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'coinla_spider.middlewares.RandomUserAgentMiddleware': 110,
        },
        'CONCURRENT_REQUESTS': 2,
        'DOWNLOAD_DELAY': 0.2,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 522, 524, 408, 403],
        'RETRY_TIMES': 10,
        'LOG_LEVEL': 'INFO'
    }
    allowed_domains = ['graphs2.coinmarketcap.com']
    global_api = 'https://graphs2.coinmarketcap.com/global/marketcap-total'
    no_btc_api = 'https://graphs2.coinmarketcap.com/global/marketcap-altcoin'

    def start_requests(self):
        total_value = ChartItem(data_list=list())
        total_value.__collection__ = 'TotalValue'
        yield Request(self.global_api,
                      callback=self.parse_global,
                      dont_filter=True,
                      meta={'item': total_value,
                            'url_base': self.global_api,
                            'step': 3600 * 24 * 7 * 1000})

    def parse_global(self, response):
        item = response.meta['item']
        data = json.loads(response.body.decode(response.encoding))
        if data.get('volume_usd'):
            origin = response.meta.get('original_time', None)
            for i, d in enumerate(data['volume_usd'][::-1]):
                timestamp = d[0]
                if timestamp <= origin:
                    response.meta['is_origin'] = True
                    break
                i = -i - 1
                item['data_list'].append({
                    'timestamp': timestamp,
                    'total_value_usd': data['market_cap_by_available_supply'][i][1],
                    'turnover_usd': data['volume_usd'][i][1],
                })
        no_btc_url = response.url.replace('total', 'altcoin')
        return Request(no_btc_url, callback=self.parse_no_btc,
                       meta=response.meta, dont_filter=True)

    def parse_no_btc(self, response):
        item = response.meta['item']
        data = json.loads(response.body.decode(response.encoding))
        for each in item['data_list']:
            if each.get('total_value_usd_no_btc', None) is None:
                each['total_value_usd_no_btc'] = data[
                    'market_cap_by_available_supply'].pop()[1]
                each['turnover_usd_no_btc'] = data['volume_usd'].pop()[1]
        if response.meta.get('is_origin', None) is True:
            return item
        return Request(response.meta['url_base'], callback=self.parse_global,
                       meta=response.meta, dont_filter=True)
