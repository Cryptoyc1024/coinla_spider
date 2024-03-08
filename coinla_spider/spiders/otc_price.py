# -*- coding: utf-8 -*-

"""
主流币种的OTC场外价格爬虫
"""

import json
import re

from scrapy import Spider, Request, FormRequest

from ..items import OTCPriceItem


class OTCPriceSpider(Spider):
    name = 'otc_price'
    custom_settings = {
        'ITEM_PIPELINES': {
            'coinla_spider.pipelines.OTCPricePipeline': 300
        },
        'DOWNLOADER_MIDDLEWARES': {
            # 'coinla_spider.middlewares.OTCPriceDownloaderMiddleware': 1,
            # 'coinla_spider.middlewares.OKEXOTCPriceMiddleware': 2,
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'coinla_spider.middlewares.RandomUserAgentMiddleware': 110,
        },
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'DOWNLOAD_DELAY': 0.5,
        'COOKIES_ENABLED': True,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 522, 524, 408, 403],
        'RETRY_TIMES': 2,
        'CLOSESPIDER_TIMEOUT': 60
    }

    def start_requests(self):
        # yield Request('https://www.coincola.com/', callback=self.parse_coincola_index,
        #              meta={'cookiejar': 1})

        for coin_id, ccy_short in [('1', 'BTC'), ('2', 'USDT'), ('3', 'ETH'),
                                   ('4', 'HT'), ('5', 'EOS')]:

            yield Request('https://otc-api.eiijo.cn/v1/otc/trade/list/public?'
                          'country=37&currency=1&payMethod=0&currPage=1&coinId={}&'
                          'tradeType=1&merchant=1'.format(coin_id),
                          meta={'ccy_short': ccy_short}, callback=self.parse_huobi)

        # for ccy_short in ['BTC', 'USDT', 'ETH', 'ETC', 'BCH', 'LTC', 'QTUM', 'NEO', 'EOS']:
        #    yield Request('https://www.okex.com/v3/c2c/tradingOrders/book?side=sell&'
        #                  'baseCurrency={}&quoteCurrency=cny&userType=all&'
        #                  'paymentMethod=all'.format(ccy_short.lower()),
        #                  headers={'Referer': 'https://www.okex.com/otc'},
        #                  meta={'ccy_short': ccy_short}, callback=self.parse_okex)

    def parse_coincola_index(self, response):
        resp_text = response.body.decode(response.encoding)
        csrf = re.findall(r"csrfToken: '(.+?)'", resp_text)[0]
        form_data = {
            'country_code': 'CN',
            'crypto_currency': '',
            'currency': '',
            'limit': '20',
            'offset': '0',
            'sort_order': 'GENERAL',
            'type': 'SELL',
            '_csrf': csrf
        }
        headers = {
            'authorization': 'Bearer',
            'x-user-hash': '895876ab0814fc3dd2d328f072f375a4'
        }
        for short in re.findall(r'"(\w+)":{"CNY"', resp_text):
            form_data.update(crypto_currency=short)
            yield FormRequest('https://www.coincola.com/api/v2/contentslist/list', headers=headers,
                              formdata=form_data, callback=self.parse_coincola, dont_filter=True,
                              meta={'cookiejar': response.meta['cookiejar'], 'ccy_short': short})

    def parse_coincola(self, response):

        ccy_short = response.meta['ccy_short']
        data = json.loads(response.body.decode(response.encoding))
        if not data.get('data') or 'advertisements' not in data['data']:
            self.logger.info(data)
            self.logger.error('CoinCola获取 {} OTC价格出错'.format(ccy_short))
            return None
        sell_list = data['data']['advertisements']
        if len(sell_list) == 0:
            return None
        return OTCPriceItem(
            ccy_short_name=ccy_short,
            otc_price=self.get_avg_price(sell_list)
        )

    def parse_huobi(self, response):
        print(response.url)
        ccy_short = response.meta['ccy_short']
        data = json.loads(response.body.decode(response.encoding))
        if not data.get('data', None):
            self.logger.info(data)
            self.logger.error('Huobi获取{} OTC价格出错'.format(ccy_short))
            return None
        return OTCPriceItem(
            ccy_short_name=ccy_short,
            otc_price=self.get_avg_price(data['data'])
        )

    def parse_okex(self, response):
        ccy_short = response.meta['ccy_short']
        data = json.loads(response.body.decode(response.encoding))
        if not data.get('data', None) or not data['data']['sell']:
            self.logger.info(data)
            self.logger.error('OKEX获取{} OTC价格出错'.format(ccy_short))
            return None
        return OTCPriceItem(
            ccy_short_name=ccy_short,
            otc_price=self.get_avg_price(data['data']['sell'])
        )

    @staticmethod
    def get_avg_price(data, limit=3, price_key='price'):
        sellers = data[:limit]
        return sum(s[price_key] for s in sellers) / len(sellers)
