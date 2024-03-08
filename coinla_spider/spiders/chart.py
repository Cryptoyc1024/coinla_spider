# -*- coding: utf-8 -*-

"""
币种行情图表爬虫，爬取每几分钟和每天的行情数据
并支持计算模式，可以自行计算当天没有爬到数据的币种
"""

import json
import re
from datetime import datetime, timedelta

from scrapy import Spider, Request
from scrapy.exceptions import CloseSpider
from twisted.internet.defer import inlineCallbacks

from coinla_spider.databases.connections import cache, mongo_db
from coinla_spider.databases.getters import \
    get_common_ege_id, get_tocny_exrate
from ..items import ChartItem, QuotationItem


class ChartSpider(Spider):
    name = 'chart'
    custom_settings = {
        'ITEM_PIPELINES': {
            'coinla_spider.pipelines.ChartPipeline': 300
        },
        'DOWNLOADER_MIDDLEWARES': {
            'coinla_spider.middlewares.DummyRequestMiddleware': 1,
            'coinla_spider.middlewares.ChartDownloaderMiddleware': 2,
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'coinla_spider.middlewares.RandomUserAgentMiddleware': 110,
        },
        'RETRY_TIMES': 10,
        'CONCURRENT_ITEMS': 5
    }

    def __init__(self, mode='day', url=None, *a, **kw):
        super(ChartSpider, self).__init__(*a, **kw)
        if mode not in ['day', 'min', 'calc']:
            raise CloseSpider('需要指定`mode`参数：-a mode=min')
        self.crawl_mode = mode
        self.url = url
        self.ccy_ids = set()

    def start_requests(self):
        if self.crawl_mode == 'calc':
            yield Request(':', callback=self.calc_today)
        elif self.url:
            yield Request(self.url, callback=self.parse_detail)
        else:
            yield Request('https://www.feixiaohao.com', callback=self.parse_index)

    def parse_index(self, response):
        for tr in response.xpath('//table[@id="table"]/tbody/tr'):
            detail_url = tr.xpath('./td[2]/a/@href').extract_first()
            yield Request(response.urljoin(detail_url), callback=self.parse_detail)
        # 判断是否有指定的停止页数并达到，或没有下个页数
        page_list = response.xpath('//div[1]/a[contains(@class, "btn btn-white")]/@href').extract()
        if page_list:
            next_page = page_list[-1]
            if next_page != '#':
                yield Request(response.urljoin(next_page), callback=self.parse_index)
            else:
                yield Request(':', callback=self.calc_today)

    def parse_detail(self, response):
        second_part = response.xpath('//div[@class="secondPark"]/ul')
        en_and_short = second_part.xpath('./li[1]/span[2]/text()').extract_first()
        short_name = re.findall(r'.+/(.+)', en_and_short)
        if short_name:
            short_name = short_name[0].strip()
            en_name = en_and_short.replace('/{}'.format(short_name), '').strip()
        else:
            td = response.xpath('//*[@id="markets"]/tbody/tr[1]/td[3]')
            short_name = td.xpath('./text() | ./a/text()').re_first(r'(\S+)/')
            en_name = re.findall(r'(.+)/', en_and_short)[0].strip()
        chart_item = ChartItem(
            ccy_en_name=en_name,
            ccy_short_name=short_name,
            data_list=list()
        )

        step = None
        if self.crawl_mode == 'day':
            chart_item.__collection__ = 'KLineCrawlDay'
        elif self.crawl_mode == 'min':
            chart_item.__collection__ = 'KLineCrawlMinFive'
            step = 3600 * 24 * 7 * 1000

        ccy_code = re.findall(r'/currencies/(.+)/', response.url)[0]
        api_url = 'https://api.feixiaohao.com/coinhisdata/{}'.format(ccy_code)
        return Request(api_url, callback=self.parse_chart, dont_filter=True,
                       meta={'item': chart_item, 'url_base': api_url, 'step': step})

    def parse_chart(self, response):
        item = response.meta['item']
        body = response.body.decode(response.encoding)
        if body:
            body = re.sub(r'(?<=[\[,]),', '[],', body)
            body = re.sub(r',(?=])', ',[]', body)
            data = json.loads(body)
            if data.get('price_usd'):
                if item.get('last_data_time', None) is None:
                    # 第一组数据需要去除每个数组最后一个数值
                    for v in data.values():
                        v.pop()
                    if len(data['price_usd']) == 0:
                        return None
                    item['last_data_time'] = data['price_usd'][-1][0]
                origin = response.meta.get('original_time', 0)
                for _ in range(len(data['price_usd'])):
                    price_usd = data['price_usd'].pop()
                    dt = datetime.fromtimestamp(int(price_usd[0]) / 1000)
                    dt = dt.replace(hour=0, minute=0, second=0)
                    timestamp = int(dt.timestamp()) * 1000
                    if timestamp <= origin:
                        break
                    usd_exrate = response.meta['usd_exrate']
                    price_usd = price_usd[1]
                    volume_usd = data['vol_usd'].pop()[1] if data['vol_usd'][-1] else 0
                    circulate_value_cny = data['market_cap_by_available_supply'].pop()[1]
                    item['data_list'].append({
                        'ccy_id': item['ccy_id'],
                        'ccy_short_name': item['ccy_short_name'],
                        'ccy_en_name': item['ccy_en_name'],
                        'timestamp': timestamp,
                        'price_cny': price_usd * usd_exrate,
                        'price_usd': price_usd,
                        'price_btc': data['price_btc'].pop()[1],
                        'volume_cny': volume_usd * usd_exrate,
                        'volume_usd': volume_usd,
                        'circulate_value_cny': circulate_value_cny * usd_exrate,
                        'circulate_value_usd': circulate_value_cny
                    })
                    self.ccy_ids.add(item['ccy_id'])
                else:
                    if response.meta['step']:
                        return Request(response.meta['url_base'], callback=self.parse_chart,
                                       meta=response.meta, dont_filter=True)
            if item['data_list']:
                return item

    @inlineCallbacks
    def calc_today(self, response):
        common_ege_id = yield get_common_ege_id()
        usd_rate = yield get_tocny_exrate('USD')
        btc_rate = yield get_tocny_exrate('BTC')
        docs = yield mongo_db[QuotationItem.__collection__].find(
            {'ccyId': {'$nin': list(self.ccy_ids)}, 'egeId': common_ege_id, 'type': 0,
             'time': {'$gte': (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')}},
            {'ccyId': 1, 'currencyShortName': 1, 'currencyEnglisgName': 1, 'openPrice': 1,
             'openTurnoverSort': 1, 'openCirculateTotalValueSort': 1, '_id': 0}
        )

        items = list()
        for doc in docs:
            last_data_time = yield cache.load('KLineCrawlDay', doc['ccyId'])
            if last_data_time is not None and \
                    datetime.fromtimestamp(int(last_data_time) / 1000).date() == datetime.now().date():
                continue

            dt = datetime.now().replace(hour=0, minute=0, second=0)
            timestamp = int(dt.timestamp()) * 1000
            items.append(ChartItem(
                ccy_id=doc['ccyId'],
                data_list=[{
                    'ccy_id': doc['ccyId'],
                    'ccy_short_name': doc['currencyShortName'],
                    'ccy_en_name': doc['currencyEnglisgName'],
                    'timestamp': timestamp,
                    'price_cny': doc['openPrice'],
                    'price_usd': doc['openPrice'] / usd_rate,
                    'price_btc': doc['openPrice'] / btc_rate,
                    'volume_cny': doc['openTurnoverSort'],
                    'volume_usd': doc['openTurnoverSort'] / usd_rate,
                    'circulate_value_cny': doc['openCirculateTotalValueSort'],
                    'circulate_value_usd': doc['openCirculateTotalValueSort'] / usd_rate
                }],
                last_data_time=timestamp
            ))
        if items:
            return items
