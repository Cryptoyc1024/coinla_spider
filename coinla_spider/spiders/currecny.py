# -*- coding: utf-8 -*-

"""
币种爬虫，爬取更新币种基础信息，
并自行计算币种流通值、市值等一些统计数据
"""

import json
import re
from datetime import datetime

from scrapy import Spider, Request, FormRequest

from ..formatters import Float
from ..items import CurrencyItem, CurrencyCoreDataItem


class CurrencySpider(Spider):
    name = 'currency'
    custom_settings = {
        # 'ITEM_PIPELINES': {
        #     'coinla_spider.pipelines.CurrencyPipeline': 300,
        #     'coinla_spider.pipelines.CurrencyCoreDataPipeline': 302,
        # },
        'DOWNLOADER_MIDDLEWARES': {
            'coinla_spider.middlewares.CurrencyDownloaderMiddleware': 1,
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'coinla_spider.middlewares.RandomUserAgentMiddleware': 110,
        },
        'SPIDER_MIDDLEWARES': {
            'coinla_spider.middlewares.CurrencySpiderMiddleware': 1,
        },
        'CONCURRENT_REQUESTS': 8,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 4,
        'RETRY_TIMES': 9,
        'CLOSESPIDER_TIMEOUT': 3600
    }
    url_base = 'https://www.feixiaohao.com'

    def __init__(self, page=None, *a, **kw):
        super(CurrencySpider, self).__init__(*a, **kw)
        if page is not None:
            self.start_page, self.end_page = page.split(',')

    def index_request(self, page):
        return FormRequest(
            'https://dncapi.bqiapp.com/api/coin/coinrank',
            formdata={
                'page': str(page),
                'pagesize': '100',
                'type': '0',
                'webp': '1',
            },
            method='GET',
            callback=self.parse,
            meta={'page': int(page)},
            headers={'Referer': 'https://www.feixiaohao.com/list_%s.html' % page}
        )

    def start_requests(self):
        start_page = getattr(self, 'start_page', '1')
        yield self.index_request(start_page)

    def parse(self, response):
        resp = json.loads(response.body.decode(response.encoding))
        for d in resp['data']:
            detail_url = self.url_base + '/currencies/%s/' % d['code']
            yield Request(detail_url, callback=self.parse_detail,
                          meta={'url': detail_url})
        # 判断当前页是否是最后一页
        current_page = response.meta['page']
        last_page = int(resp['maxpage'])
        if current_page < last_page and current_page != int(getattr(self, 'end_page', 0)):
            next_page = current_page + 1
            yield self.index_request(next_page)
        # else:
        #     yield Request('https://api.schail.com/v3/ticker/summary?'
        #                   'limit=5000&offset=0&sort=1&top=5000&type=0',
        #                   headers={'Referer': 'http://www.tokenclub.com/'},
        #                   callback=self.parse_tokenclub_code)

    def parse_detail(self, response):
        header_path = response.xpath('//div[@class="box coinInfoHeader"]')
        ccy_id = response.meta['ccy_id']
        name_path = header_path.xpath('./div[@class="title"]/div[1]/h1')
        en_name = name_path.xpath('./small/text()').extract_first().strip()
        short_and_cn_name = name_path.xpath('./text()').extract_first().strip()
        if ',' in short_and_cn_name:
            short_name, cn_name = short_and_cn_name.split(',')
            if not cn_name:
                cn_name = en_name
        else:
            short_name = short_and_cn_name
            cn_name = en_name

        # ----------------------------------  新增币种字段  ----------------------------------

        info_path = response.xpath('//div[@class="box infoDetal"]/div/div[3]')
        ccy_item = CurrencyItem(ccyId=ccy_id)
        if ccy_id is None:
            white_paper = info_path.xpath(
                './div[3]/div/div[1]/div[2]/span[@class="val"]/a/@href').extract_first()
            ccy_item.update(
                currencyName=cn_name,
                english=en_name,
                shortName=short_name,
                # 发行时间
                initiateCreateDate=info_path.xpath(
                    './div[1]//div[@class="listRow"][1]/div[1]/span[2]/text()').re_first('^\d.+'),
                # 官网
                guanw=info_path.xpath('./div[3]/div/div[1]/div[1]/span[2]/a[1]/@href').extract_first(),
                # 区块站
                blockChain=info_path.xpath(
                    './div[3]/div/div[2]/div[1]/span[2]/a[1]/@href').extract_first(),
                # 白皮书
                whitePaperEn=white_paper,
                whitePaperZh=white_paper,
                recordStatus=0
            )
            ccy_item._url = response.meta['url']

            yield ccy_item

        else:
            # ----------------------------------  更新币种字段  ----------------------------------

            float_fmt = Float()
            circulate_total = float_fmt.format(header_path.xpath(
                './div[2]/div[2]/div[2]/text()').re_first(r'^\d.+,\d{3}', default='0'))
            total = float_fmt.format(info_path.xpath(
                './div[1]//div[@class="listRow"][2]/div[1]/span[2]/text()').re_first(r'[\d,]+', default='0'))
            ccy_item._circulate_total = circulate_total
            ccy_item._total = total
            ccy_item = self._update_ccy_item(response, ccy_item)

            # ----------------------------------  币种核心数据  ----------------------------------

            core_items = self._get_core_items(response, ccy_item)
            code = re.findall(r'/currencies/(.+)/', response.url)[0]
            yield FormRequest('https://dncapi.bqiapp.com/api/coin/cointrades-web',
                              formdata={'code': code, 'webp': '1'},
                              method='GET',
                              callback=self.parse_pair_chart,
                              meta={'items': core_items})
            yield ccy_item

    def parse_pair_chart(self, response):
        items = response.meta['items']
        item = items[0]
        if response.body:
            resp = json.loads(response.body.decode(response.encoding))
            for d in resp['data']:
                name = re.search(r'[^\s\d.%]+', d['name'])
                if name:
                    item['pair_ratio'].append({
                        'base': name[0].upper() if name[0] != 'other' else '其他',
                        'ratio': round(float(d['percent']), 2) if float(d['percent']) > 0 else 0
                    })
            item['pair_ratio'].sort(key=lambda x: x['ratio'], reverse=True)
            item['pair_ratio'].sort(key=lambda x: '其他' in x['base'])
        return items

    # ===================================  TokenClub 数据源  ===================================

    def parse_tokenclub_code(self, response):
        data = json.loads(response.body.decode(response.encoding))
        for each in data['data']['summaryList']:
            yield Request('https://api.schail.com/v1/source/coin/get?coin={}'.format(each['tickerId']),
                          callback=self.parse_tokenclub_detail, priority=-1)

    def parse_tokenclub_detail(self, response):
        data = json.loads(response.body.decode(response.encoding))['data']
        ccy_id = response.meta['ccy_id']

        # ----------------------------------  新增币种字段  ----------------------------------

        ccy_item = CurrencyItem(ccyId=ccy_id)
        if ccy_id is None:
            ccy_item.update(
                currencyName=data['zhName'] if data.get('zhName') else data['name'],
                english=data['name'],
                shortName=data['symbol'],
                # 发行时间
                initiateCreateDate=datetime.fromtimestamp(
                    int(data['publicTime']) / 1000).strftime(
                    '%Y-%m-%d') if data.get('publicTime') else None,
                # 官网
                guanw=data['websites'][0] if data['websites'] else None,
                # 区块站
                blockChain=data.get('Explorer', ''),
                # 白皮书
                whitePaperEn=data.get('whitepaper', ''),
                whitePaperZh=data.get('whitepaper', ''),
                recordStatus=0
            )
            ccy_item._url = response.meta['url']
            yield ccy_item

        else:
            # ----------------------------------  更新币种字段  ----------------------------------

            total = float(data['supple']) if data.get('supple') else 0
            circulate_total = float(data['available_supply']) if data.get('available_supply') else 0
            ccy_item._circulate_total = circulate_total
            ccy_item._total = total
            ccy_item = self._update_ccy_item(response, ccy_item)

            # ----------------------------------  币种核心数据  ----------------------------------

            core_items = self._get_core_items(response, ccy_item)
            if data.get('pairData'):
                pair_turnover_total = sum(float(i['y']) for i in data['pairData'])
                for d in data['pairData']:
                    ratio = float(d['y']) / pair_turnover_total
                    core_items[0]['pair_ratio'].append({
                        'base': d['name'] if d['name'] != 'other' else '其他',
                        'ratio': round(ratio, 2) if ratio > 0 else 0
                    })
            yield ccy_item
            return core_items

    def _update_ccy_item(self, response, ccy_item):
        price_cny = response.meta['price_cny']
        price_usd = response.meta['price_usd']
        price_btc = response.meta['price_btc']
        circulate_total = ccy_item._circulate_total
        total = ccy_item._total
        circulate_cny = price_cny * circulate_total
        circulate_usd = price_usd * circulate_total
        circulate_btc = price_btc * circulate_total
        total_value_cny = price_cny * total
        total_value_usd = price_usd * total
        total_value_btc = price_btc * total
        ccy_item.update(
            circulateTotal=circulate_total,
            # 流通市值
            circulateTotalValue=circulate_cny,
            sortCirculateTotalValue=circulate_cny,
            # 总发行量
            total=total,
            # 总市值
            totalValue=total_value_cny,
            sortTotalValue=total_value_cny,
        )
        # 向Pipeline传递部分字段的原始值
        ccy_item._circulate_usd = circulate_usd
        ccy_item._circulate_btc = circulate_btc
        ccy_item._total_value_usd = total_value_usd
        ccy_item._total_value_btc = total_value_btc
        return ccy_item

    def _get_core_items(self, response, ccy_item):
        circ_value_total = self.crawler.stats.get_value('circ_value_total')
        circulate_cny = ccy_item['sortCirculateTotalValue']
        core_item_cny = CurrencyCoreDataItem(
            ccy_id=ccy_item['ccyId'],
            type=0,
            circulate_ratio=ccy_item._circulate_total / ccy_item._total * 100 if ccy_item._total else 0,
            turnover_ratio_24h=response.meta['turnover'] / circulate_cny * 100 if circulate_cny else 0,
            total_value=ccy_item['sortTotalValue'],
            sort_total_value=ccy_item['sortTotalValue'],
            total_value_ratio=circulate_cny / circ_value_total * 100 if circ_value_total else 0,
            circulate_total=ccy_item._circulate_total,
            sort_circulate_total=ccy_item._circulate_total,
            circulate_total_value=circulate_cny,
            sort_circulate_total_value=circulate_cny,
            pair_ratio=list()
        )
        core_item_cny._circulate_total_value = circulate_cny

        core_item_usd = core_item_cny.copy()
        core_item_usd.update(
            type=1,
            total_value=ccy_item._total_value_usd,
            sort_total_value=ccy_item._total_value_usd,
            circulate_total_value=ccy_item._circulate_usd,
            sort_circulate_total_value=ccy_item._circulate_usd
        )

        core_item_btc = core_item_cny.copy()
        core_item_btc.update(
            type=2,
            total_value=ccy_item._total_value_btc,
            sort_total_value=ccy_item._total_value_btc,
            circulate_total_value=ccy_item._circulate_btc,
            sort_circulate_total_value=ccy_item._circulate_btc,
        )
        return [core_item_cny, core_item_usd, core_item_btc]
