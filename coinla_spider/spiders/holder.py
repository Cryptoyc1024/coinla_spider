# -*- coding: utf-8 -*-

"""
币种持有量的爬虫，爬取主流区块浏览器，汇总成币种的富豪榜数据
"""

import json
import re

from scrapy import FormRequest
from scrapy.spiders import Spider, Request

from ..formatters import Float
from ..items import HolderItem


class HolderSpider(Spider):
    name = 'holder'
    custom_settings = {
        'ITEM_PIPELINES': {
            'coinla_spider.pipelines.HolderPipeline': 300
        },
        'DOWNLOADER_MIDDLEWARES': {
            'coinla_spider.middlewares.GetCcyIdMiddleware': 1,
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'coinla_spider.middlewares.RandomUserAgentMiddleware': 110,
        },
        'SPIDER_MIDDLEWARES': {
            'coinla_spider.middlewares.HolderSpiderMiddleware': 2
        },
        'CONCURRENT_REQUESTS': 32,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 4,
        'DOWNLOAD_DELAY': 0.3,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 522, 524, 408, 403],
        'RETRY_TIMES': 10,
        'CLOSESPIDER_TIMEOUT': 1200,
        'REDIRECT_ENABLED': True
    }

    def start_requests(self):
        yield Request('https://btc.com/stats/rich-list', callback=self.parse_btc,
                      meta={'ccy_short_name': 'BTC', 'log': True})
        yield Request('https://etherscan.io/accounts', callback=self.parse_eth,
                      meta={'ccy_short_name': 'ETH', 'ccy_en_name': 'Ethereum', 'log': True})
        yield Request('https://bch.btc.com/stats/rich-list', callback=self.parse_btc,
                      meta={'ccy_short_name': 'BCH', 'ccy_en_name': 'Bitcoin Cash(BCC)', 'log': True})
        yield Request('https://etherscan.io/tokens', callback=self.parse_eth_list)
        yield Request('https://chainz.cryptoid.info/stats.dws', callback=self.parse_cryptoid_list)
        yield Request('https://cryptobe.com/', callback=self.parse_cryptobe_list)
        yield Request('http://explorer.nemchina.com/account/accountList', callback=self.parse_xem,
                      method='POST', meta={'ccy_short_name': 'XEM', 'log': True})
        yield Request('https://explorer.lisk.io/api/getTopAccounts?limit=51&offset=0',
                      callback=self.parse_lsk,
                      meta={'ccy_short_name': 'LSK', 'ccy_en_name': 'Lisk', 'log': True})
        yield Request('https://qtum.info/misc/rich-list', callback=self.parse_qtum,
                      meta={'ccy_short_name': 'QTUM', 'log': True})
        yield Request('https://verge-blockchain.info/richlist', callback=self.parse_xvg,
                      meta={'ccy_short_name': 'XVG', 'ccy_en_name': 'Verge'})
        yield Request('https://explorer.ark.io:8443/api/accounts/top?'
                      'orderBy=balance:desc&limit=25&offset=0',
                      callback=self.parse_ark,
                      meta={'ccy_short_name': 'ARK'})
        yield Request('https://blockchain.elastos.org/api/v1/addrs/richest-list/',
                      callback=self.parse_ela,
                      meta={'ccy_short_name': 'ELA', 'ccy_en_name': 'Elastos'})
        yield Request('http://www.presstab.pw/phpexplorer/PIVX/richlist.php',
                      callback=self.parse_pivx,
                      meta={'ccy_short_name': 'PIVX'})
        yield Request('https://explorer.skycoin.net/api/richlist', callback=self.parse_sky,
                      meta={'ccy_short_name': 'SKY', 'ccy_en_name': 'Skycoin'})
        yield Request('http://explorer.nebl.io/richlist', callback=self.parse_nebl,
                      meta={'ccy_short_name': 'NEBL', 'ccy_en_name': 'Neblio'})
        for code, short_name, en_name in [('dash', 'DASH', '*'), ('dogecoin', 'DOGE', 'Dogecoin'),
                                          ('reddcoin', 'RDD', 'ReddCoin'), ('vertcoin', 'VTC', 'Vertcoin'),
                                          ('namecoin', 'NMC', 'Namecoin'), ('auroracoin', 'AUR', 'Auroracoin'),
                                          ('novacoin', 'NVC', 'Novacoin')]:
            yield Request('https://bitinfocharts.com/zh/top-100-richest-{}-addresses.html'.format(code),
                          callback=self.parse_bitinfocharts,
                          meta={'ccy_short_name': short_name, 'ccy_en_name': en_name})
        yield FormRequest('https://bosradar.com/ajax.php', formdata={'action': 'richlist', 'page': '1'},
                          headers={'Referer': 'https://bosradar.com'}, callback=self.parse_bos,
                          meta={'ccy_short_name': 'BOS', 'ccy_en_name': 'BOScoin'})
        yield Request('https://xsnexplorer.io/api/balances?offset=0&limit=50&orderBy=available:desc',
                      callback=self.parse_xsn,
                      meta={'ccy_short_name': 'XSN'})

    def parse_btc(self, response):
        for tr in response.xpath('//table[@class="table"]/tr')[1:51]:
            item = HolderItem(
                ccy_id=response.meta['ccy_id'],
                rank=tr.xpath('./td[1]/text()').re_first(r'\d+'),
                address=tr.xpath('./td[2]/span/a/@href').re_first(r'com/([^\s(\n)]+)'),
                balance=tr.xpath('string(./td[3])').re_first(r'[\d\.,]+'),
            )
            yield item

    def parse_eth(self, response):
        for tr in response.xpath('//table[@class="table table-hover "]/tbody/tr'):
            yield HolderItem(
                ccy_id=response.meta['ccy_id'],
                rank=tr.xpath('./td[1]/text()').re_first(r'\d+'),
                address=tr.xpath('./td[3]/a/text()').re_first(r'\S+'),
                balance=tr.xpath('string(./td[5])').re_first(r'[\d\.,]+'),
                share=tr.xpath('./td[6]/text()').re_first(r'[\d\.,]+%')
            )

    def parse_eth_list(self, response):
        for tr in response.xpath('//div/table/tbody/tr'):
            href = tr.xpath('./td[2]/h3/a/@href').extract_first()
            ccy_short_name = tr.xpath('./td[2]/h3/a/text()').re_first(r'\((.+)\)')
            yield Request(response.urljoin(href), callback=self.parse_eth_holder_url,
                          meta={'ccy_short_name': ccy_short_name})
        next_page = response.xpath('//*[@id="ContentPlaceHolder1_divPagination"]/ul/li[4]/a/@href').extract_first()
        if not next_page == None:
            if '#' not in next_page:
                yield Request(response.urljoin(next_page), callback=self.parse_eth_list)

    def parse_eth_holder_url():
        url = re.findall(r"'(/token/generic-tokenholders2\?a=.+)'",
                         response.body.decode(response.encoding))
        if not url:
            return None
        return Request(response.urljoin(url[0]), callback=self.parse_eth_holders,
                       meta={'ccy_id': response.meta['ccy_id']})

    def parse_eth_holders(self, response):
        if 'no matching' in response.body.decode(response.encoding):
            return None
        count = response.meta.get('count', 0) + 1
        for idx, tr in enumerate(response.xpath('//table[@class="table"]/tr')[1:]):
            if count > 50:
                break
            share = Float().format(tr.xpath('./td[4]/text()').re_first(r'[\d\.,]+%'))
            if share < 100:
                yield HolderItem(
                    ccy_id=response.meta['ccy_id'],
                    rank=count,
                    address=tr.xpath('./td[2]/span/a/text()').re_first(r'\S+'),
                    balance=tr.xpath('./td[3]/text()').re_first(r'[\d\.,]+'),
                    share=tr.xpath('./td[4]/text()').re_first(r'[\d\.,]+%')
                )
                count += 1
        if count < 50:
            next_page = response.xpath('//*[@id="PagingPanel"]/span/a[3]/@href').extract_first()
            if next_page and next_page != '#':
                url = re.findall(r"javascript:move\('(.+)'\)", next_page)
                yield Request(response.urljoin('/token/' + url[0]),
                              callback=self.parse_eth_holders,
                              meta={'ccy_id': response.meta['ccy_id'], 'count': count})

    def parse_cryptoid_list(self, response):
        data = json.loads(response.body.decode(response.encoding))
        for d in data:
            if d:
                yield Request('https://chainz.cryptoid.info/{}/'.format(d[0]),
                              callback=self.parse_cryptoid_circulate,
                              meta={'ccy_short_name': d[0].upper()})

    def parse_cryptoid_circulate(self, response):
        coin_arg = re.findall(r'info/(.+?)/', response.url)
        if not coin_arg:
            return None
        circulate = response.xpath(
            '//script[last()-1]/text()').re_first(r'lastOutstanding = ([\d\.,]+)', default=0)
        return Request('https://chainz.cryptoid.info/explorer/'
                       'index.stats.dws?coin={}'.format(coin_arg[0]),
                       callback=self.parse_cryptoid_holders,
                       meta={'ccy_id': response.meta['ccy_id'],
                             'circulate': float(circulate)})

    def parse_cryptoid_holders(self, response):
        data = json.loads(response.body.decode(response.encoding))
        circulate = response.meta['circulate']
        for i, d in enumerate(data['largestAddresses'][:50]):
            yield HolderItem(
                ccy_id=response.meta['ccy_id'],
                rank=i + 1,
                address=d['addr'],
                balance=d['amount'],
                share=float(d['amount']) / circulate * 100 if circulate != 0 else None
            )

    def parse_cryptobe_list(self, response):
        for tr in response.xpath('//table[@class="maintable"]/tr')[1:]:
            ccy_short_name = tr.xpath('./td[2]/text()').re_first(r'\S+')
            ccy_name = tr.xpath('./td[1]/a/@href').re_first(r'chain/(.+)')
            yield Request(response.urljoin(ccy_name),
                          callback=self.parse_cryptobe_holders,
                          meta={'ccy_short_name': ccy_short_name})

    def parse_cryptobe_holders(self, response):
        if 'not listed' in response.body.decode(response.encoding):
            return None
        for tr in response.xpath('//table[@class="richlist"]/tr')[1:51]:
            yield HolderItem(
                ccy_id=response.meta['ccy_id'],
                rank=tr.xpath('./td[1]/text()').re_first(r'\d+'),
                address=tr.xpath('./td[2]/text()').re_first(r'\S+'),
                balance=tr.xpath('./td[3]/text()').re_first(r'[\d\.,]+'),
                share=tr.xpath('./td[4]/text()').re_first(r'[\d\.,]+%')
            )

    def parse_xem(self, response):
        data = json.loads(response.body.decode(response.encoding))
        for i, d in enumerate(data[:50]):
            yield HolderItem(
                ccy_id=response.meta['ccy_id'],
                rank=i + 1,
                address=d['address'],
                balance=float(d['balance']) / 1e6,
                share=float(d['importance']) * 100
            )

    def parse_lsk(self, response):
        data = json.loads(response.body.decode(response.encoding))
        for i, d in enumerate(data['accounts'][:50]):
            yield HolderItem(
                ccy_id=response.meta['ccy_id'],
                rank=i + 1,
                address=d['address'],
                balance=float(d['balance']) / 1e8,
            )

    def parse_qtum(self, response):
        for tr in response.xpath('//tbody/tr')[:50]:
            yield HolderItem(
                ccy_id=response.meta['ccy_id'],
                rank=tr.xpath('./td[1]/text()').re_first(r'\d+'),
                address=tr.xpath('./td[2]/span/a/text()').re_first(r'\S+'),
                balance=tr.xpath('./td[3]/text()').re_first(r'[\d,\.]+'),
                share=tr.xpath('./td[4]/text()').re_first(r'[\d,\.]+%'),
            )

    def parse_xvg(self, response):
        for tr in response.xpath('//*[@id="balance"]/div/table/tbody/tr')[:50]:
            yield HolderItem(
                ccy_id=response.meta['ccy_id'],
                rank=tr.xpath('./td[1]/text()').re_first(r'\d+'),
                address=tr.xpath('./td[2]/a/text()').re_first(r'\S+'),
                balance=tr.xpath('./td[3]/text()').re_first(r'[\d,\.]+'),
                share=tr.xpath('./td[4]/text()').re_first(r'[\d,\.]+'),
            )

    def parse_ark(self, response):
        data = json.loads(response.body.decode(response.encoding))
        for i, d in enumerate(data['accounts'][:50]):
            yield HolderItem(
                ccy_id=response.meta['ccy_id'],
                rank=i + 1,
                address=d['address'],
                balance=float(d['balance']) / 1e8,
            )

    def parse_ela(self, response):
        data = json.loads(response.body.decode(response.encoding))
        for i, d in enumerate(data['info'][:50]):
            yield HolderItem(
                ccy_id=response.meta['ccy_id'],
                rank=i + 1,
                address=d['address'],
                balance=d['balance'],
                share=d['percentage']
            )

    def parse_pivx(self, response):
        for tr in response.xpath('//table[@class="hovertable"]/tr')[1:51]:
            yield HolderItem(
                ccy_id=response.meta['ccy_id'],
                rank=tr.xpath('./td[1]/text()').re_first(r'\d+'),
                address=tr.xpath('./td[3]/a/text()').re_first(r'\S+'),
                balance=tr.xpath('./td[4]/text()').re_first(r'[\d,\.]+'),
                share=tr.xpath('./td[6]/text()').re_first(r'[\d,\.]+'),
            )

    def parse_sky(self, response):
        data = json.loads(response.body.decode(response.encoding))
        for i, d in enumerate(data['richlist'][:50]):
            yield HolderItem(
                ccy_id=response.meta['ccy_id'],
                rank=i + 1,
                address=d['address'],
                balance=d['coins']
            )

    def parse_nebl(self, response):
        for tr in response.xpath('//table[@cellspacing="0"]/tbody/tr')[2:52]:
            yield HolderItem(
                ccy_id=response.meta['ccy_id'],
                rank=tr.xpath('./td[1]/text()').re_first(r'\d+'),
                address=tr.xpath('./td[2]/a/text()').re_first(r'\S+'),
                balance=tr.xpath('./td[3]/text()').re_first(r'[\d,\.]+'),
                share=tr.xpath('./td[4]/text()').re_first(r'[\d,\.]+'),
            )

    def parse_bitinfocharts(self, response):
        for tr in response.xpath('//table[@id="tblOne"]/tbody/tr')[:50]:
            yield HolderItem(
                ccy_id=response.meta['ccy_id'],
                rank=tr.xpath('./td[1]/text()').re_first(r'\d+'),
                address=tr.xpath('./td[2]/a/text()').re_first(r'\S+'),
                balance=tr.xpath('./td[3]/text()').re_first(r'[\d,\.]+'),
                share=tr.xpath('./td[4]/text()').re_first(r'[\d,\.]+'),
            )

    def parse_bos(self, response):
        data = json.loads(response.body.decode(response.encoding))
        for i, d in enumerate(data['richList'][:50]):
            yield HolderItem(
                ccy_id=response.meta['ccy_id'],
                rank=i + 1,
                address=d['id'],
                balance=d['balance'],
                share=d['percentage']
            )

    def parse_xsn(self, response):
        data = json.loads(response.body.decode(response.encoding))
        for i, d in enumerate(data['data'][:50]):
            yield HolderItem(
                ccy_id=response.meta['ccy_id'],
                rank=i + 1,
                address=d['address'],
                balance=d['available'],
            )

# https://neotracker.io/browse/asset/1
