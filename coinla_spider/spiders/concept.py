# -*- coding: utf-8 -*-

"""
币种板块概念爬虫，爬取最新板块，并关联币种
"""

import json

from scrapy import Spider, Request, FormRequest

from ..items import ConceptItem


class ConceptSpider(Spider):
    name = 'concept'
    custom_settings = {
        'ITEM_PIPELINES': {
            'coinla_spider.pipelines.ConceptPipeline': 300
        },
        'CLOSESPIDER_TIMEOUT': 60
    }

    def start_requests(self):
        yield FormRequest(
            'https://dncapi.feixiaohao.com/api/concept/web-conceptlist',
            formdata={'page': '1', 'webp': '1'},
            method='GET'
        )

    def parse(self, response):
        resp = json.loads(response.body.decode(response.encoding))
        for d in resp['data']:
            yield Request(
                'https://www.feixiaohao.com/conceptcoin/%d/' % int(d['id']),
                callback=self.parse_detail
            )

    def parse_detail(self, response):
        yield ConceptItem(
            concept_name=response.xpath('//div[@class="main"]/h1/text()').extract_first().strip(),
            ccy_urls=[
                response.urljoin(u) for u in response.xpath(
                    '//tbody[@class="ivu-table-tbody"]/tr/td[2]/div/a/@href').extract()
            ]
        )
