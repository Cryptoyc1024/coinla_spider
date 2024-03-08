# -*- coding: utf-8 -*-

import json
import re
from datetime import datetime

from scrapy.spiders import Spider, Request

from ..items import NewsItem


class NewsSpider(Spider):
    name = 'news'
    custom_settings = {
        'ITEM_PIPELINES': {
            'coinla_spider.pipelines.NewsPipeline': 300
        },
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'coinla_spider.middlewares.RandomUserAgentMiddleware': 110,
        },
        'CONCURRENT_REQUESTS_PER_DOMAIN': 2,
        'DOWNLOAD_DELAY': 0.3,
        'CLOSESPIDER_TIMEOUT': 120,
    }

    def start_requests(self):
        yield Request('http://api.coindog.com/live/list',
                      callback=self.parse_jinse)
        yield Request('http://www.xnqb.com/news/newslist.html',
                      callback=self.parse_xiaoniu)

    def parse_jinse(self, response):
        data = json.loads(response.body.decode(response.encoding))
        for each in data['list'][0]['lives']:
            text = re.findall(r'【(.+?)】(.+)', each['content'])
            if '|' in text[0][0]:
                text = re.findall(r'【(.+?)】(.+)', each['content'])
                news_type, title = text[0][0].split('|')
            else:
                news_type, title = '', text[0][0].strip()
            # 跳过一些含有关键词的快讯
            if news_type and '行情' in news_type:
                continue
            for w in ['金色盘面']:
                if w in title:
                    break
            else:
                good = int(each['up_counts'])
                bad = int(each['down_counts'])
                yield NewsItem(
                    title=title,
                    content=text[0][1],
                    show_date=datetime.fromtimestamp(int(each['created_at'])),
                    source='https://www.jinse.com/lives/{}.htm'.format(each['id']),
                    proportion=round(good / (good + bad) * 100, 2) if good + bad >= 10 else 50
                )

    def parse_xiaoniu(self, response):
        for row in response.xpath('//div[@class="layui-timeline-content layui-clear"]'):
            title = row.xpath('./h2/a/text()').extract_first()
            if '资金流向' in title:
                continue
            content = row.xpath('string(./div[@class="qb_info"])').extract_first()
            date_str = row.xpath('string(./h3)').extract_first()
            news_date = datetime.strptime(date_str, '%m月%d日 %H:%M')
            good = int(row.xpath('.//*[@class="qb_con_kd"]/@num').extract_first())
            bad = int(row.xpath('.//*[@class="qb_con_kk"]/@num').extract_first())
            yield NewsItem(
                title=title,
                content=content,
                show_date=news_date.replace(year=datetime.now().year),
                source=response.urljoin(row.xpath('./h2/a/@href').extract_first()),
                proportion=round(good / (good + bad) * 100, 2) if good + bad >= 10 else 50
            )
