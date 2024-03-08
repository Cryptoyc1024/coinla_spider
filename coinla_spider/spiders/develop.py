# -*- coding: utf-8 -*-

"""
币种开发进度爬虫，主要是github提交时间和社区互动等一些数据
"""

import json
import re

from scrapy import Spider, Request

from ..items import DevelopDataItem


class DevelopDataSpider(Spider):
    name = 'develop'
    custom_settings = {
        'ITEM_PIPELINES': {
            'coinla_spider.pipelines.DevelopPipeline': 300,
        },
        'DOWNLOADER_MIDDLEWARES': {
            # 'coinla_spider.middlewares.ProxyMiddleware': 750,
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
        },
        'SPIDER_MIDDLEWARES': {
            'coinla_spider.middlewares.DevelopSpiderMiddleware': 2
        },
        'CONCURRENT_REQUESTS': 2,
        'DOWNLOAD_DELAY': 1,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 522, 524, 408, 403, 404],
        'RETRY_TIMES': 2,
        'CLOSESPIDER_TIMEOUT': 7200
    }

    def detail_request(self, code_id):
        return Request('https://api.finbtc.net/app//coin/detail/fx?coinId=%d' % code_id,
                       header={
			                'X-App-Info': '4.2.0/baidu/android',
                           'device': 'a3939777-6de2-442b-ad58-322a5b33365e',
                           'User-Agent': 'OPPO_OPPO_R9s_Plus',
                           'Host': 'api.finbtc.net',
                           'Connection': 'Keep-Alive',
                           'Accept-Encoding': 'gzip',
                           'If-Modified-Since': 'Mon, 13 May 2019 08:06:04',
                       },
                       meta={'code_id': code_id})

    def start_requests(self):
        yield self.detail_request(1)

    def parse(self, response):
        body = response.body.decode(response.encoding)
        data = json.loads(body)['data']
        code_id = response.meta['code_id']
        if not data:
            if code_id <= 20:
                self.logger.error('DevelopDataSpider 返回异常')
            return None
        else:
            indicator = data['indicator']
            item = DevelopDataItem(
                holder_address={'total': self.extract_digit(indicator['holdersAddCnt'])},
                github_newest_time=indicator['newestCommitTime'] if re.match(
                    r'\d{4}', indicator['newestCommitTime']) else '',
                github_commit={'total': self.extract_digit(indicator['totalCommit'])},
                github_contributor={'total': self.extract_digit(indicator['contributorCount'])},
                github_follow={'total': self.extract_digit(indicator['followCount'])},
                github_star={'total': self.extract_digit(indicator['fansCount'])},
                github_fork={'total': self.extract_digit(indicator['copyCount'])},
                reddit_sub={'total': self.extract_digit(indicator['redditSubCount'])},
                twitter_follow={'total': self.extract_digit(indicator['twitterFollowerCount'])},
                facebook_thumb={'total': self.extract_digit(indicator['facebookThumbCount'])}
            )
            item._ccy_short_name = data['market']['trends'][0]['symbol']
            yield item
            yield self.detail_request(code_id + 1)

    @staticmethod
    def extract_digit(string):
        digits = re.findall(r'^\d+', string)
        return int(digits[0]) if digits else 0
