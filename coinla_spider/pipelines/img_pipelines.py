# -*- coding: utf-8 -*-

from io import BytesIO

from scrapy import Request
from scrapy.pipelines.images import ImagesPipeline

from ..items import CurrencyItem


class CoinlaSpiderImagePipeline(ImagesPipeline):

    def file_path(self, request, response=None, info=None):
        item = request.meta['item']
        return item._pic_name

    def get_media_requests(self, item, info):
        if isinstance(item, CurrencyItem) and \
                getattr(item, '__picName', None) is not None:
            return Request('https:' + item['pic'], meta={'item': item})

    def item_completed(self, results, item, info):
        return item

    def convert_image(self, image, size=None):
        buf = BytesIO()
        image.save(buf, 'PNG')
        return image, buf
