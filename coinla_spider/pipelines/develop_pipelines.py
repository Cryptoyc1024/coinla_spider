# -*- coding: utf-8 -*-

from twisted.internet.defer import inlineCallbacks

from .base_pipelines import ConnectionPipeline
from ..items import DevelopDataItem


class DevelopPipeline(ConnectionPipeline):

    def __init__(self, crawler):
        super(DevelopPipeline, self).__init__(crawler)
        self.embedded_fields = self.get_embedded_fields()

    @inlineCallbacks
    def process_item(self, item, spider):
        # 查询Mongo并处理成和Item一样的数据结构
        filter_fields = {k: 1 for k in self.embedded_fields}
        filter_fields.update(_id=0)
        doc = yield self._mongo_db[item.__collection__].find_one(
            {'ccy_id': item['ccy_id']}, filter_fields)
        if doc is None:
            return item
        # Mongo文档字段足够则进行趋势计算
        if len(doc.keys()) >= len(self.embedded_fields):
            for field in self.embedded_fields:
                if item[field]['total'] > doc[field]['total']:
                    item[field]['trend'] = 1
                elif item[field]['total'] < doc[field]['total']:
                    item[field]['trend'] = -1
                else:
                    item[field]['trend'] = 0
            # 如果趋势均是0，则不进行更新
            if len(set(item[k]['total'] for k in item.keys()
                       if k in self.embedded_fields)) == 1:
                return item
        yield self._mongo_db[item.__collection__].update_many(
            {'ccy_id': item['ccy_id']}, {'$set': item}, timeout=20)
        return item

    @staticmethod
    def get_embedded_fields():
        """ 获得Item里有内嵌文档的字段 """
        fields = list()
        for k, v in DevelopDataItem.fields.items():
            if isinstance(v.get('default'), dict) and 'trend' in v['default']:
                fields.append(k)
        return fields
