# -*- coding: utf-8 -*-

import logging

from pymongo.errors import DuplicateKeyError, OperationFailure
from twisted.internet.defer import inlineCallbacks

from .base_pipelines import ConnectionPipeline
from ..items import ChartItem


class ChartPipeline(ConnectionPipeline):

    @inlineCallbacks
    def process_item(self, item, spider):
        if not isinstance(item, ChartItem):
            return item

        coll = self._mongo_db[item.__collection__]
        item['data_list'].sort(key=lambda x: x['timestamp'])
        try:
            yield coll.insert(item['data_list'], safe=True, ordered=True,
                              continue_on_error=True)
        except DuplicateKeyError:
            pass
        except OperationFailure as e:
            logging.error(e)
        if spider.name == 'chart':
            yield self._cache.save(
                item.__collection__, item['ccy_id'], item['last_data_time'])
        return item
