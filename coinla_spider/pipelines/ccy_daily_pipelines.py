# -*- coding: utf-8 -*-

from datetime import datetime

from twisted.internet.defer import inlineCallbacks

from .base_pipelines import ConnectionPipeline


class CurrencyDailyDataPipeline(ConnectionPipeline):

    @inlineCallbacks
    def process_item(self, item, spider):
        coll = self._mongo_db[item.__collection__]
        if item.get('doc_id', None) is None:
            item['data_list'].sort(
                key=lambda x: x['date_time'], reverse=True)
            yield coll.insert_one({
                'ccy_id': item['ccy_id'],
                'ccy_short_name': item['ccy_short_name'],
                'data_list': item['data_list'],
                'last_data_time': item['last_data_time'],
                'update_time': datetime.now()
            })
        else:
            yield coll.update_one(
                {
                    '_id': item['doc_id']
                },
                {
                    '$push': {
                        'data': {
                            '$each': item['data_list'],
                            '$sort': {'date_time': -1}
                        }
                    },
                    '$set': {
                        'last_data_time': item['last_data_time'],
                        'update_time': datetime.now()
                    }
                })
        return item
