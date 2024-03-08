# -*- coding: utf-8 -*-

import logging

from twisted.internet.defer import inlineCallbacks
from txmongo.errors import TimeExceeded

from coinla_spider.databases.getters import get_ege_id
from coinla_spider.databases.models import Exchange
from .base_pipelines import ConnectionPipeline
from ..items import ExchangeItem


class ExchangePipeline(ConnectionPipeline):

    @inlineCallbacks
    def process_item(self, item, spider):
        print(item)
        if not isinstance(item, ExchangeItem):

            return item

        if item['egeId'] is None:
            field_map = {
                'exchange_name': item['exchangeName'],
                'exchange_name_en': item['exchangeNameEn'],
                'exchange_name_zhhk': item['exchangeName'],
                'country': item['country'],
                'link': item['link'],
                'introduce': item.get('introduce', None),
                'record_status': item['recordStatus']
            }
            yield get_ege_id(item, field_map)

        else:
            self._sql_db.update(Exchange, item['egeId'], {'introduce': item['introduce']})
            try:
                yield self._mongo_db[item.__collection__].update_one(
                    {'egeId': item['egeId']}, {'$set': item}, timeout=20)
            except TimeExceeded as e:
                logging.warning(e)
            except Exception as e:
                logging.error(e)

        return item
