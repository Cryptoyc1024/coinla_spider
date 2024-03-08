# -*- coding: utf-8 -*-

from sqlalchemy.exc import DataError
from twisted.internet.defer import inlineCallbacks

from .base_pipelines import ConnectionPipeline
from ..databases.models import HeadNotice


class NoticePipeline(ConnectionPipeline):

    @inlineCallbacks
    def process_item(self, item, spider):
        doc = self._sql_db.select(
            HeadNotice,
            {HeadNotice.original_link == item['original_link']}
        )
        if doc is None:
            try:
                self._sql_db.insert(HeadNotice, item)
            except DataError:
                pass
        yield self._cache.save(
            'Notice', item['original_link'], item['notice_title'])
        return item
