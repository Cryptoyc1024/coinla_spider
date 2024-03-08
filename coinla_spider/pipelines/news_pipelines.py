# -*- coding: utf-8 -*-

from sqlalchemy import or_

from .base_pipelines import ConnectionPipeline
from ..databases.models import HeadNewsLetter


class NewsPipeline(ConnectionPipeline):

    def process_item(self, item, spider):
        filters = {
            HeadNewsLetter.source == item['source'],
            or_(HeadNewsLetter.content == item['content'])
        }
        news = self._sql_db.select(
            HeadNewsLetter,
            filters
        )
        if news is None:
            self._sql_db.insert(HeadNewsLetter, item)
        elif item['proportion'] != 50:
            news.proportion = item['proportion']
        return item
