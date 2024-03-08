# -*- coding: utf-8 -*-

"""
缓存 币种名与币种ID 和 交易所名与交易所ID
用于爬虫快速查询到ID
"""

import os
import sys

current_url = os.path.dirname(__file__)
parent_url = os.path.abspath(os.path.join(current_url, os.pardir))
sys.path.append(parent_url)

from jobs import sql_session, cache_cli
from coinla_spider.databases.models import CurrencyName, Exchange


def cache_ccy_name():
    """ 将币种名重新进行缓存 """
    with cache_cli.pipeline(transaction=False) as pipe:
        keys = cache_cli.keys('SpiderCache:Currency:*')
        pipe.delete(*keys)
        sql_db = sql_session()
        flt = {
            CurrencyName.ege_id == 0,
            CurrencyName.ccy_id > 0,
        }
        cols = sql_db.query(
            CurrencyName.short_name, CurrencyName.english,
            CurrencyName.ccy_id).filter(*flt).all()
        list(map(lambda x: pipe.set(
            'SpiderCache:Currency:{}-{}'.format(
                x[0], x[1]), x[2]), cols))
        pipe.execute()


def cache_ege_name():
    """ 将交易所名重新进行缓存 """
    with cache_cli.pipeline(transaction=False) as pipe:
        keys = cache_cli.keys('SpiderCache:Exchange:*')
        pipe.delete(*keys)
        sql_db = sql_session()
        cols = sql_db.query(
            Exchange.exchange_name, Exchange.exchange_name_en,
            Exchange.id).all()
        list(map(lambda x: pipe.set(
            'SpiderCache:Exchange:{}-{}'.format(
                x[0], x[1]), x[2]), cols))
        pipe.execute()


def cache_sync():
    cache_ccy_name()
    cache_ege_name()


if __name__ == '__main__':
    cache_sync()
