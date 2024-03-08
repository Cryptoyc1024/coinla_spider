# -*- coding: utf-8 -*-

"""
合并指定的 币种或交易所 的交易对趋势图数据
用于特殊情况出现两个相同的交易对，结果在两个交易对ID里的情况
"""

import os
import sys

current_url = os.path.dirname(__file__)
parent_url = os.path.abspath(os.path.join(current_url, os.pardir))
sys.path.append(parent_url)

from utils import sql_session, mongo_db, cache_cli
from coinla_spider.databases.models import \
    CurrencyExchangeRelation, Currency, Exchange
from pymongo.errors import DuplicateKeyError
import click

sql_db = sql_session()


def _alter_cqn_map(id_type, old_id, new_id):
    if id_type == 'ccy_id':
        flt = {CurrencyExchangeRelation.currency_id == old_id}
    elif id_type == 'ege_id':
        flt = {CurrencyExchangeRelation.exchange_id == old_id}
    else:
        raise ValueError()
    old_cqn_cols = sql_db.query(
        CurrencyExchangeRelation).filter(*flt).all()

    alter_map = dict()
    for c in old_cqn_cols:
        if id_type == 'ccy_id':
            flt = {
                CurrencyExchangeRelation.currency_id == new_id,
                CurrencyExchangeRelation.exchange_id == c.exchange_id,
                CurrencyExchangeRelation.currency_right_id == c.currency_right_id
            }
        elif id_type == 'ege_id':
            flt = {
                CurrencyExchangeRelation.currency_id == c.currency_id,
                CurrencyExchangeRelation.exchange_id == new_id,
                CurrencyExchangeRelation.currency_right_id == c.currency_right_id
            }
        new_cqn = sql_db.query(CurrencyExchangeRelation).filter(*flt).first()
        # 如果新ID不存在冲突行情，则将旧行情改为新的
        if new_cqn is None:
            alter_map[c.id] = c.id
            if id_type == 'ccy_id':
                c.currency_id = new_id
            elif id_type == 'ege_id':
                c.exchange_id = new_id
        # 如有冲突，则映射旧ID与新ID
        else:
            alter_map[c.id] = new_cqn.id
    return alter_map


def _delete_old(id_type, old_id, new_id):
    # 修改缓存
    if id_type == 'ccy_id':
        model = Currency
        flt = {CurrencyExchangeRelation.currency_id == old_id}
        coll = 'Currency'
        field = 'ccyId'
    elif id_type == 'ege_id':
        model = Exchange
        flt = {CurrencyExchangeRelation.exchange_id == old_id}
        coll = 'Exchange'
        field = 'egeId'
    else:
        raise ValueError()

    # 将Redis缓存中旧ID改为新ID
    for tail in [coll, coll + 'Url']:
        keys = cache_cli.keys('SpiderCache:{}:*'.format(tail))
        for k in keys:
            if int(old_id) == int(cache_cli.get(k)):
                cache_cli.set(k, new_id)

    # 删除旧ID
    sql_db.query(CurrencyExchangeRelation).filter(*flt).delete()
    sql_db.query(model).filter_by(id=old_id).delete()

    # 删除相关的行情
    mongo_db[coll].delete_one({field: old_id})
    mongo_db['OpenQuotationInfo'].delete_many({field: old_id})


@click.command()
@click.option('--id_type', help='ccy_id or ege_id')
@click.option('--old_id')
@click.option('--new_id')
def merge_kline(id_type, old_id, new_id):
    # 如果是币种ID，则合并趋势图
    if id_type == 'ccy_id':
        for d in mongo_db['KLineCrawlDay'].find({'ccy_id': old_id}, {'_id': 1, 'timestamp': 1}):
            try:
                mongo_db['KLineCrawlDay'].update_one({'_id': d['_id']},
                                                     {'$set': {'ccy_id': new_id}})
            except DuplicateKeyError:
                mongo_db['KLineCrawlDay'].delete_one({'ccy_id': new_id, 'timestamp': d['timestamp']})
                mongo_db['KLineCrawlDay'].update_one({'_id': d['_id']},
                                                     {'$set': {'ccy_id': new_id}})

    cqn_map = _alter_cqn_map(id_type, old_id, new_id)

    for coll in ['KLineDay', 'KLineHour', 'KLineHourFour', 'KLineHourSix', 'KLineHourTwelve',
                 'KLineHourTwo', 'KLineMinute', 'KLineMinuteFifteen', 'KLineMinuteOne', 'KLineMinuteThirty',
                 'KLineMinuteThree', 'KLineMonth', 'KLineWeek'] + \
                ['QuotationInfoDay', 'QuotationInfoHour', 'QuotationInfoMonth', 'QuotationInfoWeek']:

        for old_cqn_id, new_cqn_id in cqn_map.items():
            if coll in ['QuotationInfoDay', 'QuotationInfoHour',
                        'QuotationInfoMonth', 'QuotationInfoWeek']:
                cer_field = 'cerId'
                old_cqn_id = str(old_cqn_id)
                new_cqn_id = str(new_cqn_id)
            else:
                cer_field = 'relationId'

            # 如果新旧ID不相等(即存在旧数据)，需要把合并的时间段内旧数据清空
            if old_cqn_id != new_cqn_id:
                min_doc = list(mongo_db[coll].aggregate([
                    {'$match': {cer_field: old_cqn_id}},
                    {'$group': {'_id': '${}'.format(cer_field), 'min_date': {'$min': '$createTime'}}}
                ]))
                if min_doc:
                    min_date = min_doc[0]['min_date']
                    mongo_db[coll].delete_many({cer_field: new_cqn_id, 'createTime': {'$gte': min_date}})

            mongo_db[coll].update_many({cer_field: old_cqn_id}, {'$set': {cer_field: new_cqn_id}})
            print('{}  {}: {} → {}, '.format(coll, id_type, old_id, new_id),
                  'cer_id: {} → {}'.format(old_cqn_id, new_cqn_id))

    _delete_old(id_type, old_id, new_id)
    try:
        sql_db.commit()
    except Exception as exc:
        sql_db.rollback()
        raise exc
    finally:
        sql_db.close()


if __name__ == '__main__':
    merge_kline()
