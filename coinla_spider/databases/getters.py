# -*- coding: utf-8 -*-

"""
定义了一些从数据库获取数据的常用函数
如获取币种ID 币种名等
"""

import re
from difflib import SequenceMatcher
from random import randint

from sqlalchemy import or_

from coinla_spider.databases.connections import *
from coinla_spider.databases.models import Currency, CurrencyName, Exchange
from coinla_spider.formatters import AbbreviationToFloat
from coinla_spider.items import CurrencyItem, QuotationItem, ExchangeItem
from coinla_spider.settings import COMMON_EXCHANGE, OTC_EXCHANGE


def get_model_obj(model, filters, field_map=None):
    """
    查询或插入 Model 对象
    :param model: Model 对象
    :param filters: 过滤条件，字典形式
    :param field_map: SQL字段与Item键值的映射，字典形式
    :return: 查询到或创建的 Model 对象
    """
    obj = sql_db.select(model, filters)
    if obj is None:
        obj = sql_db.insert(model, field_map)
    return obj


@inlineCallbacks
def get_ccy_id(item, field_map):
    """ 查询或创建币种Model对象 """
    cache_key = '{}-{}'.format(item['shortName'], item['english'])
    ccy_id = yield cache.load('Currency', cache_key)

    if not ccy_id:
        # 启动SQL事务，保证币种表和币种名表一致性
        sql_db.session.begin()

        # 从币种名表里查询币种ID
        cn_filters = {
            CurrencyName.short_name == item['shortName'],
            CurrencyName.english == item['english'],
            CurrencyName.ege_id == 0
        }
        cn = sql_db.select(CurrencyName, cn_filters)

        if cn is None:
            cn = yield _insert_ccy_name(item, field_map)
            ccy_id = cn.ccy_id

        # 标注-1则不进行判断直接插入新币
        elif cn.ccy_id == -1:
            ccy_id = cn.ccy_id = yield _insert_ccy(item, field_map)

        # 标注-2则更新原有币种所有字段
        elif cn.ccy_id == -2:
            # 更新MySQL币种表的信息
            ccy_filters = {
                or_(Currency.short_name == item['shortName'],
                    Currency.english == item['english'])
            }
            ccy = sql_db.select(Currency, ccy_filters)
            if ccy is None:
                cn.ccy_id = -1
                return (yield get_ccy_id(item, field_map))
            ccy_id = item['ccyId'] = cn.ccy_id = ccy.id
            sql_db.update(Currency, ccy_id, field_map)
            # 更新Mongo币种表的信息
            yield mongo_db[item.__collection__].update_one(
                {'ccyId': ccy_id}, {'$set': item}, timeout=20)
            # 更新全网行情的币种信息
            common_ege_id = yield get_common_ege_id()
            if item['initiateCreateDate']:
                init_date = datetime.strptime(item['initiateCreateDate'], '%Y-%m-%d')
                init_tm = int(init_date.timestamp()) * 1000
            else:
                init_tm = 0
            mongo_map = {
                'currencyName': item['currencyName'],
                'currencyEnglisgName': item['english'],
                'currencyShortName': item['shortName'],
                'initiateCreateDate': init_tm
            }
            yield mongo_db[QuotationItem.__collection__].update_many(
                {'ccyId': ccy_id, 'egeId': common_ege_id},
                {'$set': mongo_map}, timeout=20)

        else:
            ccy_id = cn.ccy_id

        # 提交SQL事务
        sql_db.commit()

        # 保存币种名缓存
        ex = _rand_expire(ccy_id)
        yield cache.save('Currency', cache_key, ccy_id, expire=ex)

    # 保存币种链接缓存
    url = getattr(item, '_url', None)
    if url is not None:
        ex = 3600 * 12 if ccy_id <= 0 else None
        yield cache.save('CurrencyUrl', url, ccy_id, expire=ex)
        # 保存链接保证下次爬取必有此币种(用于补充核心数据)
        yield cache.save('Addition', 'Currency', url, command='sadd')
    item['ccyId'] = ccy_id
    return ccy_id


@inlineCallbacks
def _insert_ccy_name(item, field_map):
    # 若没有查询到币种名，则查询缩写或英文名是否重复
    ccy_id = 0
    cn_filters = {
        or_(CurrencyName.short_name == item['shortName'],
            CurrencyName.english == item['english']),
        CurrencyName.ege_id == 0
    }
    similar_cn = sql_db.select(CurrencyName, cn_filters)

    # 有重复名则查看币种的官网或区块站是否相似，如相似则视为同一币种
    if similar_cn is not None and (item['guanw'] or item['blockChain']):
        ccy = sql_db.select(Currency, {Currency.id == similar_cn.ccy_id})

        def are_idenical(url, another):
            if not url or not another:
                return False
            pattern = r'(https*://)|(www\.)|((?<=\w)/.+)'
            url = re.sub(pattern, '', url)
            another = re.sub(pattern, '', another)
            return SequenceMatcher(None, url, another).ratio() >= 0.8

        if are_idenical(item['guanw'], ccy.guanw) or \
                are_idenical(item['blockChain'], ccy.block_chain):
            ccy_id = ccy.id
            # 更新币种名
            if ccy.short_name == ccy.english != item['english']:
                ccy.currency_name = item['currencyName']
                ccy.english = item['english']

    # 没有重复名则进一步查询币种表
    if similar_cn is None:
        # 币种表查询是否有重复名
        ccy_filters = {
            or_(Currency.short_name == item['shortName'],
                Currency.english == item['english'])
        }
        ccy = sql_db.select(Currency, ccy_filters)
        # 若币种表有重复，则为这条重复插入币种名
        if ccy is not None:
            cn_map = {
                'short_name': ccy.short_name,
                'english': ccy.english,
                'ccy_id': ccy.id,
                'origin_url': ''
            }
            sql_db.insert(CurrencyName, cn_map)
        # 没有则新建币种
        else:
            ccy_id = yield _insert_ccy(item, field_map)

    # 无论是否新建了币种，都插入币种名
    cn_map = {
        'short_name': item['shortName'],
        'english': item['english'],
        'ccy_id': ccy_id,
        'origin_url': getattr(item, '_url', None)
    }
    return sql_db.insert(CurrencyName, cn_map)


@inlineCallbacks
def _insert_ccy(item, field_map):
    ccy = sql_db.insert(Currency, field_map)
    item['ccyId'] = ccy_id = ccy.id
    list(map(item.setdefault, item.fields.keys()))
    yield mongo_db[item.__collection__].update_one(
        {'ccyId': ccy_id}, {'$setOnInsert': item}, upsert=True, timeout=20)
    return ccy_id


@inlineCallbacks
def get_ege_id(item, field_map):
    """ 查询或创建交易所Model对象 """
    filters = {Exchange.exchange_name == item['exchangeName']}
    ege = sql_db.select(Exchange, filters)
    if ege is None:
        ege = sql_db.insert(Exchange, field_map)
        ege_id = item['egeId'] = ege.id
        list(map(item.setdefault, item.fields.keys()))
        yield mongo_db[item.__collection__].update_one(
            {'egeId': ege_id}, {'$setOnInsert': item}, upsert=True, timeout=20)
    else:
        ege_id = item['egeId'] = ege.id
    cache_key = '{}-{}'.format(item['exchangeName'], item['exchangeNameEn'])
    # 交易所名称缓存
    ex = _rand_expire(ege_id)
    yield cache.save('Exchange', cache_key, ege_id, expire=ex)
    # 交易所链接缓存
    url = getattr(item, '_url', None)
    if url is not None:
        yield cache.save('ExchangeUrl', url, ege_id)
    return ege_id


@inlineCallbacks
def get_common_ege_id():
    """ 获取全网交易所ID """
    name, en_name = COMMON_EXCHANGE
    return (yield _get_common_ege_id(name, en_name))


@inlineCallbacks
def get_otc_ege_id():
    """ 获取国行交易所ID """
    name, en_name = OTC_EXCHANGE
    return (yield _get_common_ege_id(name, en_name))


@inlineCallbacks
def _get_common_ege_id(name, en_name):
    cache_key = '{}-{}'.format(name, en_name)
    ege_id = yield cache.load('Exchange', cache_key)
    if ege_id is None:
        item = ExchangeItem(
            exchangeName=name,
            exchangeNameEn=en_name,
            exchangeNameZh=name,
            recordStatus=-1,
            introduce='全网数据关联显示用，不可删除'
        )
        field_map = {
            'exchange_name': name,
            'exchange_name_en': en_name,
            'exchange_name_zhhk': name,
            'record_status': -1,
            'introduce': '全网数据关联显示用，不可删除'
        }
        ege_id = yield get_ege_id(item, field_map)
    return ege_id


@inlineCallbacks
def get_legal_ids():
    """ 获取所有法币ID """
    legal_ccy = yield mongo_db[CurrencyItem.__collection__].find(
        {'recordStatus': -1}, {'ccyId': 1, '_id': 0}, timeout=10)
    return [d['ccyId'] for d in legal_ccy]


@inlineCallbacks
def get_circulate_value_map(limit=None):
    keys = yield cache.load('CurrencyData', '*', command='keys')
    val_map = dict()
    for k in keys[:limit]:
        ccy_id = int(k.split(':')[-1])
        circ = yield cache.load(
            'CurrencyData', ccy_id, 'circulate_value', command='hget')
        val_map[int(k.split(':')[-1])] = float(circ or 0)
    return val_map


@inlineCallbacks
def get_ccy_data(ccy_id, field):
    if field not in ['circulate_total', 'circulate_value', 'total']:
        raise ValueError('Does not support {} field'.format(field))
    data = yield cache.load('CurrencyData', ccy_id, field, command='hget')
    if data is None:
        doc = yield mongo_db[CurrencyItem.__collection__].find_one(
            {'ccyId': ccy_id},
            {'circulateTotal': 1, 'sortCirculateTotalValue': 1,
             'total': 1, '_id': 0})
        if doc is None:
            return 0
        num_fmt = AbbreviationToFloat().format
        circulate = num_fmt(doc['circulateTotal'])
        total = num_fmt(doc['total'])
        cache_data = {
            'circulate_total': circulate,
            'circulate_value': doc['sortCirculateTotalValue'],
            'total': total
        }
        yield cache.save('CurrencyData', ccy_id, cache_data, command='hmset')
        data = cache_data[field]
    return float(data)


@inlineCallbacks
def get_ccy_short_name_count(short_name):
    keys = yield cache.load(
        'Currency', '{}-*'.format(short_name), command='keys')
    if len(keys) > 1:
        ids = set()
        for k in keys:
            name_tail = k.split(':')[-1]
            ccy_id = yield cache.load('Currency', name_tail)
            ids.add(ccy_id)
        return len(ids)
    elif not keys:
        return 0
    return 1


@inlineCallbacks
def get_tocny_exrate(ccy_short, raise_error=True):
    exrate = yield cache.load('ExrateCNY', ccy_short)
    if exrate is None and raise_error:
        ValueError('Missing {} exrate in cache'.format(ccy_short))
    return float(exrate)


def _rand_expire(value):
    if value <= 0:
        return 3600 * 2
    else:
        return 3600 * 24 * 14 + randint(0, 3600)
