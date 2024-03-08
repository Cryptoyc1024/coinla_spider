# -*- coding: utf-8 -*-

"""
增加交易对的关注量
会根据交易量计算命中率，然后随机增加
如 火币BTC/USDT 有大约5%的命中率
"""

import os
import sys

current_url = os.path.dirname(__file__)
parent_url = os.path.abspath(os.path.join(current_url, os.pardir))
sys.path.append(parent_url)

# from jobs import mongo_db, cache_cli, logging_decorator
from jobs import mongo_db, cache_cli
from coinla_spider.settings import REDIS_INTERNAL_HOST, REDIS_INTERNAL_PORT, REDIS_INTERNAL_PASSWORD
from redis import StrictRedis
import random

EXCHANGE_LEVEL_FORCE = {
    '全网': 1.5,
    'BitMEX': 0.5,
    '火币Pro': 7,
    'OKEX': 6,
    '币夫': 0.5,
    'Lykke Exchange': 0.5,
    '币安网': 4,
    '国行': 2,
}


def quotation_data_map():
    docs = mongo_db['OpenQuotationInfo'].find(
        {'type': 0, 'recordStatus': 0},
        {'cerId': 1, 'egeId': 1, 'openTurnoverSort': 1, '_id': 0,
         'currencyShortName': 1, 'currencyRightShortName': 1, 'exchangeName': 1, 'openTurnover': 1})
    return {d.pop('cerId'): d for d in docs}


def exchange_level_map():
    docs = mongo_db['Exchange'].find({}, {'egeId': 1, 'exchangeName': 1, 'level': 1, '_id': 0})

    def reset_level(ege_name, level):
        if ege_name in EXCHANGE_LEVEL_FORCE:
            level = EXCHANGE_LEVEL_FORCE[ege_name]
        else:
            level = level
        if level < 0:
            level = 0.1
        return level

    data_map = {d['egeId']: reset_level(d['exchangeName'], d['level']) ** 3 for d in docs}
    return data_map


def calc_quota_weights(quota_turnover_map, quota_ege_map, ege_level_map):
    weight_map = dict()
    otc_ege_id = int(cache_cli.get('SpiderCache:Exchange:国行-OTC'))
    for cer_id, turnover in quota_turnover_map.items():
        ege_id = quota_ege_map[cer_id]
        if ege_id == otc_ege_id:
            turnover = 1e9
        elif turnover == 0:
            turnover = 1e4
        weight = ege_level_map.get(ege_id, 0.1) * turnover
        weight_map[cer_id] = weight
    return weight_map


def increase(quota_hits):
    redis_cli = StrictRedis(REDIS_INTERNAL_HOST, REDIS_INTERNAL_PORT,
                            password=REDIS_INTERNAL_PASSWORD,
                            max_connections=10, decode_responses=True,
                            socket_timeout=5, socket_connect_timeout=5)
    with redis_cli.pipeline(transaction=False) as pipe:
        for cer_id, hits in quota_hits.items():
            key = 'Defaultmarket:attention:{}'.format(cer_id)
            pipe.incrby(key, hits)
        pipe.execute()

#
# @logging_decorator
def attention_incr(times: int, debug=None):
    """
    随机增加交易对的关注量
    :param times: 执行次数
    :param debug: 可执行交易所名进行debug输出
    :return:
    """
    cache_key = 'SpiderCache:Jobs:AttentionIncr'

    def get_weight_map():
        global quota_data_map
        quota_data_map = quotation_data_map()
        ege_level_map = exchange_level_map()
        quota_turnover_map = dict()
        quota_ege_map = dict()
        for _cer_id, data in quota_data_map.items():
            quota_turnover_map[_cer_id] = data['openTurnoverSort']
            quota_ege_map[_cer_id] = data['egeId']
        _weight_map = calc_quota_weights(quota_turnover_map, quota_ege_map, ege_level_map)
        cache_cli.hmset(cache_key, _weight_map)
        cache_cli.expire(cache_key, 3600 * (24 + random.randint(12, 24)))
        return _weight_map

    if not debug:
        weight_map = {int(k): float(v) for k, v in
                      cache_cli.hgetall(cache_key).items()}
        if not weight_map:
            weight_map = get_weight_map()
    else:
        weight_map = get_weight_map()

    cer_ids = list(weight_map.keys())
    weights = list(weight_map.values())
    quota_hits = dict()
    for _ in range(times):
        hit_id = random.choices(cer_ids, weights=weights)[0]
        quota_hits[hit_id] = quota_hits.get(hit_id, 0) + 1

    if not debug:
        increase(quota_hits)

    else:
        ege_count = dict()
        huobi_count = dict()
        for cer_id, hits in quota_hits.items():
            ege_name = quota_data_map[cer_id]['exchangeName']
            ege_count[ege_name] = ege_count.get(ege_name, 0) + hits
            if ege_name == debug:
                huobi_count[cer_id] = huobi_count.get(cer_id, 0) + hits
        import pprint
        pprint.pprint(ege_count)
        for cer_id, hits in huobi_count.items():
            print(quota_data_map[cer_id], '    ', hits)


if __name__ == '__main__':
    # 火币BTC/USDT有5%的命中率
    attention_incr(1, debug='全网')
