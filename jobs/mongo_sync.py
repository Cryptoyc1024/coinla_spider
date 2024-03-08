# -*- coding: utf-8 -*-

"""
将 MySQL数据库 同步到 Mongo数据库
"""

import os
import sys

current_url = os.path.dirname(__file__)
parent_url = os.path.abspath(os.path.join(current_url, os.pardir))
sys.path.append(parent_url)

# from jobs import mongo_db, sql_session, logging_decorator
from jobs import mongo_db, sql_session
from coinla_spider.databases.models import *
from datetime import datetime

#
# @logging_decorator
def mongo_sync():
    sql_db = sql_session()
    cq = sql_db.query(Currency).all()
    mongo_ccy = mongo_db['Currency']

    for c in cq:
        mcm = sql_db.query(ModuleCurrencyMiddle).filter_by(ccy_id=c.id).all()
        concept = sql_db.query(ModuleConcept).filter(ModuleConcept.id.in_([i.mc_id for i in mcm])).all()
        insert_doc = {
            'ccyId': c.id,
            'currencyName': c.currency_name,
            'english': c.english,
            'shortName': c.short_name,
            'circulateTotal': c.circulate_total,
            'circulateTotalValue': c.circulate_total_value,
            'sortCirculateTotalValue': c.sort_circulate_total_value,
            'total': c.total,
            'totalValue': c.total_value,
            'sortTotalValue': c.sort_total_value,
            'numberOfExchange': c.market_qty,
            'follow': c.follow,
            'initiatePerson': c.initiate_person,
            'initiateCreateDate': c.initiate_create_date,
            'concept': ','.join([i.mc_name for i in concept]) if concept else None,
            'consensusMechanism': c.consensus_mechanism,
            'pic': c.pic,
            'guanw': c.guanw,
            'blockChain': c.block_chain,
            'whitePaperZh': c.white_paper_zh,
            'whitePaperEn': c.white_paper_en,
            'officialCommunity': c.official_community,
            'introduce': c.introduce,
            'teamIntroduce': c.team_introduce,
            'appIntroduce': c.app_introduce,
            'isIco': c.is_ico,
            'ico': c.ico,
            'privatePrice': c.private_price,
            'privateTotalValue': c.private_total_value,
            'privateTime': c.private_time,
            'isTeam': c.isTeam,
            'ismining': c.ismining,
            'recordStatus': c.record_status,
            'version': c.version,
            'createDate': datetime.now(),
            'updateDate': datetime.now(),
            'createBy': 1,
            'updateBy': 1
        }
        update_doc = {
            'ccyId': c.id,
            'currencyName': c.currency_name,
            'english': c.english,
            'shortName': c.short_name,
            'circulateTotal': c.circulate_total,
            'circulateTotalValue': c.circulate_total_value,
            'sortCirculateTotalValue': c.sort_circulate_total_value,
            'total': c.total,
            'totalValue': c.total_value,
            'sortTotalValue': c.sort_total_value,
            'numberOfExchange': c.market_qty,
            'follow': c.follow,
            'initiatePerson': c.initiate_person,
            'initiateCreateDate': c.initiate_create_date,
            'concept': ','.join([i.mc_name for i in concept]) if concept else None,
            'consensusMechanism': c.consensus_mechanism,
            'pic': c.pic,
            'guanw': c.guanw,
            'blockChain': c.block_chain,
            'whitePaperZh': c.white_paper_zh,
            'whitePaperEn': c.white_paper_en,
            'officialCommunity': c.official_community,
            'introduce': c.introduce,
            'teamIntroduce': c.team_introduce,
            'appIntroduce': c.app_introduce,
            'isIco': c.is_ico,
            'ico': c.ico,
            'privatePrice': c.private_price,
            'privateTotalValue': c.private_total_value,
            'privateTime': c.private_time,
            'isTeam': c.isTeam,
            'ismining': c.ismining,
            'recordStatus': c.record_status,
            'version': c.version,
            'createDate': datetime.now(),
            'updateDate': datetime.now(),
            'createBy': 1,
            'updateBy': 1
        }
        if not mongo_ccy.find_one({'ccyId': c.id}, {'ccyId': 1}):
            mongo_ccy.insert_one(insert_doc)
        else:
            mongo_ccy.update_one({'ccyId': c.id}, {'$set': update_doc})
        # mongo_ccy.replace_one({'ccyId': c.id}, insert_doc, upsert=True)

    # 从Mongo币种表里删除SQL里已经删除的币种
    ccy_ids_sql = list(set(c.id for c in cq))
    assert len(ccy_ids_sql) > 3000
    mongo_ccy.delete_many({'ccyId': {'$nin': ccy_ids_sql}})
    mongo_db['CurrencyCoreData'].delete_many({'ccy_id': {'$nin': ccy_ids_sql}})
    mongo_db['OpenQuotationInfo'].delete_many({'ccyId': {'$nin': ccy_ids_sql}})
    mongo_db['HolderList'].delete_many({'ccy_id': {'$nin': ccy_ids_sql}})
    mongo_db['KLineCrawlDay'].delete_many({'ccy_id': {'$nin': ccy_ids_sql}})
    mongo_db['CurrencyHistoryDay'].delete_many({'ccy_id': {'$nin': ccy_ids_sql}})

    # ------------------------------------------------------------------------

    eq = sql_db.query(Exchange).all()
    mongo_ege = mongo_db['Exchange']

    for e in eq:
        insert_doc = {
            'egeId': e.id,
            'exchangeName': e.exchange_name,
            'exchangeNameZh': e.exchange_name_zhhk,
            'exchangeNameEn': e.exchange_name_en,
            'turnover': e.turnover,
            'turnoverUsd': None,
            'sortTurnover': e.sort_turnover,
            'transactionPair': e.transaction_pair,
            'ranking': e.ranking,
            'level': e.level,
            'sort': e.sort,
            'country': e.country,
            'link': e.link,
            'dealLink': e.deal_link,
            'pic': e.pic,
            'picDeal': e.pic_deal,
            'picAccess': e.pic_access,
            'introduce': e.introduce,
            'futures': e.futures,
            'stock': e.stock,
            'legalTender': e.legal_tender,
            'recordStatus': e.record_status,
            'version': e.version,
            'createDate': datetime.now(),
            'updateDate': datetime.now(),
            'createBy': 1,
            'updateBy': 1
        }
        update_doc = {
            'egeId': e.id,
            'exchangeName': e.exchange_name,
            'exchangeNameZh': e.exchange_name_zhhk,
            'exchangeNameEn': e.exchange_name_en,
            'turnover': e.turnover,
            'turnoverUsd': None,
            'sortTurnover': e.sort_turnover,
            'transactionPair': e.transaction_pair,
            'ranking': e.ranking,
            'level': e.level,
            'sort': e.sort,
            'country': e.country,
            'link': e.link,
            'dealLink': e.deal_link,
            'pic': e.pic,
            'picDeal': e.pic_deal,
            'picAccess': e.pic_access,
            'introduce': e.introduce,
            'futures': e.futures,
            'stock': e.stock,
            'legalTender': e.legal_tender,
            'recordStatus': e.record_status,
            'version': e.version,
            'createDate': datetime.now(),
            'updateDate': datetime.now(),
            'createBy': 1,
            'updateBy': 1
        }
        if not mongo_ege.find_one({'egeId': e.id}, {'egeId': 1}):
            mongo_ege.insert_one(insert_doc)

        else:
            mongo_ege.update_one({'egeId': e.id}, {'$set': update_doc})

        # mongo_ege.replace_one({'egeId': e.id}, doc, upsert=True)

    ege_ids_sql = list(set(e.id for e in eq))
    mongo_ege.delete_many({'egeId': {'$nin': ege_ids_sql}})
    mongo_db['OpenQuotationInfo'].delete_many({'egeId': {'$nin': ege_ids_sql}})
    sql_db.close()


if __name__ == '__main__':
    mongo_sync()
