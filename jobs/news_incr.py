# -*- coding: utf-8 -*-

"""
快讯点赞数增长
"""

import os
import sys

current_url = os.path.dirname(__file__)
parent_url = os.path.abspath(os.path.join(current_url, os.pardir))
sys.path.append(parent_url)

# from jobs import sql_session, logging_decorator
from jobs import sql_session
from coinla_spider.databases.models import HeadNewsLetter
from datetime import datetime
import random
import re

# 首位快讯的概率系数
FIRST_COEF = 1.0
# 逐条递减的系数
DCR_COEF = 0.1
# 最多增长条数
INCR_LIMIT = 50
# 热点关键词
HOT_KW_COMP = r'(比特币)|(中国)|(我国)|(国内)|(挖矿)'
# 关键词概率倍数
KW_COEF = 1.2
# 低于档位则进行多次增长
MULTI_VALS = [20, 50]


def news_incr(times=1):
    sql_db = sql_session()
    try:
        news_query = sql_db.query(HeadNewsLetter).filter(
            HeadNewsLetter.shelf_state == 1,
            HeadNewsLetter.show_date <= datetime.now()
        ).order_by(HeadNewsLetter.show_date.desc()).limit(INCR_LIMIT)

        for _ in range(times):
            for idx, news in enumerate(news_query):
                coef = FIRST_COEF / (DCR_COEF * idx + 1)
                if re.search(HOT_KW_COMP, news.title):
                    coef *= KW_COEF
                _incr(news, news.proportion, coef)

                total = news.good_number + news.bad_number
                for val in MULTI_VALS:
                    if total < val:
                        _incr(news, news.proportion, coef)

        sql_db.commit()
    except Exception as e:
        sql_db.rollback()
        raise e
    finally:
        sql_db.close()


def _incr(news, pr, coef=1.0):
    good_pr = pr * coef
    bad_pr = (100 - pr) * coef
    if _roll(good_pr) is True:
        news.good_number += 1
    if _roll(bad_pr) is True:
        news.bad_number += 1


def _roll(pr):
    points = random.randint(1, 100)
    if pr >= points:
        return True
    else:
        return False


if __name__ == '__main__':
    news_incr()
