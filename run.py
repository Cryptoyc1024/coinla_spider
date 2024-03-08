# -*- coding: utf-8 -*-

import os
from datetime import datetime, timedelta

from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_MISSED
from apscheduler.schedulers.blocking import BlockingScheduler

from jobs.attention_incr import attention_incr
from jobs.cache_sync import cache_sync
from jobs.check_activity import check_activity
# from jobs.check_update import check_update
from jobs.mongo_sync import mongo_sync
from jobs.news_incr import news_incr

job_defaults = {
    'coalesce': True,
    'max_instances': 1,
    'misfire_grace_time': 120
}

scheduler = BlockingScheduler(job_defaults=job_defaults)


def retry_listen(e):
    job = scheduler.get_job(e.job_id)
    if 'retry' in job.name:
        run_time = datetime.now() + timedelta(seconds=10)
        job.modify(next_run_time=run_time)


def calc_cqn():
    os.system('scrapy crawl calc_cqn -a limit=101,')


def calc_cqn_top():
    os.system('scrapy crawl calc_cqn -a limit=1,100 -s CLOSESPIDER_TIMEOUT=15')


def currency():
    os.system('scrapy crawl currency')


def exchange():
    os.system('scrapy crawl exchange -a page=1,99')


def exchange_hot():
    os.system('scrapy crawl exchange -a page=1,2,3 -s CLOSESPIDER_TIMEOUT=100')


def concept():
    os.system('scrapy crawl concept')


def otc_price():
    os.system('scrapy crawl otc_price')


def ccy_daily():
    os.system('scrapy crawl ccy_daily')


def chart_day_calc():
    os.system('scrapy crawl chart -a mode=calc')


def exrate():
    os.system('scrapy crawl exrate')


def develop():
    os.system('scrapy crawl develop')


def holder():
    os.system('scrapy crawl holder')


def event():
    os.system('scrapy crawl event')


def news():
    os.system('scrapy crawl news')


def notice():
    os.system('scrapy crawl notice')


if __name__ == '__main__':
    scheduler.add_listener(retry_listen, EVENT_JOB_ERROR | EVENT_JOB_MISSED)

    scheduler.add_job(currency, 'cron', name='Currency', minute='30')

    scheduler.add_job(exrate, 'cron', name='Exrate', minute='*/30', max_instances=99)
    scheduler.add_job(concept, 'cron', name='Concept', hour='18', minute='45')
    scheduler.add_job(chart_day_calc, 'cron', name='Chart-Day-Calc', hour='0', minute='1')
    scheduler.add_job(notice, 'cron', name='Notice', minute='10')
    scheduler.add_job(event, 'cron', name='Event', day_of_week='mon', hour='8', minute='45')

    # 交易所交易对
    scheduler.add_job(exchange, 'interval', name='Exchange-4,99', minutes=3)
    scheduler.add_job(exchange_hot, 'interval', name='Exchange-1,2', minutes=2)

    # 交易对行情数据计算，实时计算所有交易对的最新全网价格、交易量等数据
    scheduler.add_job(calc_cqn_top, 'interval', name='Calc_CQN-Top', seconds=10,
                      max_instances=99, misfire_grace_time=3)
    scheduler.add_job(calc_cqn, 'interval', name='Calc_CQN', seconds=60,
                      max_instances=99)

    # 币种行情
    scheduler.add_job(ccy_daily, 'cron', name='CurrencyDailyData', hour='1', minute='5')

    # otc 行情
    scheduler.add_job(otc_price, 'interval', name='OTCPrice', minutes=3)

    # 同步mongodb数据库和mysql数据库
    scheduler.add_job(mongo_sync, 'cron', name='MongoSync-retry', hour='12,19', minute='15')

    # 检查长期没有行情的币种，行情数据进行清零
    scheduler.add_job(check_activity, 'cron', name='CheckActivity', hour='10')
    #
    # # 发送邮件
    # scheduler.add_job(check_update, 'interval', name='CheckUpdate', minutes=1,
    #                   misfire_grace_time=10)

    # 币种开发进度爬虫，主要是github提交时间和社区互动等一些数据
    scheduler.add_job(develop, 'cron', name='Develop', hour='9', minute='5')

    # 币种持有量的爬虫，爬取主流区块浏览器，汇总成币种的富豪榜数据
    scheduler.add_job(holder, 'cron', name='Holder', hour='9', minute='15')

    # 新闻/快讯
    scheduler.add_job(news, 'interval', name='News', minutes=5, max_instances=99)

    # 快讯点赞数增长
    scheduler.add_job(news_incr, 'interval', name='NewsIncr', seconds=30)

    #  随机增加交易对的关注量
    scheduler.add_job(attention_incr, 'interval', name='AttentionIncr', minutes=5,
                      kwargs={'times': 30})

    # 将币种名重新进行缓存
    scheduler.add_job(cache_sync, 'cron', name='CacheCcyName', hour='23', minute='59')

    scheduler.start()
