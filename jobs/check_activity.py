# -*- coding: utf-8 -*-

"""
定时提醒邮件
检查长时间未更新或突然出现更新的数据
"""
""""""
import os
import sys

current_url = os.path.dirname(__file__)
parent_url = os.path.abspath(os.path.join(current_url, os.pardir))
sys.path.append(parent_url)

# from jobs import sql_session, mongo_db, logging_decorator, jobs_logger
from jobs import sql_session, mongo_db, jobs_logger
import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr
from coinla_spider.databases.models import Currency, Exchange
from coinla_spider.settings import *
from sqlalchemy import or_
from datetime import datetime, timedelta


# class QueueMailer(object):
#
#     def __init__(self, mail_to=None, mail_from=None):
#         self.mail_to = mail_to or ['626995603@qq.com', 'coinla@zoho.com.cn']
#         self.mail_from = mail_from or MAIL_FROM
#         self.msg_queue = list()
#
#     def add_msg(self, title, desc, th1, th2, trs):
#         length = len(trs)
#         if length == 0:
#             return None
#         trs = ['<input type="checkbox">{}&nbsp;&nbsp;{}'.format(*tr) for tr in trs]
#         text = """
#         <html><body>
#         <p>{}</p>
#         <br>
#         <p><b>{}&nbsp;&nbsp;{}</b></p>
#         <p>{}</p>
#         </body></html>
#         """
#         msg = MIMEText(text.format(desc, th1, th2, '<br>'.join(trs)), 'html')
#         msg['From'] = formataddr(['Kai', MAIL_FROM])
#         msg['Subject'] = title.format(length)
#         self.msg_queue.append(msg.as_string())
#
#     def send_mails(self):
#         mail_cli = smtplib.SMTP_SSL(MAIL_HOST, MAIL_PORT)
#         mail_cli.login(MAIL_USER, MAIL_PASS)
#         for mail in self.msg_queue:
#             jobs_logger.debug(mail)
#             mail_cli.sendmail(self.mail_from, self.mail_to, mail)
#         mail_cli.quit()
#
#
# mailer = QueueMailer()


def check_currency():
    """ 检查长期没有行情的币种，行情数据进行清零 """
    inactive_time = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d %H:%M:%S')
    inactive_ids = [d['ccyId'] for d in mongo_db['OpenQuotationInfo'].find(
        {
            'date': {'$exists': False},
            'time': {'$lte': inactive_time},
            'type': 0,
            'openOrigin': {'$gt': 0}
        },
        {'ccyId': 1}
    )]
    for ccy_id in inactive_ids:
        mongo_db['OpenQuotationInfo'].update_many(
            {'ccyId': ccy_id, 'date': {'$exists': False}},
            {'$set': {
                'openOrigin': 0,
                'openPrice': 0,
                'openNextPrice': 0,
                'openRiseFall': 0,
                'openRiseFallToday': 0,
                'openRiseFallHour': 0,
                'openRiseFallWeek': 0,
                'openHighPriceToday': 0,
                'openLowPriceToday': 0,
                'openVolumeMonth': 0,
                'openTurnover': '0',
                'openTurnoverSort': 0,
                'openVolume': '0',
                'openVolumeSort': 0,
                'openTurnoverProportion': 0,
                'exchangeTurnover': '0',
                'exchangeTurnoverSort': 0,
                'openTotalValue': '0',
                'openTotalValueSort': 0,
                'openCirculateTotalValue': '0',
                'openCirculateTotalValueSort': 0,
                'openCirculateTotal': '1亿',
                'openCirculateTotalSort': 100000000.0,
                'openTotal': '1亿',
                'openShrinkage': -100
            }}
        )


def check_currency_no_guanw():
    """ 检查有官网但没有介绍的币种 """
    sql_filter = {
        Currency.guanw != '',
        Currency.guanw.isnot(None),
        Currency.record_status == 0,
        or_(Currency.introduce == '', Currency.introduce.is_(None))
    }
    sql_db = sql_session()
    ccy_trs = sql_db.query(
        Currency.id, Currency.currency_name).filter(
        *sql_filter).all()
    sql_db.close()
    # mailer.add_msg(
    #     title='有{}个币种需要更新资料',
    #     desc='以下币种缺少介绍等必要信息，请及时补充',
    #     th1='币种ID',:w
    #     th2='币种名',
    #     trs=ccy_trs
    # )


def check_exchange():
    """ 检查 没有行情的但状态为0的交易所 和 有行情但状态为1的交易所 """
    active_ids = {d['_id']: d['last_date'] for d in mongo_db['OpenQuotationInfo'].aggregate([
        {'$group': {'_id': '$egeId', 'last_date': {'$max': '$time'}}}
    ])}
    sql_db = sql_session()
    ege_query = sql_db.query(
        Exchange.id, Exchange.exchange_name, Exchange.record_status).all()
    sql_db.close()
    active_trs = list()
    inactive_trs = list()
    for ege_id, ege_name, status in ege_query:
        if status == 0 and ege_id not in active_ids:
            inactive_trs.append([ege_id, ege_name])
        elif status == 1 and ege_id in active_ids and \
                datetime.now() - datetime.strptime(
            active_ids[ege_id], '%Y-%m-%d %H:%M:%S') < timedelta(days=1):
            active_trs.append([ege_id, ege_name])

    for ege_id, _ in active_trs:
        sql_db.query(Exchange).filter_by(id=ege_id).update({
            'record_status': 0})
    for ege_id, _ in inactive_trs:
        sql_db.query(Exchange).filter_by(id=ege_id).update({
            'record_status': 1})
    sql_db.commit()

    # mailer.add_msg(
    #     title='有{}个交易所没有行情更新',
    #     desc='请检查下列数据，如确定退市可暂时隐藏',
    #     th1='交易所ID',
    #     th2='交易所名',
    #     trs=inactive_trs
    # )
    # mailer.add_msg(
    #     title='有{}个交易所出现行情更新',
    #     desc='下列隐藏数据有最新更新，请及时完善信息然后解除隐藏',
    #     th1='交易所ID',
    #     th2='交易所名',
    #     trs=active_trs
    # )



def check_activity():
    check_currency()
    # check_currency_no_guanw()
    check_exchange()
    # mailer.send_mails()


if __name__ == '__main__':
    check_activity()
