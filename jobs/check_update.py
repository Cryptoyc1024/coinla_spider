# -*- coding: utf-8 -*-

"""

"""

import os
import sys

current_url = os.path.dirname(__file__)
parent_url = os.path.abspath(os.path.join(current_url, os.pardir))
sys.path.append(parent_url)

# from jobs import cache_cli, logging_decorator
from jobs import cache_cli
from coinla_spider.settings import *
from email.mime.text import MIMEText
from email.utils import formataddr
from datetime import datetime
import smtplib

EXPIRE = 3600 * 24 * 8


def mail(to_addrs, text, subject, subtype='plain'):
    msg = MIMEText(text, _subtype=subtype)
    msg['From'] = formataddr(['Kai', MAIL_FROM])
    msg['Subject'] = subject
    mailer = smtplib.SMTP_SSL(MAIL_HOST, MAIL_PORT)
    mailer.login(MAIL_USER, MAIL_PASS)
    mailer.sendmail(MAIL_FROM, to_addrs, msg.as_string())
    mailer.quit()


def check_cny_cqn_cache():
    ttl = cache_cli.ttl('SpiderCache:NewestQuotation:-1-BTC-Bitcoin')
    if EXPIRE - ttl > 300 and cache_cli.exists('SpiderCache:Exception:CnyCQN-MailLock') is False:
        return False
    return True


def check_pair_cqn_cache():
    ttl = cache_cli.ttl('SpiderCache:NewestQuotation:3-BTC-USDT')
    if EXPIRE - ttl > 3600 * 3 and cache_cli.exists('SpiderCache:Exception:PairCQN-MailLock') is False:
        cache_cli.set('SpiderCache:Exception:PairCQN-MailLock', datetime.now(), ex=3600 * 24)
        return False
    return True



def check_update():
    to = ['kai@coinla.com']
    if check_cny_cqn_cache() is False:
        text = '全网行情更新异常'
        mail(to, text, text)
    if check_pair_cqn_cache() is False:
        text = '交易对行情更新异常'
        mail(to, text, text)


if __name__ == '__main__':
    check_update()
