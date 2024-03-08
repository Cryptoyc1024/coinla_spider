# -*- coding: utf-8 -*-
# MYSQL_URI = 'mysql+mysqldb://root:66F(v+6#geW4Fa,R@localhost:3306/Coinla?charset=utf8'
# MONGO_URI = 'mongodb://root:coinla2018@localhost:27017/' \
#             'coinla?authSource=admin'
#
# CACHE_HOST = 'localhost'
# CACHE_PORT = 7379
# CACHE_PASSWORD = 'coinla2018'
# CACHE_DIR = 'SpiderCache'
#
# REDIS_DB_HOST = 'localhost'
# REDIS_DB_PORT = 7381
# REDIS_DB_PASSWORD = 'coinla2018'
#
# globals()
# REDIS_INTERNAL_HOST = 'localhost'
# REDIS_INTERNAL_PORT = 6379
# REDIS_INTERNAL_PASSWORD = 'coinla2018'
#
# MQ_URI = 'failover:tcp://localhost:61613?randomize=false,startupMaxReconnectAttempts=5'
# MQ_QUEUE = '/queue/CurrencyQuotation'

MYSQL_URI = 'mysql+mysqldb://root:mysql@localhost:3306/Coinla?charset=utf8'
MONGO_URI = 'mongodb://root:coinla2018@10.0.0.22:27017/' \
            'coinla?authSource=admin'

CACHE_HOST = 'localhost'
CACHE_PORT = 7379
CACHE_PASSWORD = 'coinla2018'
CACHE_DIR = 'SpiderCache'

REDIS_DB_HOST = 'localhost'
REDIS_DB_PORT = 7381
REDIS_DB_PASSWORD = 'coinla2018'

globals()
REDIS_INTERNAL_HOST = 'localhost'
REDIS_INTERNAL_PORT = 6379
REDIS_INTERNAL_PASSWORD = 'coinla2018'

MQ_URI = 'failover:tcp://localhost:61613?randomize=false,startupMaxReconnectAttempts=5'
MQ_QUEUE = '/queue/CurrencyQuotation'

# SPLASH_URL = 'http://47.75.8.116:8050'

# 交易对可能存在的法币(简称：[英文全称, 中文名])
LEGAL_CURRENCY = {
    'CNY': ['CNY', '人民币'], 'USD': ['USD', '美元'], 'JPY': ['JPY', '日元'],
    'KRW': ['KRW', '韩元'], 'HKD': ['HKD', '港元'], 'SGD': ['SGD', '新加坡元'],
    'INR': ['INR', '卢比'], 'RUB': ['RUB', '卢布'], 'RUR': ['RUR', '卢布'],
    'EUR': ['EUR', '欧元'], 'CHF': ['CHF', '法郎'], 'GBP': ['GBP', '英镑'],
    'CAD': ['CAD', '加元'], 'IDR': ['IDR', '印尼盾'], 'BRL': ['BRL', '巴西雷亚索'],
    'TWD': ['TWD', '新台币'], 'MXN': ['MXN', '比索'], 'AUD': ['AUD', '澳元'],
}

# 作为全网交易所的名称(中文, 英文)
COMMON_EXCHANGE = ['全网', 'All']

# 作用国行交易所的名称(中文, 英文)
OTC_EXCHANGE = ['国行', 'OTC']

EXCLUDE_URL = [
    'https://www.feixiaohao.com/currencies/mrtoken/'
]

# 是否另外输出DEBUG日志
DEBUG_OUTPUT = True

ALLOW_PIC = False
ALLOW_UPDATE_SQL = False

# 通用邮件
# MAIL_FROM = ''
# MAIL_HOST = ''
# MAIL_PORT = 465
# MAIL_USER = ''
# MAIL_PASS = ''
# MAIL_SSL = False

# 跟踪记录报错
ERR_TRACK_ENABLED = True

# 忽略有下列关键词的报错(同时作用于邮件和'errors.log'输出)
IGNORE_EXC = []

# 网络异常超过比率进行记录
HTTP_EXC_RATIO = 0.3

# 新闻推送邮件
NEWS_MAIL_TO = []

