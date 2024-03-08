# -*- coding: utf-8 -*-

import os

PROJECT_ENV = os.environ.get('PROJECT_ENV', 'proj')

# 测试和开发环境
if PROJECT_ENV == 'test' or PROJECT_ENV == 'dev':
    REDIS_HOST = ''
    REDIS_PASSWORD = ''
    MYSQL_URI = ''
    MONGO_URI = ''
    LOG_LEVEL = 'DEBUG'

    EXTENSIONS = {
        'scrapy.extensions.telnet.TelnetConsole': None,
    }

# 生产环境
elif PROJECT_ENV == 'prod':
    REDIS_HOST = '127.0.0.1'
    MONGO_URI = ''
    SPLASH_URL = 'http://127.0.0.1:8050'
