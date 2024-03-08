# -*- coding: utf-8 -*-

BOT_NAME = 'coinla_spider'

SPIDER_MODULES = ['coinla_spider.spiders']
NEWSPIDER_MODULE = 'coinla_spider.spiders'

# LOG_FILE = 'log.txt'
# LOG_FORMAT = '%(asctime)s [%(module)s] %(levelname)s: %(message)s'
LOG_LEVEL = 'WARNING'

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# Configure maximum concurrent requests performed by Scrapy (default: 16)
CONCURRENT_REQUESTS = 16
CONCURRENT_ITEMS = 100
REACTOR_THREADPOOL_MAXSIZE = 300

# Configure a delay for requests for the same website (default: 0)
DOWNLOAD_DELAY = 0.2
DOWNLOAD_TIMEOUT = 10
# The download delay setting will honor only one of:
#CONCURRENT_REQUESTS_PER_DOMAIN = 16
#CONCURRENT_REQUESTS_PER_IP = 16

DNSCACHE_SIZE = 500000
DNS_TIMEOUT = 60

# Disable cookies (enabled by default)
COOKIES_ENABLED = False
REDIRECT_ENABLED = False

# Disable Telnet Console (enabled by default)
TELNETCONSOLE_ENABLED = False

# Override the default request headers:
DEFAULT_REQUEST_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'Accept-Encoding': 'gzip, deflate',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
    'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36'
}

# Enable or disable downloader middlewares
DOWNLOADER_MIDDLEWARES = {
    'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
    'coinla_spider.middlewares.RandomUserAgentMiddleware': 110,
}

EXTENSIONS = {
    'scrapy.extensions.telnet.TelnetConsole': None,
    'scrapy.extensions.closespider.CloseSpider': 999,
    'coinla_spider.loggers.Logger': 1,
}
