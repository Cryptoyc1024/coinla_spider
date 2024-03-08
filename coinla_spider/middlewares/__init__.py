# -*- coding: utf-8 -*-

from .ccy_daily_mid import CurrencyDailyDataDownloaderMiddleware
from .chart_mid import ChartDownloaderMiddleware
from .common_mid import RandomUserAgentMiddleware, DummyRequestMiddleware, \
    GetCcyIdMiddleware, ProxyMiddleware, ApiHeaderMiddleware
from .currency_mid import CurrencyDownloaderMiddleware, CurrencySpiderMiddleware
from .develop_mid import DevelopSpiderMiddleware
from .event_mid import EventDownloaderMiddleware
from .exchange_mid import ExchangeDownloaderMiddleware
from .holder_mid import HolderSpiderMiddleware
from .notice_mid import NoticeDownloaderMiddleware
from .otc_price_mid import OTCPriceDownloaderMiddleware, OKEXOTCPriceMiddleware
from .quotation_mid import QuotationSpiderMiddleware
from .splash_mid import SplashDownloaderMiddleware
