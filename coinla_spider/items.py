# -*- coding: utf-8 -*-

"""
字段为了与 Mongo 数据库字段一致
方便可以直接将 item 传入 pymongo
所以部分使用驼峰命名
"""

from scrapy import Field

from .formatters import *


class CurrencyItem(FormatItem):
    # Mongo文档名
    __collection__ = 'Currency'
    # MYSQL表ID
    ccyId = Field(format=Integer)
    # 通用名
    currencyName = Field(nullable=False)
    # 英文名
    english = Field(nullable=False)
    # 缩写名
    shortName = Field(nullable=False)
    # 流通总量 缩写
    circulateTotal = Field(format=AbbreviateNumber, default='0')
    # 流通市值 缩写
    circulateTotalValue = Field(format=AbbreviateNumber, default='0')
    # 流通市值 原始值
    sortCirculateTotalValue = Field(format=Float, default=0)
    # 总发行量 缩写
    total = Field(format=AbbreviateNumber(ndigits=0), default='0')
    # 总市值 缩写
    totalValue = Field(format=AbbreviateNumber, default='0')
    # 总市值 原始值
    sortTotalValue = Field(format=Float, default=0)
    # 上架交易所数
    numberOfExchange = Field(format=Integer, default=0)
    # 关注人数
    follow = Field(format=Integer, default=0)
    # 发行人
    initiatePerson = Field()
    # 发行时间
    initiateCreateDate = Field()
    # 板块概念
    concept = Field()
    # 共识机制
    consensusMechanism = Field()
    # LOGO
    pic = Field()
    # 官网
    guanw = Field(format=HttpUrl)
    # 区块站
    blockChain = Field(format=HttpUrl)
    # 白皮书中文
    whitePaperZh = Field(format=HttpUrl)
    # 白皮书英文
    whitePaperEn = Field(format=HttpUrl)
    # 官方社区
    officialCommunity = Field(format=HttpUrl)
    # 介绍
    introduce = Field()
    # 团队介绍
    teamIntroduce = Field()
    # APP介绍
    appIntroduce = Field()
    # 是否有募集
    isIco = Field(format=Integer, default=0)
    # 募集数量
    ico = Field(default='未知')
    # 募集价格
    privatePrice = Field(default='未知')
    # 募集总值
    privateTotalValue = Field(default='未知')
    # 募集时间
    privateTime = Field(default='未知')
    # 是否有团队
    isTeam = Field(format=Integer, default=0)
    # 是否可挖矿
    ismining = Field(format=Integer, default=0)
    # 状态(0:正常; 1:删除)
    recordStatus = Field(format=Integer, default=0)
    # 版本号
    version = Field(format=Integer, default=0)
    createDate = Field(default=datetime.now())
    updateDate = Field(init=True, default=datetime.now())
    createBy = Field(format=Integer, default=1)
    updateBy = Field(format=Integer, init=True, default=1)


class ExchangeItem(FormatItem):
    __collection__ = 'Exchange'
    egeId = Field(format=Integer)
    exchangeName = Field(nullable=False)
    exchangeNameZh = Field(nullable=False)
    exchangeNameEn = Field(nullable=False)
    # 交易所24H成交额
    turnover = Field(format=AbbreviateNumber, default='0')
    turnoverUsd = Field(format=AbbreviateNumber, default='0')
    sortTurnover = Field(format=Float, default=0)
    # 交易对数
    transactionPair = Field(format=Integer, default=0)
    # 成交量排名
    ranking = Field(format=Integer, default=0)
    # 星级
    level = Field(format=Integer, default=0)
    # 排序
    sort = Field(format=Integer, default=0)
    # 国家
    country = Field(default='未知')
    # 链接
    link = Field(format=HttpUrl)
    # 手续费链接
    dealLink = Field(format=HttpUrl)
    # LOGO
    pic = Field()
    # 交易手续费图片
    picDeal = Field()
    # 存取手续费图片
    picAccess = Field()
    # 介绍
    introduce = Field()
    # 有无期货(0:是; 1:否)
    futures = Field(format=Integer, default=1)
    # 有无现货(0:是; 1:否)
    stock = Field(format=Integer, default=1)
    # 是否接受法币(0:是; 1:否)
    legalTender = Field(format=Integer, default=1)
    # 状态(0:正常; 1:删除)
    recordStatus = Field(format=Integer, default=0)
    # 版本号
    version = Field(format=Integer, default=0)
    createDate = Field(default=datetime.now())
    updateDate = Field(init=True, default=datetime.now())
    createBy = Field(format=Integer, default=1)
    updateBy = Field(format=Integer, init=True, default=1)


class QuotationItem(FormatItem):
    __collection__ = 'OpenQuotationInfo'
    url = Field(nullable=False)
    pair_left = Field(nullable=False)
    pair_left_en = Field()
    pair_left_id = Field(format=Integer)
    pair_right = Field(nullable=False)
    ege_id = Field(format=Integer)
    exchange_name = Field(nullable=False)
    price_cny = Field(format=Price, default=0)
    price_usd = Field(format=Price, default=0)
    price_origin = Field(format=Float, default=0)
    # 成交额
    turnover_cny = Field(format=Float, default=0)
    turnover_usd = Field(format=Float, default=0)
    # 涨跌幅
    change = Field(format=Float, default=0)
    # 成交总量
    volume = Field(format=Float, default=0)
    # 成交额占比
    ratio = Field(format=Float(ndigits=2), default=0)
    # 更新时间
    update_time = Field(format=TimeStrToDatetime, init=True,
                        default=datetime.now())


class CurrencyCoreDataItem(FormatItem):
    __collection__ = 'CurrencyCoreData'
    ccy_id = Field(format=Integer, nullable=False)
    # 法币类型
    type = Field(format=Integer)
    # 上架交易所数量
    market_qty = Field(format=Integer, default=0)
    # 流通率
    circulate_ratio = Field(format=Float(ndigits=2))
    # 换手率(成交额占比)
    turnover_ratio_24h = Field(format=Float(ndigits=2))
    # 总市值
    total_value = Field(format=AbbreviateNumber, default='0')
    sort_total_value = Field(format=Float, default=0)
    # 总市值占比
    total_value_ratio = Field(format=Float(ndigits=2))
    # 流通总量
    circulate_total = Field(format=AbbreviateNumber, default='0')
    sort_circulate_total = Field(format=Float, default=0)
    # 流通总值
    circulate_total_value = Field(format=AbbreviateNumber, default='0')
    sort_circulate_total_value = Field(format=Float, default=0)
    # 流通总值排名
    circulate_total_value_rank = Field(format=Integer)
    # 相关交易对占比
    pair_ratio = Field()
    update_time = Field(init=True, default=datetime.now())


class EventItem(FormatItem):
    __collection__ = 'CurrencyCoreData'
    ccy_id = Field(format=Integer, nullable=False)
    # 事件
    event = Field()


class DevelopDataItem(FormatItem):
    __collection__ = 'CurrencyCoreData'
    ccy_id = Field(format=Integer, nullable=False)
    holder_topten_share = Field(format=Float, default=0)
    has_holder_list = Field(format=Integer, default=0)
    holder_address = Field(default={'total': 0, 'trend': 0})
    github_newest_time = Field(default='')
    github_commit = Field(default={'total': 0, 'trend': 0})
    github_contributor = Field(default={'total': 0, 'trend': 0})
    github_follow = Field(default={'total': 0, 'trend': 0})
    github_star = Field(default={'total': 0, 'trend': 0})
    github_fork = Field(default={'total': 0, 'trend': 0})
    reddit_sub = Field(default={'total': 0, 'trend': 0})
    twitter_follow = Field(default={'total': 0, 'trend': 0})
    facebook_thumb = Field(default={'total': 0, 'trend': 0})
    update_time = Field(init=True, default=datetime.now())


class ChartItem(FormatItem):
    __collection__ = 'KLineCrawlDay'
    ccy_id = Field(format=Integer, nullable=False)
    ccy_en_name = Field(nullable=False)
    ccy_short_name = Field(nullable=False)
    data_list = Field()
    last_data_time = Field()


class ConceptItem(FormatItem):
    concept_name = Field(nullable=False)
    ccy_urls = Field()


class OTCPriceItem(FormatItem):
    ccy_short_name = Field(nullable=False)
    otc_price = Field(format=Float, nullable=False)


class CurrencyDailyDataItem(FormatItem):
    __collection__ = 'CurrencyHistoryDay'
    doc_id = Field()
    ccy_id = Field(format=Integer)
    ccy_short_name = Field(nullable=False)
    ccy_en_name = Field(nullable=False)
    data_list = Field()
    last_data_time = Field(nullable=False)


class ExrateItem(FormatItem):
    ccy_short_name = Field(nullable=False)
    ccy_cn_name = Field(nullable=False)
    exrate = Field(format=Float)


class HolderItem(FormatItem):
    __collection__ = 'HolderList'
    ccy_id = Field(format=Integer)
    rank = Field(format=Integer, nullable=False)
    address = Field(nullable=False)
    balance = Field(format=Float, nullable=False)
    share = Field(format=Float(ndigits=4))


class NewsItem(FormatItem):
    title = Field(format=NoSpaceString, nullable=False)
    content = Field(format=NoSpaceString, nullable=False)
    # 快讯时间
    show_date = Field(nullable=False)
    # 来源
    source = Field(nullable=False)
    # 利好比例
    proportion = Field(nullable=False)


class NoticeItem(FormatItem):
    ege_id = Field(format=Integer, nullable=False)
    notice_title = Field(format=NoSpaceString, nullable=False)
    notice_content = Field(format=NoSpaceString, nullable=False)
    notice_date = Field(nullable=False)
    original_link = Field(nullable=False)
