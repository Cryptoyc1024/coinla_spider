
"""
SQL ORM Model
"""

from datetime import datetime

from sqlalchemy import Column, String, Integer, \
    Float, ForeignKey, TIMESTAMP, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import text

Base = declarative_base()


class BaseModel(Base):
    __abstract__ = True

    update_date = Column(TIMESTAMP(True), server_default=text(
        'CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))
    create_date = Column(TIMESTAMP(True), server_default=text('CURRENT_TIMESTAMP'))
    update_by = Column(Integer, default=1, onupdate=1)
    create_by = Column(Integer, default=1)


class Currency(BaseModel):
    """ 币种 """

    __tablename__ = 'currency'

    id = Column('ccy_id', Integer, primary_key=True)
    # 中文名
    currency_name = Column(String(100))
    # 英文名
    english = Column(String(100))
    # 英文缩写
    short_name = Column(String(100))
    # 流通总量
    circulate_total = Column(String(20), default='0')
    # 流通市值
    circulate_total_value = Column(String(20), default='0')
    sort_circulate_total_value = Column(Float, default=0)
    # 总发行量
    total = Column(String(15), default='0')
    # 总市值
    total_value = Column(String(20), default='0')
    sort_total_value = Column(Float, default=0)
    # 上架交易所数量
    market_qty = Column(Integer, default=0)
    follow = Column(Integer, default=0)
    # 发行人
    initiate_person = Column(String(15))
    # 发行时间
    initiate_create_date = Column(String(15), default='0')
    # 共识机制
    consensus_mechanism = Column(String(500))
    # LOGO地址
    pic = Column(String(150))
    # 官网
    guanw = Column(String(250))
    # 区块站
    block_chain = Column(String(250))
    # 白皮书
    white_paper_zh = Column(String(250))
    white_paper_en = Column(String(250))
    # 官方社区
    official_community = Column(String(1500))
    # 介绍
    introduce = Column(Text)
    team_introduce = Column(Text)
    app_introduce = Column(Text)
    # 是否有募集
    is_ico = Column(Integer, default=0)
    # 募集数量
    ico = Column(String(50), default='未知')
    # 是否有团队(0-无,1-有)
    isTeam = Column(Integer, default=0)
    # 是否可挖矿(0-无,1-有)
    ismining = Column(Integer, default=0)
    # 募集价格
    private_price = Column(String(50), default='未知')
    # 募集总价值
    private_total_value = Column(String(200), default='未知')
    # 募集时间
    private_time = Column(String(50), default='未知')
    # 状态
    record_status = Column(Integer, default=0)
    # 版本
    version = Column(Integer, default=0)

    def __repr__(self):
        return '{}(ccy_id={}, short_name={}, en_name={})'.format(
            self.__class__.__name__, self.id, self.short_name, self.english)


class CurrencyName(BaseModel):
    """ 币种名称 """

    __tablename__ = 'currency_name'

    id = Column('cn_id', Integer, primary_key=True)
    # 英文名
    english = Column(String(100))
    # 英文缩写
    short_name = Column(String(50))
    # 币种ID
    ccy_id = Column(Integer, default=0)
    # 交易所ID
    ege_id = Column(Integer, default=0)
    # 币种名来源
    origin = Column(String(100))
    # 币种名来源的链接
    origin_url = Column(String(250))

    def __repr__(self):
        return '{}(cn_id={}, ccy_id={}, short_name={})'.format(
            self.__class__.__name__, self.id, self.ccy_id, self.short_name)


class Exchange(BaseModel):
    """ 交易所 """

    __tablename__ = 'exchange'

    id = Column('ege_id', Integer, primary_key=True)
    exchange_name = Column(String(100))
    exchange_name_zhhk = Column(String(100))
    exchange_name_en = Column(String(100))
    # 交易额
    turnover = Column(String(100), default='未知')
    sort_turnover = Column(Float, default=0)
    # 交易对数量
    transaction_pair = Column(Integer, default=0)
    # 交易额排名
    ranking = Column(Integer, default=0)
    # 星级
    level = Column(Integer, default=1)
    # 排序
    sort = Column(Integer, default=8)
    country = Column(String(100))
    link = Column(String(150))
    # 手续费链接
    deal_link = Column(String(150))
    pic = Column(String(150))
    # 交易手续费图片
    pic_deal = Column(String(150))
    # 存取手续费图片
    pic_access = Column(String(150))
    introduce = Column(Text)
    # 是否有期货(0-是,1-否)
    futures = Column(Integer, default=1)
    # 是否有现货(0-是,1-否)
    stock = Column(Integer, default=1)
    # 是否支持法币(0-是,1-否)
    legal_tender = Column(Integer, default=1)
    record_status = Column(Integer, default=0)
    version = Column(Integer, default=0)

    def __repr__(self):
        return '{}(ege_id={}, exchange_name={})'.format(
            self.__class__.__name__, self.id, self.exchange_name)


class CurrencyExchangeRelation(BaseModel):
    """ 交易对 """

    __tablename__ = 'currency_exchange_relation'

    id = Column('cer_id', Integer, primary_key=True)
    currency_id = Column(Integer, ForeignKey('currency.ccy_id'))
    currency = relationship('Currency', foreign_keys=currency_id, uselist=False)
    exchange_id = Column(Integer, ForeignKey('exchange.ege_id'))
    exchange = relationship('Exchange', foreign_keys=exchange_id, uselist=False)
    currency_right_id = Column(Integer, ForeignKey('currency.ccy_id'))
    currency_right = relationship('Currency', foreign_keys=currency_right_id, uselist=False)

    def __repr__(self):
        return '{}(cer_id={}, currency_id={}, exchange_id={})'.format(
            self.__class__.__name__, self.id, self.currency_id, self.exchange_id)


class ModuleConcept(BaseModel):
    """ 板块概念 """

    __tablename__ = 'module_concept'

    id = Column('mc_id', Integer, primary_key=True)
    mc_name = Column(String(255))
    sort = Column(Integer, default=99)

    def __repr__(self):
        return '{}(mc_id={}, mc_name={})'.format(
            self.__class__.__name__, self.id, self.mc_name)


class ModuleCurrencyMiddle(BaseModel):
    """ 概念与币种关系 """

    __tablename__ = 'module_currency_middle'

    id = Column('mcm_id', Integer, primary_key=True)
    mc_id = Column(Integer, ForeignKey('module_concept.mc_id'))
    mc = relationship('ModuleConcept', uselist=False)
    ccy_id = Column(Integer, ForeignKey('currency.ccy_id'))
    ccy = relationship('Currency', uselist=False)

    def __repr__(self):
        return '{}(mcm_id={}, mc_id={}, ccy_id={})'.format(
            self.__class__.__name__, self.id, self.mc_id, self.ccy_id)


class HeadNewsLetter(BaseModel):
    """ 快讯 """

    __tablename__ = 'head_newsletter'

    id = Column('newsletter_id', Integer, primary_key=True)
    title = Column(String(50))
    content = Column(Text)
    # 来源
    source = Column(String(200))
    # 利好比例
    proportion = Column(Integer, default=50)
    # 利好数
    good_number = Column(Integer, default=0)
    # 利空数
    bad_number = Column(Integer, default=0)
    # 上架状态
    shelf_state = Column(Integer, default=1)
    # 展示时间
    show_date = Column(TIMESTAMP(True), default=datetime.now())

    def __repr__(self):
        return '{}(newsletter_id={}, title={}, proportion={})'.format(
            self.__class__.__name__, self.id, self.title, self.proportion)


class HeadNotice(BaseModel):
    """ 交易所公告 """

    __tablename__ = 'head_notice'

    id = Column('notice_id', Integer, primary_key=True)
    ege_id = Column(Integer, ForeignKey('exchange.ege_id'))
    exchange = relationship('Exchange', uselist=False)
    notice_title = Column(String(100))
    notice_content = Column(Text)
    notice_date = Column(TIMESTAMP(True),
                         default=datetime.now().replace(
                             hour=0, minute=0, second=0))
    original_link = Column(String(500))

    def __repr__(self):
        return '{}(notice_id)={}, ege_id={}, notice_title={})'.format(
            self.__class__.__name__, self.id, self.ege_id, self.notice_title[:8])
