# -*- coding: utf-8 -*-

"""
自动格式化 Item 字段封装的扩展类
作用是将 Item 改造成 ORM Model 类似的功能
可以省去在数据入库之前大部分格式化工作

使用方式：
1. 定义 scrapy.Item 继承类时，改为继承 FormatItem
2. scrapy.Field 里传入对应的参数，目前参数支持:
    format: 值为 Formatter 的子类，当字段每次传入键值时，子类会自动将值格式化;
    default: 字段的默认值，当传入 None 值时改为默认值;
    init: 是否自动初始化字段，bool 类型，默认 False，
          指定 True 后 Item 实例化后将自动初始化这个字段，可配合 default 参数实现初始值;
    nullable: 是否允许空值，bool 类型，默认 True，指定为 False 后，字段传入 None 会报错


example:
```
from scrapy import Field


class ExampleItem(FormatItem):

    # 字段格式化为整型
    id = Field(format=Integer)

    # 字段不能传入None值，否则报错
    name = Field(nullable=False)

    # 数字将自动缩写为 万 亿 为单位的字符串，并指定默认为'0'
    total_value = Field(format=AbbreviateNumber, default='0')

    # 初始化Item后，该字段自动生成，值为指定默认值
    update_date = Field(init=True, default=datetime.now())
```

"""

import re
from datetime import datetime, timedelta

from scrapy import Item


class FormatItem(Item):

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        for k, v in self.fields.items():
            if 'init' in v and self.fields[k]['init'] is True and k not in kw:
                self[k] = None

    def __setitem__(self, key, value):
        if key in self.fields:
            if value is None:
                if 'default' in self.fields[key]:
                    value = self.fields[key]['default']
            else:
                formatter = self.fields[key].get('format', None)
                if formatter is not None:
                    if isinstance(formatter, type) and issubclass(formatter, Formatter):
                        value = formatter().format(value)
                    elif isinstance(formatter, Formatter):
                        value = formatter.format(value)
                    else:
                        raise ValueError(
                            "{} must be Formatter's subclass".format(
                                formatter.__name__)
                        )
            if self.fields[key].get('nullable') is False and (value is None or value == ''):
                raise ValueError("{}['{}'] value can not be null".format(
                    self.__class__.__name__, key))
            self._values[key] = value
        else:
            raise KeyError("{} does not support field: {}".format(
                self.__class__.__name__, key))

    def copy(self):
        new_item = self.__class__()
        new_item._values = self._values.copy()
        return new_item


class Formatter(object):
    _num_pattern = r'[^\d\.eE\+\-]'

    def format(self, n):
        raise NotImplementedError('format must be implemented '
                                  'by Formatter subclasses')


class Integer(Formatter):

    def format(self, n):
        if isinstance(n, str):
            n = re.sub(self._num_pattern, '', n)
        return int(n)


class Float(Formatter):

    def __init__(self, ndigits=None):
        self.ndigits = ndigits

    def format(self, n):
        if isinstance(n, str):
            n = re.sub(self._num_pattern, '', n)
            n = float(n)
        if isinstance(n, float):
            if self.ndigits is not None:
                n = round(n, self.ndigits)
            # 判断小数位是否为0
            if int(n) == n:
                n = int(n)
        return n


class Price(Formatter):
    """ 按SQL规则保留小数点 """

    def format(self, n):
        if isinstance(n, str):
            n = float(re.sub(self._num_pattern, '', n))
        if n >= 10:
            n = round(n, 2)
        elif 1 <= n < 10:
            n = round(n, 4)
        else:
            n = round(n, 6)
        if int(n) == n:
            n = int(n)
        return n


class AbbreviationToFloat(Formatter):
    """ 将数字缩写的字符串转成Float """

    def format(self, n):
        multiple = 1
        if '万' in n:
            multiple *= 1e4
        if '亿' in n:
            multiple *= 1e8
        n = re.sub(self._num_pattern, '', n)
        return float(n) * multiple


class AbbreviateNumber(Formatter):
    """ 将数字缩写成`万`或`亿`，并保留指定小数位"""

    def __init__(self, ndigits=2):
        self.ndigits = ndigits

    def format(self, n):
        unit = str()
        if isinstance(n, str):
            n = float(re.sub(self._num_pattern, '', n))
        if n >= 1e12:
            n /= 1e12
            unit = '万亿'
        elif n >= 1e8:
            n /= 1e8
            unit = '亿'
        elif n >= 1e4:
            n /= 1e4
            unit = '万'
        elif n == 0:
            return '0'
        n = round(n, self.ndigits)
        if int(n) == n:
            n = int(n)
        return format(n, ',') + unit


class TimestampToDatetime(Formatter):
    """ 时间戳转Datetime """

    def format(self, n):
        n = int(n)
        if n > 1e10:
            n /= 1000
        return datetime.fromtimestamp(n)


class TimeStrToDatetime(Formatter):
    """ 将描述型的时间字符串转成Datetime """

    ago_comps = {
        '秒': 1,
        '分钟': 60,
        '小时': 3600,
        '天': 3600 * 24,
    }

    def format(self, time_str):
        time_str = time_str.strip()
        if '刚刚' in time_str:
            return datetime.now()
        elif '前' in time_str:
            date_time = self._ago_format(time_str)
        else:
            date_time = self._date_str_format(time_str)
        if date_time is None:
            raise ValueError('Wrong time string')
        return date_time

    def _ago_format(self, time_str):
        for k, v in self.ago_comps.items():
            num = re.findall(
                r'([0-9]+)\s*?' + k,
                time_str
            )
            if num:
                ago = timedelta(seconds=int(num[0]) * v)
                return datetime.now() - ago

    @staticmethod
    def _date_str_format(time_str):
        dt = re.findall(
            r'(\d{4})[\w/.-](\d{1,2})[\w/.-](\d{1,2})[\w/-]*',
            time_str
        )
        if dt:
            return datetime(
                year=int(dt[0][0]),
                month=int(dt[0][1]),
                day=int(dt[0][2])
            )


class HttpUrl(Formatter):
    """ 连接加HTTP头部 """

    def format(self, url):
        if url: 
            url = url.lower().strip()
            if re.match(r'//', url):
                url = 'https:' + url
            elif re.match(r'https*://', url) is None:
                url = 'https://' + url
        return url


class NoSpaceString(Formatter):
    """ 去除字符串所有空格 """

    def format(self, string):
        return re.sub(r'\s{3,}|(\\xa0)', '', string).strip()
