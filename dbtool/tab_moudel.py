from sqlalchemy import Column, Date, DateTime
from sqlalchemy import Column, Integer, String, Double, Numeric, BigInteger, TIMESTAMP, DateTime
from sqlalchemy.ext.declarative import declarative_base
from loguru import logger
from datetime import datetime
import numpy as np

# 先定义Base为None，稍后在obj_class.py中赋值
Base = declarative_base()

class ObjectOrResult:
    def res_to_obj(res):
        """将数据库查询结果转换为字典对象"""
        if res is None:
            return None
        return {column.comment: getattr(res, column.value) for column in res.__table__.columns}

    def obj_to_res(obj, res):
        obj = {k: float(v) if isinstance(v, np.float64) else v for k, v in obj.items()}
        obj = {k: int(v) if isinstance(v, np.int64) else v for k, v in obj.items()}

        """将字典对象转换为数据库模型对象"""
        for column in res.__table__.columns:
            if column.comment in obj:
                if obj[column.comment] == 'None' :
                    setattr(res, column.name, None)
                else:
                    setattr(res, column.name, obj[column.comment])
        
        return res

# 实时股票数据模型，获取所有股票的接口
class StockData(Base):
    """
    股票数据表
    """
    __tablename__ = 'tab_stock_data'

    # 股票代码
    code = Column(String(20),primary_key=True, nullable=False, comment='股票代码')
    # 股票名称
    name = Column(String(500), nullable=False, comment='股票名称')
    # 最新价
    latest_price = Column(Numeric(15, 4), nullable=False, comment='最新价')
    # 涨跌幅
    price_change_rate = Column(Numeric(15, 4), nullable=False, comment='涨跌幅')
    # 涨跌额
    price_change_amount = Column(Numeric(15, 4), nullable=False, comment='涨跌额')
    # 成交量
    trade_volume = Column(Numeric(20, 4), nullable=False, comment='成交量')
    # 成交额
    trade_amount = Column(Numeric(20, 4), nullable=False, comment='成交额')
    # 市盈率-动态
    pe_dynamic = Column(Numeric(15, 4), comment='市盈率-动态')
    # 市净率
    pb_ratio = Column(Numeric(15, 4), comment='市净率')
    # 总市值
    total_market_value = Column(Numeric(20, 4), comment='总市值')
    # 流通市值
    circulating_market_value = Column(Numeric(20, 4), comment='流通市值')
    # 涨速
    price_increase_speed = Column(Numeric(15, 4), comment='涨速')
    # 5分钟涨跌
    five_minute_price_change = Column(Numeric(15, 4), comment='5分钟涨跌')
    # 60日涨跌幅
    sixty_day_price_change_rate = Column(Numeric(15, 4), comment='60日涨跌幅')
    # 年初至今涨跌幅
    year_to_date_price_change_rate = Column(Numeric(15, 4), comment='年初至今涨跌幅')

# 历史股票数据模型，获取单只股票的历史数据接口
class StockDataHis(Base):
    """
    股票数据表
    """
    __tablename__ = 'tab_stock_data_his'

    # 交易日期
    stock_date = Column(Date(), primary_key=True, nullable=False, comment='日期')
    # 股票代码
    stock_code = Column(String(20), primary_key=True, nullable=False, comment='股票代码')
    # 开盘价
    open_price = Column(Numeric(15, 4), nullable=False, comment='开盘')
    # 收盘价
    close_price = Column(Numeric(15, 4), nullable=False, comment='收盘')
    # 最高价
    high_price = Column(Numeric(15, 4), nullable=False, comment='最高')
    # 最低价
    low_price = Column(Numeric(15, 4), nullable=False, comment='最低')
    # 成交量，单位：手
    trade_volume = Column(Numeric(20, 4), nullable=False, comment='成交量')
    # 成交额，单位：元
    trade_amount = Column(Numeric(20, 4), nullable=False, comment='成交额')
    # 振幅，单位：%
    amplitude = Column(Numeric(15, 4), nullable=False, comment='振幅')
    # 涨跌幅，单位：%
    price_change_rate = Column(Numeric(15, 4), nullable=False, comment='涨跌幅')
    # 涨跌额，单位：元
    price_change_amount = Column(Numeric(15, 4), nullable=False, comment='涨跌额')
    # 换手率，单位：%
    turnover_rate = Column(Numeric(15, 4), nullable=False, comment='换手率')

# 股票信息模型，获取单只股票的基本信息接口
class StockInfo(Base):
    """
    股票信息表
    """
    __tablename__ = 'tab_stock_info'

    # 最新价
    latest = Column(Numeric(15, 4), nullable=False, comment='最新')
    # 股票代码
    stock_code = Column(String(20),primary_key=True, nullable=False, comment='股票代码')
    # 股票简称
    stock_name = Column(String(100), nullable=False, comment='股票简称')
    # 总股本
    total_shares = Column(Numeric(20, 4), nullable=False, comment='总股本')
    # 流通股
    circulating_shares = Column(Numeric(20, 4), nullable=False, comment='流通股')
    # 总市值
    total_market_value = Column(Numeric(20, 4), nullable=False, comment='总市值')
    # 流通市值
    circulating_market_value = Column(Numeric(20, 4), nullable=False, comment='流通市值')
    # 行业
    industry = Column(String(100), nullable=False, comment='行业')
    # 上市时间
    listing_date = Column(Date, nullable=False, comment='上市时间')