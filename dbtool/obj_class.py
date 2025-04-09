from sqlalchemy import Column, Integer, String, Double
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Date, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession

engine = create_engine('postgresql+psycopg2://apitick:GMCAPITICK001@www.purplerosechen.com:54329/apitick'
                       ,pool_size=10
                       ,max_overflow=5
                       ,pool_timeout=30
                       #,echo=True
                       )

AsyncSessionLocal = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False
)

Base = declarative_base()

def initialize_database():
    """检查并创建所有模型对应的表"""
    Base.metadata.create_all(engine)
    print("表已创建/检查完毕！")

class StockGsrlGsdt(Base):
    # 数据中心-股市日历-公司动态
    __tablename__ = 't_stock_gsrl_gsdt'

    rl_id = Column(Integer, primary_key=True, autoincrement=True)
    stock_code = Column(String(10), nullable=False)
    stock_name = Column(String(20), nullable=False)
    num = Column(Integer, nullable=False)
    msg = Column(String(4000), nullable=False)
    msg_type = Column(String(10), nullable=False)
    msg_date = Column(Date, nullable=False)
    create_time = Column(DateTime, nullable=False)

class StockZhASpot(Base):
    # 沪深京 A 股-实时行情数据
    __tablename__ = 't_stock_zh_a_spot'

    stock_code = Column(String(10), primary_key=True, nullable=False)
    stock_name = Column(String(20), nullable=False)
    new_price = Column(Double, nullable=False)
    rise_and_fall = Column(Double, nullable=False)
    increase_and_decrease = Column(Double, nullable=False)
    trading_volume = Column(Double, nullable=False)
    turnover = Column(Double, nullable=False)
    amplitude = Column(Double, nullable=False)
    max_price = Column(Double, nullable=False)
    min_price = Column(Double, nullable=False)
    open_price = Column(Double, nullable=False)
    close_price = Column(Double, nullable=False)
    turnover_rate = Column(Double, nullable=False)
    price_earnings_ratio = Column(Double, nullable=False)
    B_ratio = Column(Double, nullable=False)
    total_market_value = Column(Double, nullable=False)
    the_market_capitalization = Column(Double, nullable=False)
    rate_of_increase = Column(Double, nullable=False)
    Five_minutes_price = Column(Double, nullable=False)
    six_day_price = Column(Double, nullable=False)
    year_increase_decrease = Column(Double, nullable=False)
    create_time = Column(DateTime, nullable=False)
    update_time = Column(DateTime, nullable=False)


Session = sessionmaker(bind=engine)
session = Session()

def save_stock_gsrl_gsdt(stock_gsrl_gsdt):
    try:
        exists_record = session.query(StockGsrlGsdt).filter(
            StockGsrlGsdt.stock_code == stock_gsrl_gsdt.stock_code,
            StockGsrlGsdt.num == stock_gsrl_gsdt.num,
            StockGsrlGsdt.msg_date == stock_gsrl_gsdt.msg_date
        ).first()
        if exists_record:
            # 如果记录已存在，则更新数据
            exists_record.msg = stock_gsrl_gsdt.msg
            exists_record.msg_type = stock_gsrl_gsdt.msg_type
            exists_record.create_time = stock_gsrl_gsdt.create_time
        else:
            session.add(stock_gsrl_gsdt)

        session.commit()
    finally:
        session.close()


def save_stock_zh_a_spot(stock_zh_a_spot):
    try:
        exists_record = session.query(StockZhASpot).filter(
            StockZhASpot.stock_code == stock_zh_a_spot.stock_code
        ).first()
        if exists_record :
            exists_record.new_price = stock_zh_a_spot.new_price
            exists_record.rise_and_fall = stock_zh_a_spot.rise_and_fall
            exists_record.increase_and_decrease = stock_zh_a_spot.increase_and_decrease
            exists_record.trading_volume = stock_zh_a_spot.trading_volume
            exists_record.turnover = stock_zh_a_spot.turnover
            exists_record.amplitude = stock_zh_a_spot.amplitude
            exists_record.max_price = stock_zh_a_spot.max_price
            exists_record.min_price = stock_zh_a_spot.min_price
            exists_record.open_price = stock_zh_a_spot.open_price
            exists_record.close_price = stock_zh_a_spot.close_price
            exists_record.turnover_rate = stock_zh_a_spot.turnover_rate
            exists_record.price_earnings_ratio = stock_zh_a_spot.price_earnings_ratio
            exists_record.B_ratio = stock_zh_a_spot.B_ratio
            exists_record.total_market_value = stock_zh_a_spot.total_market_value
            exists_record.the_market_capitalization = stock_zh_a_spot.the_market_capitalization
            exists_record.rate_of_increase = stock_zh_a_spot.rate_of_increase
            exists_record.Five_minutes_price = stock_zh_a_spot.Five_minutes_price
            exists_record.six_day_price = stock_zh_a_spot.six_day_price
            exists_record.year_increase_decrease = stock_zh_a_spot.year_increase_decrease
            exists_record.update_time = stock_zh_a_spot.update_time
            # 如果记录已存在，则更新数据
        else:
            session.add(stock_zh_a_spot)
        
        session.commit()
    finally:
        session.close()