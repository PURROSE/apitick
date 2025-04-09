from sqlalchemy import Column, Integer, String, Double
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Date, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession
from rich.progress import Progress

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
        for index, rows in stock_gsrl_gsdt.iterrows() :
            exists_record = session.query(StockGsrlGsdt).filter(
                StockGsrlGsdt.stock_code == rows.stock_code,
                StockGsrlGsdt.num == rows.num,
                StockGsrlGsdt.msg_date == rows.msg_date
            ).first()
            if exists_record:
                # 如果记录已存在，则更新数据
                exists_record.msg = rows.msg
                exists_record.msg_type = rows.msg_type
                exists_record.create_time = rows.create_time
            else:
                session.add(rows)

        session.commit()
    finally:
        session.close()


def save_stock_zh_a_spot(stock_zh_a_spot : list):
    try:
        with Progress() as progress:
            task = progress.add_task(description="[cyan]UpdateDb...", total=stock_zh_a_spot.__len__())
            for rows in stock_zh_a_spot :
                progress.update(task, advance=1)
                exists_record = session.query(StockZhASpot).filter(
                    StockZhASpot.stock_code == rows.stock_code
                ).first()
                if exists_record :
                    exists_record.new_price = rows.new_price
                    exists_record.rise_and_fall = rows.rise_and_fall
                    exists_record.increase_and_decrease = rows.increase_and_decrease
                    exists_record.trading_volume = rows.trading_volume
                    exists_record.turnover = rows.turnover
                    exists_record.amplitude = rows.amplitude
                    exists_record.max_price = rows.max_price
                    exists_record.min_price = rows.min_price
                    exists_record.open_price = rows.open_price
                    exists_record.close_price = rows.close_price
                    exists_record.turnover_rate = rows.turnover_rate
                    exists_record.price_earnings_ratio = rows.price_earnings_ratio
                    exists_record.B_ratio = rows.B_ratio
                    exists_record.total_market_value = rows.total_market_value
                    exists_record.the_market_capitalization = rows.the_market_capitalization
                    exists_record.rate_of_increase = rows.rate_of_increase
                    exists_record.Five_minutes_price = rows.Five_minutes_price
                    exists_record.six_day_price = rows.six_day_price
                    exists_record.year_increase_decrease = rows.year_increase_decrease
                    exists_record.update_time = rows.update_time
                    # 如果记录已存在，则更新数据
                else:
                    session.add(rows)
            session.commit()
    finally:
        session.close()