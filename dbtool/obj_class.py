# -*- coding: utf-8 -*-
from loguru import logger
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, select
from sqlalchemy.ext.asyncio import AsyncSession
from rich.progress import Progress
from dbtool.tab_moudel import Base, ObjectOrResult

engine = create_engine('postgresql+psycopg2://apitick:GMCAPITICK001@www.purplerosechen.com:54329/apitick'
                       ,pool_size=50
                       ,max_overflow=5
                       ,pool_timeout=30
                       ,echo=True
                       )

AsyncSessionLocal = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False
)

def initialize_database():
    """检查并创建所有模型对应的表"""
    Base.metadata.create_all(engine)
    print("表已创建/检查完毕！")

Session = sessionmaker(bind=engine)

def mergin_list_data(data_list, table_class):
    try:
        session = Session()
        # 遍历数据列表，检查每条记录是否已存在
        for rows in data_list:
            data = ObjectOrResult.obj_to_res(rows, table_class())
            session.merge(data)
        session.commit()
    finally:
        session.close()

def mergin_one_data(data, table_class):
    try:
        session = Session()
        # 遍历数据列表，检查每条记录是否已存在
        data = ObjectOrResult.obj_to_res(data, table_class())
        session.merge(data)
        session.commit()
    finally:
        session.close()

# 通用查询函数
def query_all_stock_codes(table_class, *, filter_by=None, filters=None, order_by=None, limit=None):
    """
    通用查询
    - table_class: 模型类
    - filter_by: dict，等值过滤（等同于 SQLAlchemy 的 filter_by）
    - filters: list/tuple，SQLAlchemy 表达式列表，例如 [Model.code=='000001', Model.name.like('%测%')]
    - order_by: SQLAlchemy order_by 表达式，例如 Model.code.desc()
    - limit: int
    返回 ObjectOrResult.res_to_obj() 转换后的列表
    """
    try:
        session = Session()
        q = session.query(table_class)
        if filter_by:
            q = q.filter_by(**filter_by)
        if filters:
            q = q.filter(*filters)
        if order_by:
            q = q.order_by(order_by)
        if limit:
            q = q.limit(limit)
        results = q.all()
        return results
    finally:
        session.close()

def get_subQuery(table_class, *, filter_by=None, filters=None, order_by=None, limit=None):
    """
    获取子查询对象
    - table_class: 模型类
    - filter_by: dict，等值过滤（等同于 SQLAlchemy 的 filter_by）
    - filters: list/tuple，SQLAlchemy 表达式列表，例如 [Model.code=='000001', Model.name.like('%测%')]
    - order_by: SQLAlchemy order_by 表达式，例如 Model.code.desc()
    - limit: int
    返回 SQLAlchemy 子查询对象
    """
    try:
        session = Session()
        q = session.query(table_class)
        if filter_by:
            q = q.filter_by(**filter_by)
        if filters:
            q = q.filter(*filters)
        if order_by:
            q = q.order_by(order_by)
        if limit:
            q = q.limit(limit)
        return q.subquery()
    finally:
        session.close()