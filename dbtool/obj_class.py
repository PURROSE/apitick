# -*- coding: utf-8 -*-
from loguru import logger
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
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