
from sqlalchemy import Column, Integer, String, Double, Numeric, BigInteger, TIMESTAMP, DateTime
from sqlalchemy.ext.declarative import declarative_base
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
                if obj[column.comment] == 'None' or obj[column.comment] is None or obj[column.comment] == '-' or obj[column.comment] == '':
                    setattr(res, column.name, None)
                else:
                    setattr(res, column.name, obj[column.comment])
        
        return res
