
import dbtool.obj_class as obj_class
from loguru import logger
from gm.api import *

set_token = "8cf7d67a59e0811e0007e1bfea22c52f17e49aa8"

class StockService:
    def init(content): 
        print("StockService init", content)