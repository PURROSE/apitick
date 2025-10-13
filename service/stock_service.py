import akshare as ak
import dbtool.obj_class as obj_class
from dbtool.tab_moudel import StockData, StockDataHis, StockInfo
from datetime import datetime, timedelta
from loguru import logger

def task():
    # input_realtime_data();
    # input_history_data(code="600900", period="daily", start_date="20240101", end_date="20251001")
    input_stock_info(code="600900")

# 接口数据转换成字典数据--适用于列表数据
def resdata_to_dis(res):
    converted_data = []
    # 获取列名
    columns = list(res.keys())
    # 获取行数
    num_rows = len(res[columns[0]])
    # 遍历每一行，生成字典
    for i in range(num_rows):
        row = {col: res[col][i] for col in columns}
        converted_data.append(row)
    return converted_data

# 接口数据转换成字典数据--适用于单数据
def resdata_to_one_dis(res):
    converted_data = {}
    # 获取列名
    columns = list(res.keys())
    # 获取行数
    num_rows = len(res[columns[0]])

    for i in range(0, num_rows):
        converted_data[res[columns[0]][i]] = res[columns[1]][i]
    return converted_data

# 生成日期字符串列表
def generate_date_strings(start_date, end_date):
    """生成日期范围内的所有日期字符串（格式：YYYYMMDD）"""
    date_list = []
    current_date = start_date
    while current_date <= end_date:
        date_list.append(current_date.strftime("%Y%m%d"))
        current_date += timedelta(days=1)
    return date_list

# 获取实时行情数据
def input_realtime_data():
    logger.info(f"获取A股实时行情数据--Start")
    stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()
    logger.info(f"获取A股实时行情数据--END")
    obj_class.mergin_list_data(resdata_to_dis(stock_zh_a_spot_em_df), StockData)
    logger.info("实时数据获取完毕")

# 获取历史行情数据
def input_history_data(code, period, start_date, end_date):
    logger.info(f"获取A股历史行情数据，代码：{code}，周期：{period}，开始日期：{start_date}，结束日期：{end_date}")
    stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol=code, period=period, start_date=start_date, end_date=end_date, adjust="")
    logger.info(f"获取A股历史行情数据：{stock_zh_a_hist_df}")
    obj_class.mergin_list_data(resdata_to_dis(stock_zh_a_hist_df), StockDataHis)
    logger.info("历史数据获取完毕")

def input_stock_info(code):
    logger.info(f"获取股票数据")
    stock_info_a_em_df = ak.stock_individual_info_em(symbol=code)
    logger.info(f"获取股票列表数据--Start: {resdata_to_one_dis(stock_info_a_em_df)}")
    obj_class.mergin_one_data(resdata_to_one_dis(stock_info_a_em_df), StockInfo)
    print("股票列表数据获取完毕")