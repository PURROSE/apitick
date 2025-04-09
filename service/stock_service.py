from dbtool.obj_class import StockGsrlGsdt
from dbtool.obj_class import StockZhASpot
import akshare as ak
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import traceback
from dbtool.obj_class import save_stock_gsrl_gsdt
from dbtool.obj_class import save_stock_zh_a_spot
from rich.progress import Progress

def task():
    #task_stock_gsrl_gsdt()
    task_stock_zh_a_spot()

def generate_date_strings(start_date, end_date):
    """生成日期范围内的所有日期字符串（格式：YYYYMMDD）"""
    date_list = []
    current_date = start_date
    while current_date <= end_date:
        date_list.append(current_date.strftime("%Y%m%d"))
        current_date += timedelta(days=1)
    return date_list

def task_stock_gsrl_gsdt():
    # 生成过去两年内的日期字符串（相对于今天）
    today = datetime.now()
    # two_years_ago = today - timedelta(days=365*2)  # 近似计算（不考虑闰年）
    # 更精确的方式（需安装 python-dateutil）：
    two_years_ago = today - relativedelta(months=1)
    date_list = generate_date_strings(two_years_ago, today)
    try:
        for date_str in date_list:
            try:
                # 调用 akshare 接口
                print(f"正在处理日期: {date_str}")
                # date_str = "20230808"  # 测试用例
                # 转换成date类型数据
                #date_str = datetime.strptime(date_str, "%Y%m%d").date()
                stock_gsrl_gsdt_em_df = ak.stock_gsrl_gsdt_em(date=date_str)
                if not stock_gsrl_gsdt_em_df.emmpty:
                    # 处理数据
                    dbdataList = []
                    for index, row in stock_gsrl_gsdt_em_df.iterrows():
                        stock_gsrl_gsdt = StockGsrlGsdt()
                        stock_gsrl_gsdt.num = row["序号"]
                        stock_gsrl_gsdt.stock_code = row["代码"]
                        stock_gsrl_gsdt.stock_name = row["简称"]
                        stock_gsrl_gsdt.msg_type = row["事件类型"]
                        stock_gsrl_gsdt.msg = row["具体事项"]
                        # 2023-08-08时间格式转换为日期对象
                        stock_gsrl_gsdt.msg_date = row["交易日"]
                        stock_gsrl_gsdt.create_time = datetime.now()
                        # 保存数据到数据库
                        dbdataList.append(stock_gsrl_gsdt)
                    save_stock_gsrl_gsdt(dbdataList)
            except Exception as e:
                # 异常处理，打印堆栈信息
                print("处理数据时发生异常：")
                print(f"Error processing data {date_str}: {e}")
    except Exception as e:
        # 异常处理，打印堆栈信息
        print("处理数据时发生异常：")
        print(f"Error processing data {date_str}: {e}")
        print(traceback.format_exc())

def task_stock_zh_a_spot():
    stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()
    try:
        print("正在处理数据")
        if not stock_zh_a_spot_em_df.empty:
            rowsize, columns = stock_zh_a_spot_em_df.shape
            print(f"获取到数据:{rowsize}")
            data_list = [StockZhASpot]  # Initialize an empty list to store processed data
            with Progress() as progress:
                task = progress.add_task(description="[cyan]Downloading...", total=rowsize)
                for index, rows in stock_zh_a_spot_em_df.iterrows() :
                    stockZhASpot = StockZhASpot()
                    stockZhASpot.stock_code = rows["代码"]
                    stockZhASpot.stock_name = rows["名称"]
                    stockZhASpot.new_price = rows["最新价"]
                    stockZhASpot.rise_and_fall = rows["涨跌幅"]
                    stockZhASpot.increase_and_decrease = rows["涨跌额"]
                    stockZhASpot.trading_volume = rows["成交量"]
                    stockZhASpot.turnover = rows["成交额"]
                    stockZhASpot.amplitude = rows["振幅"]
                    stockZhASpot.max_price = rows["最高"]
                    stockZhASpot.min_price = rows["最低"]
                    stockZhASpot.open_price = rows["今开"]
                    stockZhASpot.close_price = rows["昨收"]
                    stockZhASpot.turnover_rate = rows["量比"]
                    stockZhASpot.price_earnings_ratio = rows["换手率"]
                    stockZhASpot.B_ratio = rows["市净率"]
                    stockZhASpot.total_market_value = rows["总市值"]
                    stockZhASpot.the_market_capitalization = rows["流通市值"]
                    stockZhASpot.rate_of_increase = rows["涨速"]
                    stockZhASpot.Five_minutes_price = rows["5分钟涨跌"]
                    stockZhASpot.six_day_price = rows["60日涨跌幅"]
                    stockZhASpot.year_increase_decrease = rows["年初至今涨跌幅"]
                    stockZhASpot.create_time = datetime.now()
                    stockZhASpot.update_time = datetime.now()
                    # 保存数据到数据库
                    data_list.append(stockZhASpot)
                    progress.update(task, advance=1)
            save_stock_zh_a_spot(data_list)
    except Exception as e:
        # 异常处理，打印堆栈信息
        print("处理数据时发生异常：")
        print(f"Error processing data: {e}")
        print(traceback.format_exc())
    
