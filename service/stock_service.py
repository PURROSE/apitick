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
                    for index, row in stock_gsrl_gsdt_em_df.iterrows():
                        if index == 0:
                            # 跳过第一行数据
                            continue
                        row_data = row.tolist()
                        stock_gsrl_gsdt = StockGsrlGsdt()
                        stock_gsrl_gsdt.num = row_data[0]
                        stock_gsrl_gsdt.stock_code = row_data[1]
                        stock_gsrl_gsdt.stock_name = row_data[2]
                        stock_gsrl_gsdt.msg_type = row_data[3]
                        stock_gsrl_gsdt.msg = row_data[4]
                        # 2023-08-08时间格式转换为日期对象
                        stock_gsrl_gsdt.msg_date = row_data[5]
                        stock_gsrl_gsdt.create_time = datetime.now()
                        # 保存数据到数据库
                        save_stock_gsrl_gsdt(stock_gsrl_gsdt)
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
            print(f"数据:{rowsize}")
            with Progress() as progress:
                task = progress.add_task(description="[cyan]Downloading...", total=rowsize)
                for index, rows in stock_zh_a_spot_em_df.iterrows() :
                    progress.update(task, advance=1)
                    if index == 0:
                        # 跳过第一行数据
                        continue
                    
                    row_data = rows.tolist()
                    stockZhASpot = StockZhASpot()
                    stockZhASpot.stock_code = row_data[1]
                    stockZhASpot.stock_name = row_data[2]
                    stockZhASpot.new_price = row_data[3]
                    stockZhASpot.rise_and_fall = row_data[4]
                    stockZhASpot.increase_and_decrease = row_data[5]
                    stockZhASpot.trading_volume = row_data[6]
                    stockZhASpot.turnover = row_data[7]
                    stockZhASpot.amplitude = row_data[8]
                    stockZhASpot.max_price = row_data[9]
                    stockZhASpot.min_price = row_data[10]
                    stockZhASpot.open_price = row_data[11]
                    stockZhASpot.close_price = row_data[12]
                    stockZhASpot.turnover_rate = row_data[13]
                    stockZhASpot.price_earnings_ratio = row_data[14]
                    stockZhASpot.B_ratio = row_data[15]
                    stockZhASpot.total_market_value = row_data[16]
                    stockZhASpot.the_market_capitalization = row_data[17]
                    stockZhASpot.rate_of_increase = row_data[18]
                    stockZhASpot.Five_minutes_price = row_data[19]
                    stockZhASpot.six_day_price = row_data[20]
                    stockZhASpot.year_increase_decrease = row_data[21]
                    stockZhASpot.create_time = datetime.now()
                    stockZhASpot.update_time = datetime.now()
                    # 保存数据到数据库
                    save_stock_zh_a_spot(stockZhASpot)
    except Exception as e:
        # 异常处理，打印堆栈信息
        print("处理数据时发生异常：")
        print(f"Error processing data: {e}")
        print(traceback.format_exc())
    
