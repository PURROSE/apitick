import sys
from service.stock_service import task
from dbtool.obj_class import initialize_database

def main():
    print(f"扫描模块：{sys.path}")
    initialize_database()
    print("程序启动！")
    # 这里可以调用其他模块的函数
    task()

if __name__ == "__main__":
    main()
