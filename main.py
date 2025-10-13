import sys
from service.stock_service import task
from dbtool.obj_class import initialize_database
from dbtool.log_set import set_log
from view.pgui import setup

def main():
    
    print(f"扫描模块：{sys.path}")
    set_log()

    task()
    
    # initialize_database()
    setup()
    
if __name__ == "__main__":
    main()
