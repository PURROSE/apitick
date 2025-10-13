from loguru import logger

def set_log():
    log_format = "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level:<8} | {name}:{function}:{line} - {message}"

    # 文件输出
    logger.add(
        sink="logs/log_{time}.log",
        format=log_format,
        rotation="1 day",
        retention="7 days",
        level="INFO",
    )
