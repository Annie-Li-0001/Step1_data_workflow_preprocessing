import os
from urllib.parse import quote
from sqlalchemy import create_engine
from datetime import datetime as dt


class Config:
    """
    项目的配置文件，包括了数据库的信息，量化数据表的名称等
    如果在本地运行，需要调整为自己本地的配置
    """

    START_DATE = "20120101"
    # START_DATE = '20230101'
    TODAY = dt.now().strftime("%Y%m%d")

    DAILY_BASIC = "AShareEodBasic"
    ST_TABLE = "AShareST"
    EOD_PRICE = "AShareEodPrice"
    EOD_PRICE_HFQ = "AShareEodPriceHFQ"
    EOD_PRICE_QFQ = "AShareEodPriceQFQ"
    EOD_PRICE_CB = "CBEodPrice"
    INCOME = "income"
    BALANCE_SHEET = "balance_sheet"
    CASH_FLOW = "cashflow"

    MYSQL_ENGINE_STR = "mysql+pymysql://{}:{}@{}:{}/{}".format(
        "root", quote("root"), "mysql", "3306", "stock"
    )
    MYSQL_ENGINE_FACTOR_STR = "mysql+pymysql://{}:{}@{}:{}/{}".format(
        "root", quote("root"), "mysql", "3306", "factors"
    )
    MONGO_ENGINE_STR = "mongodb://admin:xxxxx@localhost:27017/quant"
    REDIS_HOST = "xxxxx"
    REDIS_PORT = "6379"
    REDIS_KEY_EXPIRE_TIME = 60 * 60 * 8

    TOKEN_PATH = os.path.join(os.path.expanduser("~"), ".config", "config.yaml")
